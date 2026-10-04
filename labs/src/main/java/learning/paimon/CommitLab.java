package learning.paimon;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 实验 7：并发提交——抢快照号与重试、真实冲突、提交幂等。
 *
 * <p>用 Paimon Java API 直接调用 StreamTableWrite / StreamTableCommit，不经过 Flink，便于精确控制“谁先提交”。
 *
 * <p>运行：{@code MAIN_CLASS=learning.paimon.CommitLab ./run.sh}
 */
public class CommitLab {

    private static Catalog catalog;

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            String dir = System.getProperty("warehouse", "warehouse");
            java.nio.file.Path path = Paths.get(dir).toAbsolutePath().normalize();
            catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(path.toUri().toString())));
            catalog.createDatabase("default", true);

            String only = System.getProperty("scenario", "all");
            if (only.equals("all") || only.equals("race")) {
                race();
            }
            if (only.equals("all") || only.equals("conflict")) {
                conflict();
            }
            if (only.equals("all") || only.equals("idempotent")) {
                idempotent();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    // ---------------------------------------------------------------------------------------
    // 场景 1：两个作业同时往同一张 append 表提交，抢同一个快照号
    // ---------------------------------------------------------------------------------------
    private static void race() throws Exception {
        title("场景 1：两个作业并发 APPEND，抢快照号");
        Table table = recreate("race_log", false, Collections.<String, String>emptyMap());
        final int commitsPerJob = 30;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger failures = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (final String user : new String[] {"job-A", "job-B"}) {
            Thread t =
                    new Thread(
                            () -> {
                                try {
                                    StreamWriteBuilder builder =
                                            table.newStreamWriteBuilder().withCommitUser(user);
                                    try (StreamTableWrite write = builder.newWrite();
                                            StreamTableCommit commit = builder.newCommit()) {
                                        start.await();
                                        for (long i = 1; i <= commitsPerJob; i++) {
                                            write.write(row(i, user));
                                            commit.commit(i, write.prepareCommit(false, i));
                                        }
                                    }
                                } catch (Exception e) {
                                    failures.incrementAndGet();
                                    e.printStackTrace();
                                }
                            },
                            user);
            threads.add(t);
            t.start();
        }
        start.countDown();
        for (Thread t : threads) {
            t.join();
        }

        SnapshotManager sm = snapshotManager("race_log");
        Map<String, Integer> perUser = new HashMap<>();
        Iterator<Snapshot> it = sm.snapshots();
        while (it.hasNext()) {
            Snapshot s = it.next();
            perUser.merge(s.commitUser(), 1, Integer::sum);
        }
        System.out.println("两个作业各提交 " + commitsPerJob + " 次，失败的作业数 = " + failures.get());
        System.out.println("最新快照 id = " + sm.latestSnapshotId());
        System.out.println("按 commitUser 统计快照数 = " + new java.util.TreeMap<>(perUser));
        System.out.println("（抢号失败会打印 WARN “Atomic commit failed for snapshot #N”，然后自动重试）");
    }

    // ---------------------------------------------------------------------------------------
    // 场景 2：两个作业对同一个 bucket 做全量合并，删除同一批文件
    // ---------------------------------------------------------------------------------------
    private static void conflict() throws Exception {
        title("场景 2：两个作业合并同一个 bucket，删除同一批文件");
        Map<String, String> opts = new HashMap<>();
        opts.put("bucket", "1");
        // 不让写入自动触发合并，保证 3 个 L0 文件都留着，由下面两个作业手动合并
        opts.put("num-sorted-run.compaction-trigger", "100");
        // num-levels 默认 = trigger + 1，这里固定回默认的 6 层（maxLevel = 5）
        opts.put("num-levels", "6");
        Table table = recreate("conflict_orders", true, opts);

        StreamWriteBuilder seed = table.newStreamWriteBuilder().withCommitUser("writer");
        try (StreamTableWrite write = seed.newWrite();
                StreamTableCommit commit = seed.newCommit()) {
            for (long i = 1; i <= 3; i++) {
                write.write(row(i, "v" + i));
                commit.commit(i, write.prepareCommit(false, i));
            }
        }
        printSnapshots("conflict_orders");

        // 两个合并作业都基于快照 3：各自恢复出 3 个 L0 文件，做全量合并
        StreamWriteBuilder b1 = table.newStreamWriteBuilder().withCommitUser("compact-job-1");
        StreamWriteBuilder b2 = table.newStreamWriteBuilder().withCommitUser("compact-job-2");
        StreamTableWrite w1 = b1.newWrite();
        StreamTableWrite w2 = b2.newWrite();
        w1.compact(BinaryRow.EMPTY_ROW, 0, true);
        w2.compact(BinaryRow.EMPTY_ROW, 0, true);
        List<CommitMessage> m1 = w1.prepareCommit(true, 1);
        List<CommitMessage> m2 = w2.prepareCommit(true, 1);
        System.out.println("compact-job-1 的变更：" + m1);
        System.out.println("compact-job-2 的变更：" + m2);

        try (StreamTableCommit c1 = b1.newCommit()) {
            c1.commit(1, m1);
            System.out.println("compact-job-1 提交成功");
        }
        printSnapshots("conflict_orders");
        try (StreamTableCommit c2 = b2.newCommit()) {
            c2.commit(1, m2);
            System.out.println("compact-job-2 提交成功（不应该出现）");
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            System.out.println("compact-job-2 提交失败：" + e.getClass().getSimpleName());
            System.out.println("[CONFLICT] " + firstLines(e.getMessage(), 12));
        }
        printSnapshots("conflict_orders");
        w1.close();
        w2.close();
    }

    // ---------------------------------------------------------------------------------------
    // 场景 3：作业重启后重复提交同一个 checkpoint
    // ---------------------------------------------------------------------------------------
    private static void idempotent() throws Exception {
        title("场景 3：重启后重复提交同一个 identifier");
        Table table = recreate("idem_log", false, Collections.<String, String>emptyMap());

        StreamWriteBuilder first = table.newStreamWriteBuilder().withCommitUser("job-X");
        List<CommitMessage> messages;
        try (StreamTableWrite write = first.newWrite();
                StreamTableCommit commit = first.newCommit()) {
            write.write(row(1, "cp-5"));
            messages = write.prepareCommit(false, 5);
            commit.commit(5, messages);
        }
        printSnapshots("idem_log");

        // 模拟 failover：同一个 commitUser 的新作业，从 checkpoint 恢复后再次提交 identifier 5
        StreamWriteBuilder restarted = table.newStreamWriteBuilder().withCommitUser("job-X");
        try (StreamTableCommit commit = restarted.newCommit()) {
            Map<Long, List<CommitMessage>> again = new HashMap<>();
            again.put(5L, messages);
            int n = commit.filterAndCommit(again);
            System.out.println("重启后 filterAndCommit(identifier 5) 实际提交了 " + n + " 个");
        }
        printSnapshots("idem_log");

        // 不同的 commitUser 提交同样的文件：不会被过滤
        StreamWriteBuilder other = table.newStreamWriteBuilder().withCommitUser("job-Y");
        try (StreamTableWrite write = other.newWrite();
                StreamTableCommit commit = other.newCommit()) {
            write.write(row(2, "cp-5-by-Y"));
            Map<Long, List<CommitMessage>> m = new HashMap<>();
            m.put(5L, write.prepareCommit(false, 5));
            int n = commit.filterAndCommit(m);
            System.out.println("job-Y filterAndCommit(identifier 5) 实际提交了 " + n + " 个");
        }
        printSnapshots("idem_log");
    }

    // ---------------------------------------------------------------------------------------

    private static Table recreate(String name, boolean primaryKey, Map<String, String> options)
            throws Exception {
        Identifier id = Identifier.create("default", name);
        catalog.dropTable(id, true);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("v", DataTypes.STRING())
                        .options(options);
        if (primaryKey) {
            schema.primaryKey("id");
        }
        catalog.createTable(id, schema.build(), false);
        return catalog.getTable(id);
    }

    private static SnapshotManager snapshotManager(String name) throws Exception {
        return ((FileStoreTable) catalog.getTable(Identifier.create("default", name)))
                .snapshotManager();
    }

    private static void printSnapshots(String name) throws Exception {
        SnapshotManager sm = snapshotManager(name);
        System.out.println("  ┌ " + name + " 的快照");
        Iterator<Snapshot> it = sm.snapshots();
        while (it.hasNext()) {
            Snapshot s = it.next();
            System.out.printf(
                    "  │ snapshot %d  %-7s  user=%-13s  identifier=%d  total=%d%n",
                    s.id(),
                    s.commitKind(),
                    s.commitUser(),
                    s.commitIdentifier(),
                    s.totalRecordCount());
        }
        System.out.println("  └");
    }

    private static GenericRow row(long id, String v) {
        return GenericRow.of(id, BinaryString.fromString(v));
    }

    private static void title(String t) {
        System.out.println();
        System.out.println("==================== " + t + " ====================");
    }

    private static String firstLines(String s, int n) {
        if (s == null) {
            return "null";
        }
        String[] lines = s.split("\n");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(n, lines.length); i++) {
            sb.append(lines[i]).append('\n');
        }
        if (lines.length > n) {
            sb.append("...（共 ").append(lines.length).append(" 行）");
        }
        return sb.toString();
    }
}
