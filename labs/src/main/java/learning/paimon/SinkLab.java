package learning.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 实验 11：Flink 流式写入 Paimon 的两阶段提交（S2 第 9 讲）。
 *
 * <p>三个场景（第一个参数，默认 all）：
 *
 * <ul>
 *   <li>checkpoint：每 2 秒一次 checkpoint 写 100 行，看快照的 commit_identifier 与 checkpoint id 的关系；
 *   <li>nockpt：不开 checkpoint 的无界流作业写 10 秒后取消，看有没有快照；
 *   <li>failover：Append 表（没有主键去重）写 200 行，第 120 行第一次处理时抛异常，作业从 checkpoint 恢复，
 *       看最终行数是否不重不丢。
 * </ul>
 *
 * <p>运行：{@code MAIN_CLASS=learning.paimon.SinkLab ./run.sh [checkpoint|nockpt|failover]}
 */
public class SinkLab {

    private static String warehouse;

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            // 打印 checkpoint 完成日志（全局配置里 org.apache.flink.runtime 是 ERROR）
            Configurator.setLevel("org.apache.flink.runtime.checkpoint.CheckpointCoordinator", Level.INFO);
            String dir = System.getProperty("warehouse", "warehouse");
            warehouse = Paths.get(dir).toAbsolutePath().normalize().toUri().toString();
            String only = args.length > 0 ? args[0] : "all";
            if (only.equals("all") || only.equals("checkpoint")) {
                checkpoint();
            }
            if (only.equals("all") || only.equals("nockpt")) {
                noCheckpoint();
            }
            if (only.equals("all") || only.equals("failover")) {
                failover();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    // ---------------------------------------------------------------------------------------------

    private static void checkpoint() throws Exception {
        title("场景 1：每 2 秒一次 checkpoint，写 100 行（每秒 20 行）");
        TableEnvironment tEnv = streamEnv(true);
        prepare(tEnv, "sk_ckpt", 100);
        run(tEnv, "INSERT INTO sk_ckpt SELECT id FROM gen");
        query("SELECT snapshot_id, commit_user, commit_identifier, commit_kind, delta_record_count FROM `sk_ckpt$snapshots`");
        query("SELECT COUNT(*) AS cnt FROM sk_ckpt");
    }

    private static void noCheckpoint() throws Exception {
        title("场景 2：不开 checkpoint 的无界流作业，写 10 秒后取消");
        TableEnvironment tEnv = streamEnv(false);
        prepare(tEnv, "sk_nockpt", -1);
        tEnv.getConfig().set("table.dml-sync", "false");
        echo("INSERT INTO sk_nockpt SELECT id FROM gen");
        TableResult result = tEnv.executeSql("INSERT INTO sk_nockpt SELECT id FROM gen");
        JobClient job = result.getJobClient().orElseThrow(IllegalStateException::new);
        TimeUnit.SECONDS.sleep(10);
        System.out.println("[SinkLab] 运行 10 秒，作业状态：" + job.getJobStatus().get() + "，取消作业");
        job.cancel().get();
        query("SELECT COUNT(*) AS snapshots FROM `sk_nockpt$snapshots`");
        query("SELECT COUNT(*) AS cnt FROM sk_nockpt");
    }

    private static void failover() throws Exception {
        title("场景 3：Append 表写 200 行，第 120 行第一次处理时抛异常，从 checkpoint 恢复");
        TableEnvironment tEnv = streamEnv(true);
        tEnv.getConfig().set("restart-strategy.type", "fixed-delay");
        tEnv.getConfig().set("restart-strategy.fixed-delay.attempts", "3");
        tEnv.getConfig().set("restart-strategy.fixed-delay.delay", "1 s");
        prepare(tEnv, "sk_fail", 200);
        tEnv.createTemporarySystemFunction("fail_once", FailOnce.class);
        run(tEnv, "INSERT INTO sk_fail SELECT fail_once(id) FROM gen");
        query("SELECT snapshot_id, commit_user, commit_identifier, commit_kind, delta_record_count FROM `sk_fail$snapshots`");
        query("SELECT COUNT(*) AS cnt, COUNT(DISTINCT id) AS distinct_ids, MIN(id) AS min_id, MAX(id) AS max_id FROM sk_fail");
        // 快照引用的数据文件数 vs 磁盘上的数据文件数：多出来的就是没被提交的孤儿文件
        query("SELECT COUNT(*) AS referenced_files FROM `sk_fail$files`");
        long onDisk;
        try (java.util.stream.Stream<java.nio.file.Path> files =
                java.nio.file.Files.walk(Paths.get(java.net.URI.create(warehouse)).resolve("default.db/sk_fail"))) {
            onDisk = files.filter(f -> f.toString().endsWith(".parquet")).count();
        }
        long referenced = countRows("SELECT COUNT(*) FROM `sk_fail$files`");
        System.out.println("[SinkLab] sk_fail 目录下的 parquet 文件数：" + onDisk + "，快照引用：" + referenced
                + "，孤儿文件：" + (onDisk - referenced));
    }

    /** 第一次遇到 id = 120 时抛异常；作业重启后同一个 JVM 里不再抛。 */
    public static class FailOnce extends ScalarFunction {
        private static final AtomicBoolean FAILED = new AtomicBoolean(false);

        // eval 是 Flink ScalarFunction 约定的方法名（UDF 入口），不是动态执行代码
        public Long eval(Long id) {
            if (id != null && id == 120 && FAILED.compareAndSet(false, true)) {
                System.out.println("[SinkLab] 模拟故障：处理到 id = 120，抛出异常");
                throw new RuntimeException("[SinkLab] 模拟故障：id = 120");
            }
            return id;
        }
    }

    // ---------------------------------------------------------------------------------------------

    private static TableEnvironment streamEnv(boolean checkpointing) {
        Configuration conf = new Configuration();
        conf.setString("parallelism.default", "1");
        if (Boolean.getBoolean("debug")) {
            conf.setString("heartbeat.timeout", "3600000");
            conf.setString("pekko.ask.timeout", "1 h");
            conf.setString("pekko.lookup.timeout", "1 h");
            System.out.println("[INFO] 调试模式：心跳/RPC 超时 1 小时");
        }
        if (checkpointing) {
            conf.setString("execution.checkpointing.interval", "2 s");
        }
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().withConfiguration(conf).inStreamingMode().build());
        tEnv.getConfig().set("table.dml-sync", "true");
        System.out.println("[SinkLab] checkpoint：" + (checkpointing ? "每 2 秒" : "未开启"));
        return tEnv;
    }

    /** 建 catalog、Append 表和数据源；rows < 0 表示无界。 */
    private static void prepare(TableEnvironment tEnv, String table, int rows) {
        tEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.executeSql("USE CATALOG paimon");
        exec(tEnv, "DROP TABLE IF EXISTS " + table);
        exec(tEnv, "CREATE TABLE " + table + " (id BIGINT)");
        String bound = rows > 0 ? ", 'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '" + rows + "'" : "";
        // 调试模式下断点会拖慢 checkpoint，数据源降到每秒 5 行，保证故障发生在若干个 checkpoint 之后
        String rate = Boolean.getBoolean("debug") ? "5" : "20";
        exec(tEnv, "CREATE TEMPORARY TABLE gen (id BIGINT) WITH ('connector' = 'datagen', 'rows-per-second' = '" + rate + "'" + bound + ")");
    }

    private static void run(TableEnvironment tEnv, String sql) throws Exception {
        echo(sql);
        long start = System.currentTimeMillis();
        tEnv.executeSql(sql).await();
        System.out.printf("[SinkLab] 作业结束，用时 %.1f 秒%n", (System.currentTimeMillis() - start) / 1000.0);
    }

    private static void exec(TableEnvironment tEnv, String sql) {
        echo(sql);
        tEnv.executeSql(sql);
    }

    /** 查询用独立的批模式环境，读完即结束。 */
    private static void query(String sql) {
        TableEnvironment batch = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        batch.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = '" + warehouse + "')");
        batch.executeSql("USE CATALOG paimon");
        echo(sql);
        batch.executeSql(sql).print();
    }

    private static long countRows(String sql) throws Exception {
        TableEnvironment batch = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        batch.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = '" + warehouse + "')");
        batch.executeSql("USE CATALOG paimon");
        try (org.apache.flink.util.CloseableIterator<org.apache.flink.types.Row> it = batch.executeSql(sql).collect()) {
            return ((Number) it.next().getField(0)).longValue();
        }
    }

    private static void echo(String sql) {
        System.out.println();
        System.out.println("Flink SQL> " + sql + ";");
    }

    private static void title(String text) {
        System.out.println();
        System.out.println("==================== " + text + " ====================");
    }
}
