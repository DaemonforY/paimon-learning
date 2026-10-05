package learning.paimon;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 实验 12：Flink 流读 Paimon（S2 第 10 讲）。
 *
 * <ul>
 *   <li>read：后台流读一张主键表（2 个 bucket，source 并行度 2，changelog-producer = none），
 *       前台依次写入、更新、手动合并、删除，打印每条变更被读到的时间；
 *   <li>plan：同一条流读 SQL，配不配 consumer-id，Flink 生成的算子有什么不同。
 * </ul>
 *
 * <p>运行：{@code MAIN_CLASS=learning.paimon.SourceLab ./run.sh [read|plan]}（默认 all）
 */
public class SourceLab {

    private static String warehouse;
    private static long start;

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            String dir = System.getProperty("warehouse", "warehouse");
            warehouse = Paths.get(dir).toAbsolutePath().normalize().toUri().toString();
            String only = args.length > 0 ? args[0] : "all";
            if (only.equals("all") || only.equals("read")) {
                read();
            }
            if (only.equals("all") || only.equals("plan")) {
                plan();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    private static void read() throws Exception {
        title("场景 1：流读主键表，同时写入 / 更新 / 合并 / 删除");
        TableEnvironment batch = batchEnv();
        exec(batch, "DROP TABLE IF EXISTS src_t");
        exec(batch, "CREATE TABLE src_t (k INT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '2')");

        TableEnvironment stream = streamEnv();
        String query = "SELECT * FROM src_t /*+ OPTIONS('continuous.discovery-interval' = '1 s') */";
        echo(query);
        start = System.currentTimeMillis();
        TableResult result = stream.executeSql(query);
        Thread reader = new Thread(() -> {
            try (CloseableIterator<Row> it = result.collect()) {
                while (it.hasNext()) {
                    Row row = it.next();
                    log("读到 " + row.getKind().shortString() + "[" + row.getField(0) + ", " + row.getField(1) + "]");
                }
            } catch (Exception e) {
                log("流读结束：" + e.getClass().getSimpleName());
            }
        }, "collect");
        reader.setDaemon(true);
        reader.start();
        long pause = Boolean.getBoolean("debug") ? 25 : 5;
        TimeUnit.SECONDS.sleep(pause);

        write(batch, "INSERT INTO src_t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        TimeUnit.SECONDS.sleep(pause);
        write(batch, "INSERT INTO src_t VALUES (1, 'A')");
        TimeUnit.SECONDS.sleep(pause);
        write(batch, "CALL sys.compact(`table` => 'default.src_t')");
        TimeUnit.SECONDS.sleep(pause);
        write(batch, "DELETE FROM src_t WHERE k = 2");
        TimeUnit.SECONDS.sleep(pause);

        log("取消流读作业");
        result.getJobClient().orElseThrow(IllegalStateException::new).cancel().get();
        TimeUnit.SECONDS.sleep(1);
        exec(batch, "SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `src_t$snapshots`");
        batch.executeSql("SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `src_t$snapshots`").print();
    }

    private static void plan() throws Exception {
        title("场景 2：同一条流读 SQL，配不配 consumer-id，生成的算子不同");
        TableEnvironment batch = batchEnv();
        exec(batch, "DROP TABLE IF EXISTS src_p");
        exec(batch, "CREATE TABLE src_p (k INT, v STRING, PRIMARY KEY (k) NOT ENFORCED) "
                + "WITH ('bucket' = '2', 'changelog-producer' = 'lookup', 'consumer.expiration-time' = '1 d')");
        exec(batch, "INSERT INTO src_p VALUES (1, 'a')");
        TableEnvironment stream = streamEnv();
        String plain = "SELECT * FROM src_p";
        String withConsumer = "SELECT * FROM src_p /*+ OPTIONS('consumer-id' = 'lab12') */";
        for (String sql : new String[] {plain, withConsumer}) {
            echo("EXPLAIN JSON_EXECUTION_PLAN " + sql);
            String json = stream.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN);
            Matcher m = Pattern.compile("\"type\"\\s*:\\s*\"([^\"]+)\"").matcher(json);
            Set<String> ops = new LinkedHashSet<>();
            while (m.find()) {
                ops.add(m.group(1));
            }
            System.out.println("算子：" + String.join("  →  ", ops));
        }
    }

    private static void write(TableEnvironment batch, String sql) {
        log("写入：" + sql);
        batch.executeSql(sql).print();
        log("写入完成");
    }

    // ---------------------------------------------------------------------------------------------

    private static TableEnvironment streamEnv() {
        Configuration conf = new Configuration();
        conf.setString("parallelism.default", "2");
        conf.setString("execution.checkpointing.interval", "2 s");
        if (Boolean.getBoolean("debug")) {
            conf.setString("heartbeat.timeout", "3600000");
            conf.setString("pekko.ask.timeout", "1 h");
            conf.setString("pekko.lookup.timeout", "1 h");
            System.out.println("[INFO] 调试模式：心跳/RPC 超时 1 小时，每步间隔 25 秒");
        }
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().withConfiguration(conf).inStreamingMode().build());
        useCatalog(tEnv);
        return tEnv;
    }

    private static TableEnvironment batchEnv() {
        Configuration conf = new Configuration();
        conf.setString("parallelism.default", "1");
        TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().withConfiguration(conf).inBatchMode().build());
        tEnv.getConfig().set("table.dml-sync", "true");
        useCatalog(tEnv);
        return tEnv;
    }

    private static void useCatalog(TableEnvironment tEnv) {
        tEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = '" + warehouse + "')");
        tEnv.executeSql("USE CATALOG paimon");
    }

    private static void exec(TableEnvironment tEnv, String sql) {
        echo(sql);
        if (!sql.startsWith("SELECT")) {
            tEnv.executeSql(sql);
        }
    }

    private static void log(String msg) {
        System.out.printf("[SourceLab +%5.1fs] %s%n", (System.currentTimeMillis() - start) / 1000.0, msg);
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
