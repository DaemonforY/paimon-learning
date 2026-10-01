package learning.paimon;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 一个极简的 “SQL Client”：逐条执行 SQL 文件中的语句，并打印结果。
 *
 * <p>用法：SqlRunner <sql 文件>。默认批模式。SQL 文件中支持：
 *
 * <ul>
 *   <li>{@code SET 'k' = 'v';} 设置 Flink / Paimon 参数
 *   <li>{@code -- @sh <命令>} 执行一条 shell 命令并打印输出（工作目录为 labs/），用于观察表目录
 *   <li>{@code -- @expect-error} 下一条语句预期会失败：只打印异常原因，不中断执行
 * </ul>
 *
 * <p>在 IDEA 中直接运行本类（Program arguments 填 SQL 文件路径），即可在 Paimon 源码里下断点。
 */
public class SqlRunner {

    private static final Pattern SET_PATTERN =
            Pattern.compile("(?is)^SET\\s+'([^']+)'\\s*=\\s*'([^']*)'$");
    private static final String SH_PREFIX = "@sh ";
    private static final String EXPECT_ERROR = "@expect-error";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: SqlRunner <sql-file>");
            System.exit(1);
        }
        int exitCode = 0;
        try {
            run(args[0]);
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 1;
        }
        // Flink 本地 MiniCluster 的非守护线程会让 JVM 不退出，这里显式退出
        System.exit(exitCode);
    }

    private static void run(String sqlFile) throws Exception {
        // -Dmode=stream：流模式，脚本最后一条 SELECT 以流方式持续读取 -Dstream.seconds 秒
        boolean streaming = "stream".equals(System.getProperty("mode"));
        // -Ddebug=true：断点调试模式。并行度 1（只有一个 writer 线程），并把心跳/RPC 超时调到 1 小时，
        // 避免停在断点上时 MiniCluster 判定 TaskManager 失联导致作业失败
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        if (Boolean.getBoolean("debug")) {
            conf.setString("parallelism.default", "1");
            conf.setString("heartbeat.timeout", "3600000");
            conf.setString("pekko.ask.timeout", "1 h");
            conf.setString("pekko.lookup.timeout", "1 h");
            System.out.println("[INFO] 调试模式：parallelism=1，心跳/RPC 超时 1 小时");
        }
        EnvironmentSettings.Builder settings = EnvironmentSettings.newInstance().withConfiguration(conf);
        TableEnvironment tEnv =
                TableEnvironment.create(
                        streaming ? settings.inStreamingMode().build() : settings.inBatchMode().build());
        tEnv.getConfig().set("table.dml-sync", "true");
        if (streaming) {
            // consumer 进度、collect 结果都依赖 checkpoint 完成
            tEnv.getConfig().set("execution.checkpointing.interval", "2s");
        }

        String content = substituteVariables(
                new String(Files.readAllBytes(Paths.get(sqlFile)), StandardCharsets.UTF_8));
        List<String> statements = splitStatements(content);
        boolean expectError = false;
        for (int i = 0; i < statements.size(); i++) {
            String stmt = statements.get(i);
            if (streaming && i == statements.size() - 1
                    && stmt.toUpperCase(Locale.ROOT).startsWith("SELECT")) {
                streamSelect(tEnv, stmt, Long.parseLong(System.getProperty("stream.seconds", "30")));
                return;
            }
            if (stmt.equals(EXPECT_ERROR)) {
                expectError = true;
                continue;
            }
            if (stmt.startsWith(SH_PREFIX)) {
                runShell(stmt.substring(SH_PREFIX.length()));
                continue;
            }
            System.out.println();
            System.out.println("Flink SQL> " + stmt.replace("\n", "\n        > ") + ";");
            try {
                executeSql(tEnv, stmt);
                if (expectError) {
                    System.out.println("[WARN] 预期失败，但执行成功了");
                }
            } catch (Throwable t) {
                if (!expectError) {
                    throw t;
                }
                System.out.println("[EXPECTED ERROR] " + rootCause(t));
            }
            expectError = false;
        }
    }

    private static void executeSql(TableEnvironment tEnv, String sql) throws Exception {
        Matcher set = SET_PATTERN.matcher(sql);
        if (set.matches()) {
            tEnv.getConfig().set(set.group(1), set.group(2));
            System.out.println("[INFO] Set " + set.group(1) + " = " + set.group(2));
            return;
        }
        TableResult result = tEnv.executeSql(sql);
        String head = sql.trim().toUpperCase(Locale.ROOT);
        if (head.startsWith("SELECT") || head.startsWith("SHOW") || head.startsWith("DESC")
                || head.startsWith("EXPLAIN") || head.startsWith("WITH") || head.startsWith("CALL")) {
            result.print();
        } else {
            result.await();
            System.out.println("[INFO] OK");
        }
    }

    /**
     * 替换 SQL 文件中的变量，使实验在任何机器上都能运行：
     *
     * <ul>
     *   <li>{@code ${warehouse}}：Paimon warehouse 的 URI，用于 CREATE CATALOG
     *   <li>{@code ${warehouse_dir}}：warehouse 的本地路径，用于 {@code -- @sh} 命令查看目录
     * </ul>
     *
     * <p>默认 warehouse 为当前目录（labs/）下的 warehouse/，可用 {@code -Dwarehouse=/some/dir} 覆盖。
     */
    static String substituteVariables(String content) {
        String dir = System.getProperty("warehouse", "warehouse");
        java.nio.file.Path path = Paths.get(dir).toAbsolutePath().normalize();
        return content
                .replace("${warehouse}", path.toUri().toString())
                .replace("${warehouse_dir}", path.toString());
    }

    /** 流式执行 SELECT：逐行打印（带 RowKind 和相对时间），到时间后退出。 */
    private static void streamSelect(TableEnvironment tEnv, String sql, long seconds) throws Exception {
        System.out.println();
        System.out.println("Flink SQL (streaming, " + seconds + "s)> " + sql.replace("\n", "\n        > ") + ";");
        long start = System.currentTimeMillis();
        Thread timer = new Thread(() -> {
            try {
                Thread.sleep(seconds * 1000);
            } catch (InterruptedException ignored) {
            }
            System.out.println("[INFO] 流式读取 " + seconds + "s 到时，退出");
            System.exit(0);
        });
        timer.setDaemon(true);
        timer.start();
        try (org.apache.flink.util.CloseableIterator<org.apache.flink.types.Row> it =
                tEnv.executeSql(sql).collect()) {
            // 编排脚本靠这行标记判断“流作业已就绪”
            System.out.printf("[t+%5.1fs] [READY] 流式作业已提交%n", (System.currentTimeMillis() - start) / 1000.0);
            while (it.hasNext()) {
                org.apache.flink.types.Row row = it.next();
                System.out.printf("[%s t+%5.1fs] %s%n",
                        java.time.LocalTime.now().withNano(0),
                        (System.currentTimeMillis() - start) / 1000.0, row);
            }
        }
    }

    private static void runShell(String command) throws Exception {
        System.out.println();
        System.out.println("$ " + command);
        Process process = new ProcessBuilder("bash", "-c", command)
                .directory(new File(System.getProperty("user.dir")))
                .redirectErrorStream(true)
                .start();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
        process.waitFor();
    }

    private static String rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    /** 按行尾分号切分语句；普通 -- 注释行忽略，-- @sh / -- @expect-error 作为指令保留。 */
    static List<String> splitStatements(String content) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (String line : content.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("--")) {
                String directive = trimmed.substring(2).trim();
                if (directive.startsWith(SH_PREFIX) || directive.equals(EXPECT_ERROR)) {
                    statements.add(directive);
                }
                continue;
            }
            if (trimmed.isEmpty()) {
                continue;
            }
            current.append(line).append("\n");
            if (trimmed.endsWith(";")) {
                String stmt = current.toString().trim();
                statements.add(stmt.substring(0, stmt.length() - 1).trim());
                current.setLength(0);
            }
        }
        if (current.toString().trim().length() > 0) {
            statements.add(current.toString().trim());
        }
        return statements;
    }
}
