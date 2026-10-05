package learning.paimon.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 逐条执行 SQL 文件里的 Spark SQL（local[1]，单线程、shuffle 分区 1，文件数可复现），打印结果。和 Flink 实验的 SqlRunner 用法一致：
 *
 * <ul>
 *   <li>Paimon catalog 名为 {@code paimon}，warehouse 由 {@code -Dwarehouse} 指定（默认 warehouse/）；
 *   <li>{@code -- @sh <命令>}：执行 shell 命令并打印输出，{@code ${warehouse_dir}} 替换为 warehouse 目录；
 *   <li>{@code -- @expect-error}：下一条语句预期失败，打印根因后继续。
 * </ul>
 *
 * <p>运行：{@code ./spark.sh <sql 文件>}
 */
public class SparkSqlRunner {

    private static final String SH_PREFIX = "@sh ";
    private static final String EXPECT_ERROR = "@expect-error";
    private static final Map<String, String> VARS = new LinkedHashMap<>();

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: SparkSqlRunner <sql-file>");
            System.exit(1);
        }
        int exitCode = 0;
        try {
            run(args[0]);
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    private static void run(String sqlFile) throws Exception {
        String dir = System.getProperty("warehouse", "warehouse");
        String warehouse = Paths.get(dir).toAbsolutePath().normalize().toUri().toString();
        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("paimon-labs-spark")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
                .config("spark.sql.catalog.paimon.warehouse", warehouse)
                .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        System.out.println("[INFO] Spark " + spark.version() + "，catalog paimon（warehouse = ${warehouse}）");

        String content = new String(Files.readAllBytes(Paths.get(sqlFile)), StandardCharsets.UTF_8);
        boolean expectError = false;
        for (String raw : splitStatements(content)) {
            String stmt = substituteVariables(raw);
            if (stmt.equals(EXPECT_ERROR)) {
                expectError = true;
                continue;
            }
            if (stmt.startsWith(SH_PREFIX)) {
                runShell(raw.substring(SH_PREFIX.length()), stmt.substring(SH_PREFIX.length()));
                continue;
            }
            System.out.println();
            System.out.println("Spark SQL> " + raw.replace("\n", "\n        > ") + ";");
            try {
                execute(spark, stmt);
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
        spark.stop();
    }

    private static void execute(SparkSession spark, String sql) {
        Dataset<Row> result = spark.sql(sql);
        String head = sql.trim().toUpperCase(Locale.ROOT);
        if (head.startsWith("EXPLAIN")) {
            // EXPLAIN 只有一列一行，原样打印，不加表格边框
            for (Row row : result.collectAsList()) {
                System.out.println(row.getString(0));
            }
        } else if (head.startsWith("SET ")) {
            System.out.println("[INFO] " + sql.substring(4).trim());
        } else if (head.startsWith("SELECT") || head.startsWith("SHOW") || head.startsWith("DESC")
                || head.startsWith("WITH") || head.startsWith("CALL")) {
            List<Row> rows = result.collectAsList();
            result.show(1000, false);
            System.out.println(rows.size() + (rows.size() == 1 ? " row" : " rows") + " in set");
        } else {
            System.out.println("[INFO] OK");
        }
    }

    /** {@code ${warehouse_dir}}：warehouse 的本地路径（在 labs/spark/ 之内时用相对路径，输出里不出现本机绝对路径）。 */
    static String substituteVariables(String content) {
        String dir = System.getProperty("warehouse", "warehouse");
        java.nio.file.Path path = Paths.get(dir).toAbsolutePath().normalize();
        java.nio.file.Path cwd = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        String localDir = path.startsWith(cwd) ? cwd.relativize(path).toString() : path.toString();
        String result = content.replace("${warehouse_dir}", localDir);
        for (Map.Entry<String, String> var : VARS.entrySet()) {
            result = result.replace("${" + var.getKey() + "}", var.getValue());
        }
        return result;
    }

    private static void runShell(String display, String command) throws Exception {
        System.out.println();
        System.out.println("$ " + display);
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
