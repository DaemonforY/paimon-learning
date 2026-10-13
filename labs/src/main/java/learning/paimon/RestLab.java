package learning.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogServer;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 实验 14：REST Catalog（S2 第 12 讲）。
 *
 * <p>在进程内启动 Paimon 测试代码里的参考服务端 {@code RESTCatalogServer}（paimon-core 的 tests jar），
 * 客户端用 {@code metastore = rest} 连接，打印服务端收到的每个 HTTP 请求。
 *
 * <ul>
 *   <li>flow：Flink SQL 经 REST Catalog 建表、写两次、查询，看请求序列；
 *   <li>cas：带着过期的 base 快照 uuid 调用 commitSnapshot，看服务端是否拒绝；
 *   <li>uuid：拿着旧表对象，删表、同名重建后再提交；
 *   <li>token：服务端下发 data-token.enabled，读写数据前先要临时凭证。
 * </ul>
 *
 * <p>运行：{@code MAIN_CLASS=learning.paimon.RestLab ./run.sh [flow|cas|uuid|token]}（默认 all）
 */
public class RestLab {

    private static final String TOKEN = "lab14-token";
    private static final String WAREHOUSE = "lab14_wh";
    private static RESTCatalogServer server;
    private static MockWebServer http;
    private static Catalog catalog;

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            String only = args.length > 0 ? args[0] : "all";
            boolean token = only.equals("token");
            startServer(token);
            if (only.equals("all") || only.equals("flow")) {
                flow();
            }
            if (only.equals("all") || only.equals("cas")) {
                cas();
            }
            if (only.equals("all") || only.equals("uuid")) {
                uuid();
            }
            if (token) {
                token();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    private static void startServer(boolean dataToken) throws Exception {
        String dataPath = Paths.get(System.getProperty("warehouse", "warehouse"), "rest-data")
                .toAbsolutePath().normalize().toUri().toString();
        Map<String, String> defaults = new HashMap<>();
        defaults.put("prefix", "paimon");
        defaults.put("warehouse", WAREHOUSE);
        if (dataToken) {
            // 由服务端下发：客户端从 GET /v1/config 拿到这个选项
            defaults.put("data-token.enabled", "true");
        }
        server = new RESTCatalogServer(dataPath, new BearTokenAuthProvider(TOKEN),
                new ConfigResponse(defaults, new HashMap<>()), WAREHOUSE);
        server.start();
        Field f = RESTCatalogServer.class.getDeclaredField("server");
        f.setAccessible(true);
        http = (MockWebServer) f.get(server);
        System.out.println("[RestLab] REST 服务端已启动（参考实现 RESTCatalogServer）" + (dataToken ? "，下发 data-token.enabled = true" : ""));
        catalog = CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(clientOptions())));
        printRequests("客户端创建 RESTCatalog");
    }

    private static Map<String, String> clientOptions() {
        Map<String, String> o = new LinkedHashMap<>();
        o.put("metastore", "rest");
        o.put("uri", server.getUrl());
        o.put("warehouse", WAREHOUSE);
        o.put("token.provider", "bear");
        o.put("token", TOKEN);
        return o;
    }

    // ---------------------------------------------------------------------------------------------

    private static void flow() throws Exception {
        title("场景 1：Flink SQL 经 REST Catalog 建表、写入、查询");
        Configuration conf = new Configuration();
        conf.setString("parallelism.default", "1");
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().withConfiguration(conf).inBatchMode().build());
        tEnv.getConfig().set("table.dml-sync", "true");
        StringBuilder ddl = new StringBuilder("CREATE CATALOG rest WITH ('type' = 'paimon'");
        for (Map.Entry<String, String> e : clientOptions().entrySet()) {
            String v = e.getKey().equals("uri") ? "${uri}" : e.getValue();
            ddl.append(", '").append(e.getKey()).append("' = '").append(v).append("'");
        }
        ddl.append(")");
        sql(tEnv, ddl.toString());
        printRequests("CREATE CATALOG");
        sql(tEnv, "CREATE DATABASE rest.db1");
        printRequests("CREATE DATABASE");
        sql(tEnv, "CREATE TABLE rest.db1.orders (id INT, status STRING, PRIMARY KEY (id) NOT ENFORCED) WITH ('bucket' = '1')");
        printRequests("CREATE TABLE");
        sql(tEnv, "INSERT INTO rest.db1.orders VALUES (1, 'CREATED'), (2, 'CREATED')");
        printRequests("第一次 INSERT");
        sql(tEnv, "INSERT INTO rest.db1.orders VALUES (1, 'PAID')");
        printRequests("第二次 INSERT");
        sql(tEnv, "SELECT * FROM rest.db1.orders");
        printRequests("SELECT");
        sql(tEnv, "SELECT snapshot_id, commit_kind, total_record_count FROM rest.db1.`orders$snapshots`");
    }

    private static void cas() throws Exception {
        title("场景 2：服务端 CAS——带着过期的 base 快照去提交");
        catalog.createDatabase("db2", true);
        Identifier id = Identifier.create("db2", "logs");
        catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT()).column("v", DataTypes.STRING()).build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(id);
        StreamWriteBuilder b = table.newStreamWriteBuilder().withCommitUser("job-A");
        try (StreamTableWrite w = b.newWrite(); StreamTableCommit c = b.newCommit()) {
            for (long i = 1; i <= 2; i++) {
                w.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
                c.commit(i, w.prepareCommit(false, i));
            }
        }
        printRequests("建表并正常提交 2 次");
        org.apache.paimon.Snapshot s1 = table.snapshotManager().snapshot(1);
        org.apache.paimon.Snapshot s2 = table.snapshotManager().latestSnapshot();
        String tableUuid = table.catalogEnvironment().uuid();
        System.out.println("服务端记录的最新快照 = " + s2.id() + "，uuid = " + s2.uuid());
        System.out.println("快照 1 的 uuid = " + s1.uuid());
        // 假装是一个在快照 1 之后就开始准备、此时才来提交的作业：base 还停留在快照 1
        boolean ok = catalog.commitSnapshot(id, tableUuid, s1.uuid(), s2, java.util.Collections.emptyList());
        System.out.println("commitSnapshot(base = 快照 1 的 uuid) 返回 " + ok);
        printRequests("带过期 base 提交");
        ok = catalog.commitSnapshot(id, tableUuid, null, s2, java.util.Collections.emptyList());
        System.out.println("commitSnapshot(base = null，即“我以为表还是空的”) 返回 " + ok);
        printRequests("带 null base 提交");
        FileStoreTable latest = (FileStoreTable) catalog.getTable(id);
        System.out.println("提交被拒之后，最新快照仍是 " + latest.snapshotManager().latestSnapshotId());
    }

    private static void uuid() throws Exception {
        title("场景 3：拿着旧表对象，删表、同名重建后再提交");
        catalog.createDatabase("db3", true);
        Identifier id = Identifier.create("db3", "t");
        Schema schema = Schema.newBuilder().column("id", DataTypes.BIGINT()).column("v", DataTypes.STRING()).build();
        catalog.createTable(id, schema, false);
        FileStoreTable oldTable = (FileStoreTable) catalog.getTable(id);
        System.out.println("旧表 uuid = " + oldTable.catalogEnvironment().uuid());
        StreamWriteBuilder b = oldTable.newStreamWriteBuilder().withCommitUser("old-job");
        StreamTableWrite w = b.newWrite();
        w.write(GenericRow.of(1L, BinaryString.fromString("old")));
        catalog.dropTable(id, false);
        catalog.createTable(id, schema, false);
        System.out.println("同名重建后的 uuid = " + ((FileStoreTable) catalog.getTable(id)).catalogEnvironment().uuid());
        drain();
        try (StreamTableCommit c = b.newCommit()) {
            c.commit(1, w.prepareCommit(false, 1));
            System.out.println("[WARN] 旧表对象的提交成功了");
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            System.out.println("[EXPECTED ERROR] " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("[EXPECTED ERROR] 根因 " + root.getClass().getSimpleName() + ": " + root.getMessage());
        }
        w.close();
        printRequests("旧表对象提交");
    }

    private static void token() throws Exception {
        title("场景 4：服务端下发 data-token.enabled，读写数据前先要临时凭证");
        catalog.createDatabase("db4", true);
        Identifier id = Identifier.create("db4", "t");
        catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT()).column("v", DataTypes.STRING()).build(), false);
        Map<String, String> fake = new HashMap<>();
        fake.put("fs.lab.token", "temp-" + UUID.randomUUID().toString().substring(0, 8));
        server.setDataToken(id, new RESTToken(fake, System.currentTimeMillis() + 3_600_000L));
        printRequests("建表并在服务端登记临时凭证");
        Table table = catalog.getTable(id);
        printRequests("getTable");
        System.out.println("表的 FileIO = " + table.fileIO().getClass().getSimpleName());
        try {
            // 第一次碰数据文件：RESTTokenFileIO 先向服务端要这张表的临时凭证，再用它构造真正的 FileIO
            table.fileIO().exists(((FileStoreTable) table).location());
            System.out.println("访问表目录成功");
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            // 参考服务端配套的测试 FileIO（RESTTestFileIO）按它自己的路径约定校验凭证，本地路径对不上
            System.out.println("[mock 限制] 测试用 FileIO 校验失败：" + root.getClass().getSimpleName() + ": " + root.getMessage());
        }
        printRequests("第一次访问数据文件");
    }

    // ---------------------------------------------------------------------------------------------

    private static List<RecordedRequest> drain() throws InterruptedException {
        List<RecordedRequest> list = new ArrayList<>();
        RecordedRequest r;
        while ((r = http.takeRequest(200, TimeUnit.MILLISECONDS)) != null) {
            list.add(r);
        }
        return list;
    }

    /** 打印这一步客户端发出的请求：方法 + 路径（去掉查询参数）。请求体已被服务端分发时读走，这里取不到。 */
    private static void printRequests(String step) throws InterruptedException {
        List<RecordedRequest> list = drain();
        System.out.println("[REST] " + step + "：" + list.size() + " 个请求");
        for (RecordedRequest r : list) {
            String path = r.getPath() == null ? "" : r.getPath().replaceAll("\\?.*$", "");
            String extra = "";
            System.out.println("         " + r.getMethod() + " " + path + extra);
        }
    }

    private static void sql(TableEnvironment tEnv, String stmt) {
        System.out.println();
        System.out.println("Flink SQL> " + stmt + ";");
        String real = stmt.replace("${uri}", server.getUrl());
        String head = stmt.trim().toUpperCase();
        if (head.startsWith("SELECT")) {
            tEnv.executeSql(real).print();
        } else {
            tEnv.executeSql(real);
            System.out.println("[INFO] OK");
        }
    }

    private static void title(String t) {
        System.out.println();
        System.out.println("==================== " + t + " ====================");
    }
}
