# 11 REST Catalog 的实现

源码：`paimon-api/.../rest/`（`RESTApi`、`HttpClient`、`ResourcePaths`、`auth/*`、`DefaultErrorHandler`）、`paimon-core/.../rest/`（`RESTCatalog`、`RESTCatalogFactory`）、`paimon-common/.../rest/RESTTokenFileIO`、`catalog/CatalogUtils`、`table/CatalogEnvironment`、`catalog/CatalogSnapshotCommit`；规范 `docs/static/rest-catalog-open-api.yaml`；参考服务端 `paimon-core/src/test/.../rest/RESTCatalogServer`

## 学习目标
- 理解 REST Catalog 的模块划分与请求链路（配置、鉴权、重试、错误映射）。
- 理解表加载时注入的三个钩子，以及 REST 模式下的提交 CAS 协议。
- 理解数据凭证下发机制。

## 一句话结论
**元数据管理交给 HTTP 服务端**：数据文件、manifest、snapshot 内容仍由客户端直接读写对象存储，但“哪个快照是最新的”“这次提交成功没有”“谁能访问哪张表”由服务端裁决，用**服务端 CAS** 取代“原子 rename + 外部锁”。

## 1. 模块分工

| 模块 | 类 | 职责 |
|---|---|---|
| `paimon-api/.../rest` | `RESTApi` | 所有 endpoint 的 Java 封装 |
| | `HttpClient` / `HttpClientUtils` | Apache HttpClient 5、指数退避重试、可断点续传的响应流 |
| | `ResourcePaths` | `/v1/{prefix}/databases/{db}/tables/{table}/...` |
| | `requests/*`、`responses/*` | JSON 请求/响应体 |
| | `auth/*` | `bear`（Bearer Token）/ `dlf`（阿里云 DLF 签名） |
| | `DefaultErrorHandler` | HTTP 状态码 → 异常 |
| `paimon-core/.../rest` | `RESTCatalog` | 实现 `Catalog`，每个方法翻译成 `RESTApi` 调用 + 异常转换 |
| | `RESTCatalogFactory` | identifier = `"rest"` |
| `paimon-common/.../rest` | `RESTTokenFileIO` | 数据访问凭证下发 |
| `docs/static/` | `rest-catalog-open-api.yaml` | **OpenAPI 规范**，服务端实现与 pypaimon 共用 |

使用：
```sql
CREATE CATALOG rest_catalog WITH (
  'type' = 'paimon', 'metastore' = 'rest',
  'uri' = 'http://rest-server:8080', 'warehouse' = 'my_wh',
  'token.provider' = 'bear', 'token' = '...'
);
```

## 2. 初始化：先向服务端拉配置
```java
public RESTApi(Options options, boolean configRequired) {
    this.client = new HttpClient(options.get(URI));
    AuthProvider authProvider = createAuthProvider(options);
    Map<String, String> baseHeaders = extractPrefixMap(options, "header.");
    if (configRequired) {
        options = new Options(
            client.get(ResourcePaths.config(),                         // GET /v1/config?warehouse=xxx
                       queryParams, ConfigResponse.class, new RESTAuthFunction(baseHeaders, authProvider))
                  .merge(options.toMap()));                            // 服务端 defaults/overrides 与客户端配置合并
        baseHeaders.putAll(extractPrefixMap(options, "header."));
    }
    this.resourcePaths = ResourcePaths.forCatalogProperties(options);   // 取 prefix
}
```
服务端可下发：URL `prefix`（多租户/多 warehouse 路由）、通用请求头、默认表参数、是否启用数据 token 等。

## 3. 请求链路：鉴权、重试、错误

**鉴权**：每个请求经 `RESTAuthFunction` → `AuthProvider.mergeAuthHeader(baseHeaders, RESTAuthParameter(path, method, body))`：
- `BearTokenAuthProvider`：加 `Authorization: Bearer <token>`；
- `DLFAuthProvider`：用 AK/SK/STS **对请求签名**（`DLFDefaultSigner` / `DLFOpenApiV4Signer`），token 可来自配置、本地文件、ECS 元数据，过期前自动刷新。

**重试**：`ExponentialHttpRequestRetryStrategy(5)`，**只覆盖响应头返回前的失败**。
- 请求体 `isRetrySafe() == false`（如 `CreatePartitionsRequest`）的 POST **只发一次**；
- 响应体读一半断开，`ResumableHttpInputStream` 用 ETag + Range 续传。

**错误映射**（`DefaultErrorHandler`）：

| HTTP | 异常 | `RESTCatalog` 转成 |
|---|---|---|
| 400 | `BadRequestException` | `IllegalArgumentException` |
| 401 / 403 | `NotAuthorizedException` / `ForbiddenException` | `TableNoPermissionException` 等 |
| 404 | `NoSuchResourceException(resourceType, name)` | 按 resourceType 区分表/库/快照不存在 |
| 409 | `AlreadyExistsException` | `TableAlreadyExistException` 等 |
| 500 / 501 / 503 | `ServiceFailure` / `NotImplemented` / `ServiceUnavailable` | |

错误信息带服务端 `requestId`，便于排查。

## 4. 加载表：普通 FileStoreTable + 三个钩子

```java
public Table getTable(Identifier identifier) {
    return CatalogUtils.loadTable(this, identifier,
            path -> fileIOForData(path, identifier),   // 数据 FileIO（可能是 RESTTokenFileIO）
            this::fileIOFromOptions,
            this::loadTableMetadata,                   // GET /v1/{prefix}/databases/{db}/tables/{t}
            ...);
}
```
`GetTableResponse` 含 `schemaId`、`schema`、`path`、`uuid`、`isExternal`、审计字段，据此构造：
```java
new CatalogEnvironment(identifier, metadata.uuid(),
        catalog.catalogLoader(),                 // 表可回调 catalog
        isRestCatalog ? null : lockFactory,      // REST 不需要外部锁
        ..., catalog.supportsVersionManagement() /* REST 为 true */, ...);
FileStoreTableFactory.create(dataFileIO.apply(path), path, schema, catalogEnv);
```

返回的是**普通 `FileStoreTable`**，前面所有章节的逻辑完全复用，差异只在 `CatalogEnvironment` 注入的钩子：

| 钩子 | 文件系统 Catalog | REST Catalog |
|---|---|---|
| `snapshotCommit()` | `RenamingSnapshotCommit` | **`CatalogSnapshotCommit`** → 服务端 |
| `snapshotLoader()` | 无，列目录 / LATEST hint | **`SnapshotLoaderImpl`**：`SnapshotManager.latestSnapshot()` 直接问服务端 |
| `catalogTableRollback()` / `catalogSchemaRollback()` | 本地文件操作 | 服务端 `rollbackTo` / `rollbackSchema` |
| `schemaModification()` | 写 schema 文件 | 走 catalog 的 `alterTable` |

```java
// SnapshotManager.latestSnapshot()
if (snapshotLoader != null) snapshot = snapshotLoader.load().orElse(null);   // REST：问服务端
else                        snapshot = latestSnapshotFromFileSystem();       // 否则：列目录
```
对象存储上列目录慢且可能不一致，**服务端维护“最新快照指针”又准又快**。

## 5. 提交协议：服务端 CAS

```
客户端（Flink committer / Spark driver）                   REST Server
① 自己把数据文件、manifest、manifest list 写到对象存储
② 构造 Snapshot 对象（不自己写 snapshot 目录）
③ CatalogSnapshotCommit.commit
   → POST /v1/{prefix}/databases/{db}/tables/{t}/commit
     CommitTableRequest(tableUuid, baseSnapshotUuid,     ─►  校验 tableUuid（防同名删表重建）
                        snapshot, partitionStatistics)       CAS：当前最新快照 uuid == baseSnapshotUuid ?
                                                             ├─ 是 → 持久化快照、推进最新指针、更新分区统计 → true
                                                             └─ 否 → false
④ false → CommitFailRetryResult → 重读最新快照、冲突检测、重试
   异常 → 不确定 → 下次尝试的幂等检查（commitUser + identifier）判定
```

参考实现 `RESTCatalogServer.commitSnapshot` 核心：
```java
if (!tableId.equals(table.catalogEnvironment().uuid())) throw new TableNotExistException(identifier);
String currentSnapshotUuid = currentSnapshot == null ? null : currentSnapshot.snapshot().uuid();
if (!Objects.equals(currentSnapshotUuid, baseSnapshotUuid))
    return mockResponse(new CommitTableResponse(false), 200);        // CAS 失败
... 提交快照、更新 tableLatestSnapshotStore 与分区统计
```

要点：
- **基于 base 快照 uuid 的 CAS**，不依赖文件系统 rename 原子性，**不需要外部锁**。
- **`CommitTableRequest` 是 retry-safe 的**：重发时若第一次已成功，CAS 失败返回 false，Paimon 重试时通过 `commitUser + identifier` 幂等检查找到自己的快照返回成功——两层保险配合。
- **`tableUuid` 防删表重建**：旧作业的提交会被拒绝。
- **分区统计随提交上报**：服务端维护分区级元数据，`listPartitions` 无需扫 manifest。

## 6. 数据凭证下发：`RESTTokenFileIO`

开启 `data-token.enabled` 后：
```java
public FileIO fileIO() {
    tryToRefreshToken(...);                        // 剩余有效期不足安全阈值就刷新
    FileIO fileIO = FILE_IO_CACHE.getIfPresent(currentToken);
    if (fileIO == null) {
        options = merge(catalogOptions, currentToken.token());   // 如 fs.oss.accessKeyId / securityToken
        fileIO = FileIO.get(path, CatalogContext.create(options, ...));
        FILE_IO_CACHE.put(currentToken, fileIO);
    }
    return fileIO;
}
private void refreshToken() {
    GetTableTokenResponse response = apiInstance.loadTableToken(tableIdentifier);   // GET .../tables/{t}/token
    token = new RESTToken(mergeTokenWithCatalogOptions(response.getToken()), response.getExpiresAtMillis());
}
```
- **计算引擎不持有长期存储密钥**，服务端按表签发临时凭证，权限可细到单表。
- token 自动刷新（双重检查加锁），FileIO 按 token 全局缓存。
- 只取表对象不读数据时用 `ResolvingFileIO`，避免无谓申请 token。
- `CachingFileIO` + `io-cache.*` 可加本地读缓存。

## 7. 服务端托管的其它能力

| 能力 | 方法 |
|---|---|
| 分页/模式/全局列举 | `listTablesPaged`、`listTableDetailsPaged`、`listTablesPagedGlobally` |
| 按 id 查表 | `getTableById` |
| 版本管理 | `loadSnapshot(version)`、`listSnapshotsPaged`、`rollbackTo`、tag、branch、`fastForward` |
| 分区 | `listPartitionsPaged`、`listPartitionsByFilterPaged`、`markDonePartitions` |
| 消费者 | `listConsumersPaged`、`resetConsumer` |
| 其它对象 | 函数、视图、语义视图 |
| 治理 | `permissionManagement`、`policyManagement`、`labelManagement`、`authTableQuery`（行列级鉴权） |

## 8. 与其它 Catalog 对比

| | FileSystem | Hive | REST |
|---|---|---|---|
| 元数据存储 | 纯文件 | HMS + 文件 | 服务端（+ 文件） |
| 提交原子性 | 原子 rename（对象存储需锁） | Hive 锁 | **服务端 CAS** |
| 最新快照 | 列目录 / LATEST hint | 同左 | **问服务端** |
| 存储凭证 | 客户端持长期密钥 | 同左 | **按表临时 token** |
| 权限 | 无 | HMS 级 | 服务端，可到行列级 |
| 多语言 | 需重写文件语义 | 需 HMS 协议 | **HTTP + OpenAPI** |

## 动手实验
- [ ] 跑 `RESTCatalogTest`，对照 `RESTCatalogServer` 与 `rest-catalog-open-api.yaml`。
- [ ] 断点：`RESTApi` 构造（配置合并）、`HttpClient.post`、`CatalogSnapshotCommit.commit` → `RESTCatalogServer.commitSnapshot`、`RESTTokenFileIO.refreshToken`。
- [ ] 动手项目：实现一个最小 REST Server（`config`、`getTable`、`createTable`、`commit`），用 Paimon 客户端写入。

## 自测题
1. REST 模式下为什么不需要外部锁？
2. 提交请求被重发两次会不会重复提交？为什么？
3. `tableUuid` 防的是什么场景？
4. 数据 token 的刷新时机与缓存键是什么？
5. `SnapshotManager.latestSnapshot()` 在 REST 模式下有什么不同？好处是什么？
