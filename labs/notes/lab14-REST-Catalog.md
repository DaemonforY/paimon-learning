# 实验 14：REST Catalog

```bash
MAIN_CLASS=learning.paimon.RestLab ./run.sh            # flow + cas + uuid
MAIN_CLASS=learning.paimon.RestLab ./run.sh token      # 服务端下发 data-token.enabled
```

对应教程 11 章、S2 第 12 讲。进程内启动 Paimon 2.0.0 测试代码里的参考服务端 `RESTCatalogServer`（`paimon-core-2.0.0-tests.jar`，基于 okhttp `MockWebServer`），客户端 `metastore = rest`，打印服务端收到的每个请求。**这是测试用 mock，不是生产服务端。**

| 场景 | 结果 |
|---|---|
| flow | 创建 catalog 先 `GET /v1/config`；每次 INSERT：`GET …/snapshot` ×2、`POST …/commit`、`GET …/snapshot`；没有上传数据的请求 |
| cas | 带快照 1 的 uuid 或 null 作为 base 调 `commitSnapshot`：都返回 false，最新快照仍是 2 |
| uuid | 旧表对象在删表重建后提交：服务端按表 uuid 判“表不存在”；客户端当作不确定异常退避重试 11 次（`Retry commit for exception.`），`Commit failed … with 10 retries`，根因 `NoSuchResourceException: Table db3.t does not exist.` |
| token | 表的 FileIO 为 `RESTTokenFileIO`，第一次访问数据前 `GET …/tables/t/token`；mock 配套的测试 FileIO 按自己的路径约定校验凭证，本地对不上，未能真正读写 |

依赖与坑：
- `pom.xml` 加了 `paimon-core` tests jar（排除传递依赖）与 okhttp / okio-jvm；`flink-shaded-hadoop-2-uber` 带了未 shade 的老 okio，okio 必须排在它前面，否则 `NoSuchFieldError: Companion`。
- tests jar 里的 `log4j2-test.properties`（rootLogger OFF）会盖掉 labs 的日志配置，`run.sh` 显式指定 `-Dlog4j.configurationFile=log4j2.properties`。
- 两个线程并发提交时 mock 的分发逻辑抛 `ConcurrentModificationException`，所以 cas 场景改为可复现的直接调用。
