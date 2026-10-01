# 实验 5 记录：嵌入式运行 Paimon 时缺依赖的 4 个报错

日期：2026-10-02　|　对应教程 01 章　|　版本：Paimon 2.0.0 + Flink 1.20.1

运行：
```bash
cd labs && ./lab05.sh
```
同一段 SQL（建表 → 写入 → 读取），每个场景只改动一处 classpath（`EXCLUDE_JARS` 去掉 jar、`EXTRA_JARS` 追加 jar），完整日志在 `logs/lab05-<场景>.log`。

## 结果

| 场景 | classpath 改动 | 失败语句 | 根因 |
|---|---|---|---|
| 1 | 去掉 `flink-shaded-hadoop-2-uber` | INSERT | `ClassNotFoundException: org.apache.hadoop.conf.Configuration` |
| 2 | 去掉 `log4j-1.2-api` | INSERT | `ClassNotFoundException: org.apache.log4j.Level` |
| 3 | 去掉 `flink-connector-files` | SELECT | `ClassNotFoundException: org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase` |
| 4 | 去掉 `flink-connector-files`，**只补** `flink-connector-base` | SELECT | `ClassNotFoundException: org.apache.flink.connector.file.src.reader.BulkFormat$RecordIterator` |
| 5 | 依赖齐全（**不含** `flink-connector-base`） | — | 写入、读取全部成功 |

## 解读

1. **写入阶段**需要 Hadoop 类（场景 1）和 log4j 1.x API（场景 2，Hadoop 的 `UserGroupInformation` 初始化时用到 `org.apache.log4j.Level`）。
2. **读取阶段**才需要 Flink 的 connector 类：Paimon 的 `FileStoreSourceReader` 继承 `SingleThreadMultiplexSourceReaderBase`，`FileStoreSourceSplitReader` 返回 `BulkFormat.RecordIterator`。所以会出现“写入成功、一读就挂”。
3. **场景 3 → 4 是一个容易走的弯路**：看到 `connector.base` 包下的类缺失，自然会去加 `flink-connector-base`，结果换成了场景 4 的报错。实际上：

   ```
   $ unzip -l flink-connector-files-1.20.1.jar | grep -c org/apache/flink/connector/base/
   107
   ```
   `flink-connector-files` 的 pom 用 maven-shade-plugin 把 `flink-connector-base` 打包进来了（`<include>org.apache.flink:flink-connector-base</include>`），**只加 `flink-connector-files` 一个就够**，场景 5 验证了这一点。

## 和 Flink 发行版的关系

查 Flink 1.20.1 的打包配置 `flink-dist/src/main/assemblies/bin.xml`：

| 依赖 | 发行版 `lib/` 是否自带 |
|---|---|
| log4j2（含 `log4j-1.2-api`） | ✅ |
| `flink-connector-files`（含 connector-base 的类） | ✅ |
| Hadoop | ❌ —— 生产部署也要自己提供：放入 shaded Hadoop jar，或设置 `HADOOP_CLASSPATH` |

## 思考题
1. 为什么场景 1、2 失败在 INSERT，而场景 3、4 失败在 SELECT？
2. 如果生产环境的 Flink 集群没有配置 Hadoop，Paimon 作业会在哪一步报什么错？
3. 怎样快速判断一个 jar 里是否已经 shade 了另一个依赖？（提示：`unzip -l`、查看 pom 中的 maven-shade-plugin 配置）
