# Paimon 实验项目（labs）

在本地**嵌入式运行 Flink SQL**（Flink MiniCluster）读写 Paimon：不需要安装 Flink 集群；可以在 IDEA 里直接对 Paimon 源码下断点。

## 环境要求

- JDK 11 或 17（macOS 会自动查找；其它系统请设置 `JAVA_HOME`）
- Maven ≥ 3.6.3
- 默认 Paimon 2.0.0（Maven Central 正式版）+ Flink 1.20.1，首次运行自动下载依赖

## 运行

```bash
cd labs && ./run.sh sql/lab01/step1-create-insert.sql
```

| 脚本 | 作用 |
|---|---|
| `run.sh <sql>` | 批模式执行 SQL 文件（逐条执行并打印结果） |
| `stream.sh <sql> [秒数]` | 流模式：最后一条 SELECT 持续读取指定秒数，逐行打印 RowKind 和时间 |
| `debug.sh <sql>` | 调试模式：JVM 在 5005 端口等待 IDEA 连接，并行度 1、心跳超时 1 小时 |
| `jdb-stacks.sh <sql> <断点清单> [次数]` | 不开 IDEA：用 jdb 在断点处打印真实调用栈和变量到 `logs/stacks.log`（断点清单见 `jdb/`，缩进行为命中时执行的 `print` 等命令；需 `PAIMON_VERSION=2.2-SNAPSHOT` 以对齐行号） |
| `lab03-a.sh` / `lab03-b.sh` | 实验 3 编排：后台流读 + 前台写入 |
| `run-all.sh` | 冒烟测试：运行全部实验并断言关键结论 |
| `lab05.sh` | 实验 5：依次去掉一个依赖，复现 4 个报错 |
| `capture-lab01.sh` | 重跑实验 1 并把输出与目录快照留档到 `logs/` |

### 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `PAIMON_VERSION` | `2.0.0` | 对照源码调试时设为本地编译的版本，如 `2.2-SNAPSHOT` |
| `WAREHOUSE` | `warehouse` | Paimon warehouse 目录（相对 labs/） |
| `MAVEN_MIRROR` | 空 | 设为 `aliyun` 使用阿里云镜像（`maven-settings-aliyun.xml`） |
| `JAVA_HOME` | 自动 | JDK 路径 |
| `EXCLUDE_JARS` / `EXTRA_JARS` | 空 | 从 classpath 去掉匹配正则的 jar / 追加 jar（实验 5 用） |

想从头再来：`rm -rf warehouse`。

## SQL 文件里的约定

- `${warehouse}`：替换为 warehouse 的 URI（用于 `CREATE CATALOG`）
- `${warehouse_dir}`：替换为 warehouse 的本地路径（用于 `-- @sh` 查看目录）
- `SET 'k' = 'v';`：设置参数
- `-- @sh <命令>`：执行 shell 命令并打印输出
- `-- @expect-error`：下一条语句预期失败，只打印异常根因，不中断脚本
- `-- @set NAME <命令>`：执行 shell 命令，把输出（去掉首尾空白）存为变量 `${NAME}`，供后面的语句使用（例如从快照文件读出提交时间，见 `sql/lab02/time-travel-by-time.sql`）

## 实验列表

| 实验 | 目录 | 主题 | 对应教程 |
|---|---|---|---|
| lab01 | `sql/lab01/` | 建表、写入、更新、删除、系统表、手动合并 | 02、04 |
| lab02 | `sql/lab02/` | 时间旅行、Tag、增量读取、快照过期三道保险 | 02、09 |
| lab03 | `sql/lab03/`、`lab03-a.sh`、`lab03-b.sh` | 流式读取：changelog-producer 对比、consumer-id | 07、09 |
| lab04 | `sql/lab04/`、`debug.sh` | IDEA 远程调试，断点追踪一次写入 | 02、05、08 |
| lab05 | `sql/lab05/`、`lab05.sh` | 嵌入式运行缺依赖的 4 个典型报错（逐个去掉 jar 复现） | 01 |
| lab06 | `sql/lab06/` | Schema 演进：字段 id 如何对应新旧文件里的列 | 02 |
| lab07 | `CommitLab.java`（`MAIN_CLASS=learning.paimon.CommitLab ./run.sh`） | 并发提交：抢快照号与重试、文件删除冲突、提交幂等 | 05 |
| lab08 | `sql/lab08/`、`jdb/s2-6-read-path.txt` | 读路径：直接读 vs 合并读、过滤下推、COUNT(*) 下推 | 06 |

实验记录见 `notes/`。

## 在 IDEA 中对照源码调试

1. 克隆 Apache Paimon 源码并切到教程基线：`git checkout d15d250cf`，按 [01 章](../01-环境搭建与源码编译.md) 编译安装（得到 `2.2-SNAPSHOT`）。
2. 在 IDEA 中打开 Paimon 源码工程，新建 `Remote JVM Debug`（localhost:5005，module classpath 选 `paimon-flink-1.20`）。
3. 启动：

```bash
cd labs && PAIMON_VERSION=2.2-SNAPSHOT ./debug.sh sql/lab04/trace-write.sql
```

4. IDEA 中点 Debug 连接。断点清单见 [lab04-断点追踪写入](notes/lab04-断点追踪写入.md)。

> **断点必须与运行的 jar 版本一致**：教程中的行号基于 `d15d250cf`。用 2.0.0 运行时断点会错位。

## 常见问题

| 现象 | 原因 / 处理 |
|---|---|
| `NoClassDefFoundError` | 依赖未下载完整：删除 `target/classpath-*.txt` 后重跑 |
| 作业卡住不动 | 不要在受限沙箱/容器里运行：MiniCluster 需要监听本地端口 |
| 下载依赖很慢 | `MAVEN_MIRROR=aliyun` |
| 调试时作业失败 | 用 `debug.sh`（已调大心跳超时），不要直接给 `run.sh` 加调试参数 |

依赖说明（实验 5 逐个验证过）：嵌入式运行需要自己补齐 `flink-connector-files`、log4j2（含 `log4j-1.2-api`，Hadoop 需要）、`flink-shaded-hadoop-2-uber`。前两者 Flink 发行版的 `lib/` 里自带；**Hadoop 发行版不带**，生产部署同样要自己提供（放入 shaded Hadoop jar 或设置 `HADOOP_CLASSPATH`）。`flink-connector-files` 已经把 `flink-connector-base` 的类打包在内，不需要单独引入。
