# Paimon 源码学习：每个结论都能复现

一套 **Apache Paimon 源码级教程 + 可一键复现的实验**。

- **不讲“据说”**：每个结论都给出实验输出，或精确到行的源码位置。
- **可复现**：`labs/` 里的实验不需要安装 Flink 集群，一条命令运行；`run-all.sh` 一键验证全部关键结论。
- **可调试**：实验以嵌入式 Flink 运行，能在 IDEA 里直接对 Paimon 源码打断点。

> 版本基线：教程源码引用基于 Apache Paimon master @ `d15d250cf`（2.2-SNAPSHOT）；实验默认运行 Paimon **2.0.0** 正式版。详见 [勘误与版本说明](勘误.md)。
> 本项目与 Apache 软件基金会无隶属关系。

## 快速开始

环境：JDK 11 或 17、Maven ≥ 3.6.3。不需要安装 Flink。

```bash
git clone https://github.com/DaemonforY/paimon-learning.git
```

```bash
cd paimon-learning/labs && ./run.sh sql/lab01/step1-create-insert.sql
```

国内网络建议使用阿里云镜像：

```bash
cd paimon-learning/labs && MAVEN_MIRROR=aliyun ./run.sh sql/lab01/step1-create-insert.sql
```

第一次运行会下载依赖（Paimon、Flink 等），之后几秒钟即可启动。看到下面的输出就成功了：

```
Successfully commit snapshot 1 to table orders ... and kind APPEND.
...
5 rows in set
```

验证全部实验结论（约 6~8 分钟）：

```bash
cd paimon-learning/labs && ./run-all.sh
```

## 目录

| 章节 | 主题 |
|---|---|
| [00 学习计划](00-学习计划.md) | 14 周从入门到专家的学习路线 |
| [01 环境搭建](01-环境搭建与源码编译.md) | 源码编译、IDEA 导入、编译坑 |
| [02 整体架构](02-paimon-core整体架构.md) | paimon-core 分层、存储布局、三条主流程 |
| [03 合并策略](03-UniversalCompaction选文件策略.md) | UniversalCompaction 怎么选文件 |
| [04 合并执行](04-MergeTreeCompactTask合并执行.md) | 升级 vs 重写 |
| [05 提交](05-FileStoreCommitImpl冲突检测与重试.md) | 乐观并发、冲突检测、重试、幂等 |
| [06 读路径](06-读路径与删除向量.md) | MergeFileSplitRead 与删除向量 |
| [07 Lookup Changelog](07-Lookup-Changelog.md) | -U/+U 是怎么产生的 |
| [08 FlinkSink](08-FlinkSink与Checkpoint提交.md) | 两阶段提交与 exactly-once |
| [09 FlinkSource](09-FlinkSource流式读取.md) | 流式读取与 consumer-id |
| [10 Spark MERGE INTO](10-Spark-MERGE-INTO.md) | 三条实现路径 |
| [11 REST Catalog](11-REST-Catalog.md) | 服务端 CAS 与凭证下发 |
| [附录](附录-配置速查与调试技巧.md) | 配置速查、源码索引、调试技巧、排障表 |

## 实验

| 实验 | 主题 | 记录 |
|---|---|---|
| lab01 | 建表、更新删除、系统表、手动合并 | [lab01-记录](labs/notes/lab01-记录.md) |
| lab02 | 时间旅行、Tag、增量读取、快照过期 | [lab02-记录](labs/notes/lab02-记录.md) |
| lab03 | 流式读取：changelog-producer 对比、consumer-id | [lab03-记录](labs/notes/lab03-记录.md) |
| lab04 | IDEA 远程调试，断点追踪一次写入 | [lab04-断点追踪写入](labs/notes/lab04-断点追踪写入.md) |

运行方式与参数见 [labs/README.md](labs/README.md)。

## 参与

- 发现错误：请提 Issue，注明章节、原文、正确说法与依据，确认后记录在 [勘误](勘误.md)。
- 欢迎补充实验与章节。

## 许可

文档采用 [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/)，`labs/` 代码采用 Apache License 2.0，详见 [LICENSE](LICENSE)。
