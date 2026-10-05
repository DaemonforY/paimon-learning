# 实验 11：Flink 流式写入的两阶段提交

```bash
MAIN_CLASS=learning.paimon.SinkLab ./run.sh [checkpoint|nockpt|failover]      # 默认 all
PAIMON_VERSION=2.2-SNAPSHOT MAIN_CLASS=learning.paimon.SinkLab ./jdb-stacks.sh failover jdb/s2-9-flink-sink.txt 80
```

对应教程 08 章、S2 第 9 讲。嵌入式 Flink 2.2.0 流作业，并行度 1，datagen 每秒 20 行（jdb 调试模式下每秒 5 行），写无桶 Append 表（没有主键去重，重复会直接体现在行数上）。Paimon 2.0.0：

| 场景 | 做法 | 结果 |
|---|---|---|
| checkpoint | 每 2 秒一次 checkpoint，写 100 行 | 每个 checkpoint 完成后提交一个快照，`commit_identifier` = checkpoint id；作业结束（最后一个 checkpoint）的那次提交 identifier = `Long.MAX_VALUE`；共 100 行 |
| nockpt | 不开 checkpoint 的无界流作业，运行 10 秒后取消 | 作业 RUNNING，**0 个快照、0 行、0 个数据文件** |
| failover | 写 200 行，第 120 行第一次处理时抛异常，fixed-delay 重启 | 从最近的 checkpoint 恢复；commit_user 不变、checkpoint id 接着往下；最终 200 行、200 个不同 id；磁盘 parquet 文件数 = 快照引用数（孤儿 0） |

每个 checkpoint 写入多少行随时序变化，以日志为准。

jdb（2.2-SNAPSHOT）抓到两种故障时机：
- `logs/s2-9/jdb-sink-fail-before-cp.log`：断点拖慢了第一个 checkpoint，故障发生在任何 checkpoint 完成之前，作业从头重跑（`isRestored = false`、`restored.size() = 0`）。
- `logs/s2-9/jdb-sink.log`：checkpoint 2 完成、提交端停在 `notifyCheckpointComplete(2)` 时故障；恢复时 `isRestored = true`、状态里 1 个 committable（identifier 2），`filterCommitted` 后补提交 1 个；checkpoint 3 被放弃，之后的 identifier 是 4、5。这个时机是竞态，不保证每次复现。

无桶 Append 表的提交端用 `RestoreCommittableStateManager`（`RowAppendTableSink.java:81-82`）：补提交后**不**故意失败；主键表等默认用 `RestoreAndFailCommittableStateManager`（`FlinkWriteSink.java:65-70`），补提交后故意失败一次（源码，本实验未覆盖）。
