# 实验 12：Flink 流读 Paimon

```bash
MAIN_CLASS=learning.paimon.SourceLab ./run.sh [read|plan]      # 默认 all
PAIMON_VERSION=2.2-SNAPSHOT MAIN_CLASS=learning.paimon.SourceLab ./jdb-stacks.sh read jdb/s2-10-flink-source.txt 60
```

对应教程 09 章、S2 第 10 讲（consumer-id 的过期保护与续读见实验 3）。嵌入式 Flink 2.2.0，Paimon 2.0.0。

**场景 1（read）**：后台流读主键表 `src_t`（2 个 bucket、changelog-producer = none、source 并行度 2、discovery-interval 1 秒），前台依次：

| 写入 | 快照 | 流读输出 |
|---|---|---|
| 表为空时启动 | — | 无 |
| INSERT (1,a)(2,b)(3,c) | 1 APPEND | `+I[1,a]` `+I[2,b]` `+I[3,c]`（第一次规划读全量） |
| INSERT (1,A) | 2 APPEND | `-U[1,a]` `+U[1,A]`（none 表由 ChangelogNormalize 补 -U） |
| CALL sys.compact | 3 COMPACT | 无（DeltaFollowUpScanner 跳过 COMPACT） |
| DELETE k = 2 | 4 APPEND | `-D[2,b]` |

**场景 2（plan）**：同一条流读 SQL 的 JSON 执行计划——不配 consumer-id：`Source: src_p → DropUpdateBefore`（FLIP-27 的 ContinuousFileStoreSource）；配 `consumer-id`（默认 `consumer.mode = exactly-once`）：`Source: paimon.default.src_p-Monitor → src_p → DropUpdateBefore`（MonitorSource 生成 split，下游 ReadOperator 读）。

jdb（2.2-SNAPSHOT）：Enumerator 的方法在 `SourceCoordinator-Source: src_t[1]` 线程，规划（`DataTableStreamScan.plan`）在 `…-worker-thread-1`；第一次规划 2 个 split，之后 `nextSnapshotId` 2 → 3 → 5；快照 3（COMPACT）`shouldScanSnapshot = false`；Reader 在 `Source Data Fetcher for Source: src_t[1] (k/2)` 线程按 split 读。
