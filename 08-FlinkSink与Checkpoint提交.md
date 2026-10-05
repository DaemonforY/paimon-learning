# 08 FlinkSink 与 Checkpoint 两阶段提交

源码（`paimon-flink/paimon-flink-common/.../flink/sink/`）：`FlinkSink`、`PrepareCommitOperator`、`TableWriteOperator`、`StoreSinkWriteImpl`、`CommitterOperator`、`StoreCommitter`、`RestoreAndFailCommittableStateManager`、`StateUtils`；core 中 `table/sink/TableCommitImpl`

## 学习目标
- 画出 checkpoint N 从 writer 到 committer 的完整时序。
- 理解 `commitUser` 与 `identifier` 如何支撑幂等。
- 掌握各种故障时刻下 exactly-once 的保证方式。

## 一句话结论
**两阶段提交**：Prepare = barrier 之前 writer 把数据刷成文件并把元信息发给 committer，committer 存入 checkpoint 状态；Commit = checkpoint 完成后 committer（单并发）以 `checkpointId` 为 `identifier` 提交快照。

## 1. 拓扑：`FlinkSink.sinkFrom`

```java
DataStream<Committable> written = doWrite(input, initialCommitUser, null);   // 写，不产生快照
return doCommit(written, initialCommitUser);                                 // 提交，产生快照
```
```
Source ──(按 partition/bucket shuffle)──► Writer × N ──► [Changelog Compact 可选] ──► Global Committer × 1 ──► end
                                          TableWriteOperator                          CommitterOperator(并行度 1)
```
流作业两条硬性要求（`assertStreamingConfiguration`）：
- checkpoint 模式必须 **EXACTLY_ONCE**；
- **不支持 unaligned checkpoint**（coordinator commit 模式除外）：committable 在 barrier 之前发出，对齐 barrier 才能保证 committer 做快照前收齐 checkpoint N 的所有 committable。

## 2. 一次 checkpoint 的时序

```
Writer（每个 subtask）                     Committer（1 个 subtask）
① processElement：写数据，缓冲满 flush 数据文件
   （文件已在最终路径，但无快照引用）
② prepareSnapshotPreBarrier(N)  ─────►   processElement：放入 inputs
   prepareCommit(false, N)
   发出 Committable(N, msg)
③ snapshotState（commitUser 等）
   barrier N ───────────────────────►    ④ snapshotState
                                            pollInputs：按 cpId 分组 → ManifestCommittable(N)
                                            committablesPerCheckpoint[N] = ...
                                            committableStateManager 存入状态
                     … JobManager：checkpoint N 完成 …
                                          ⑤ notifyCheckpointComplete(N)
                                            commitUpToCheckpoint(N) → FileStoreCommitImpl.commit
                                            → 生成快照，异步 maintain()
```

### ② writer：`prepareSnapshotPreBarrier`
```java
public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    if (!endOfInput) emitCommittables(false, checkpointId);
}
```
在 barrier 下发前调用，保证 committable 先于 barrier 到达。调用链：
```
TableWriteOperator.prepareCommit → StoreSinkWriteImpl.prepareCommit(waitCompaction, cpId)
  → TableWriteImpl.prepareCommit → MergeTreeWriter.prepareCommit
  → 每个 CommitMessage 包装成 new Committable(checkpointId, message)
```
lookup 模式下 `waitCompaction` 会等 L0 合并完成，所以 lookup 表 checkpoint 更慢。

### ④ committer：存下待提交内容
```java
public void snapshotState(StateSnapshotContext context) throws Exception {
    pollInputs();
    committer.snapshotState();
    committableStateManager.snapshotState(committables(committablesPerCheckpoint));
}
```
- `StoreCommitter.combine` 把一个 checkpoint 的所有 `CommitMessage` 合成一个 `ManifestCommittable(identifier = checkpointId)`。
- `committablesPerCheckpoint` 是 `TreeMap`；同一 checkpoint 重复出现报 “Repeatedly commit the same checkpoint files”。
- 这一步完成，Prepare 阶段就持久化了。

### ⑤ checkpoint 完成后提交
```java
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    commitUpToCheckpoint(endInput ? END_INPUT_CHECKPOINT_ID : checkpointId);
}
private void commitUpToCheckpoint(long checkpointId) throws Exception {
    NavigableMap<Long, GlobalCommitT> headMap = committablesPerCheckpoint.headMap(checkpointId, true);
    committer.commit(committables(headMap));
    headMap.clear();
}
```
**为什么用 headMap(≤N)**：Flink 不保证 `notifyCheckpointComplete` 必达；N-1 的通知丢了，N 来时一起按顺序提交。

`TableCommitImpl.commitMultiple`：逐个 `FileStoreCommitImpl.commit`，然后异步 `maintain()`（自动 tag、分区过期、consumer 过期、快照过期）；maintain 失败会在下一次提交时抛出。

## 3. commitUser：作业的稳定身份
```java
commitUser = StateUtils.getSingleValueFromState(context, "commit_user_state", String.class, initialCommitUser);
```
- 新作业用随机 UUID（`createCommitUser`），写入 **union list state**；
- 重启 / 从 savepoint 恢复时从状态读回，保持不变（不用 job id，因为 savepoint 恢复 job id 会变）；
- 幂等键 = **`commitUser + identifier(checkpointId)`**。

## 4. 故障恢复

### committer：`RestoreAndFailCommittableStateManager` / `RestoreCommittableStateManager`

两种状态管理器的区别只在“补提交之后要不要故意失败”。`FlinkWriteSink` 默认用 `RestoreAndFail…`（`FlinkWriteSink.java:65-70`）；**无桶 Append 表（`RowAppendTableSink.java:81-82`）和 postpone bucket 表用 `RestoreCommittableStateManager`，只补提交、不故意失败**（`FlinkWriteSink.java:87-93`，`RestoreCommittableStateManager.java:76-79`）。实验 11（`labs` 的 `SinkLab`）在无桶 Append 表上实测：恢复时补提交了 1 个 committable，作业没有再重启。以下流程针对 `RestoreAndFail…`：
```
从状态恢复未提交的 ManifestCommittable
  → committer.filterAndCommit(committables, checkAppendFiles = true)
      → TableCommitImpl.filterAndCommitMultiple
          按 identifier 排序 → filterCommitted（跳过已提交）→ checkFilesExistence → commitMultiple
  → 若真的补提交了 → 故意抛 RuntimeException → 作业再重启一次
```
**为什么故意失败**：writer 与 committer 并行初始化，writer 恢复 LSM 层级时读的是补提交**之前**的快照，文件视图过期，继续合并会冲突。再重启一次让 writer 基于新快照重建。

### writer：从最新快照恢复
writer 不在状态里存数据，重启后 `FileSystemWriteRestore.restoreFiles` 从最新快照读 bucket 文件重建 `Levels`，sequence number 接续。最后一次成功 checkpoint 之后写的文件成为孤儿文件，由 `remove_orphan_files` 清理。

### 故障时刻 → 结果

| 故障时刻 | 结果 |
|---|---|
| checkpoint N 完成前 | N 不在 checkpoint 中，source 从 N-1 重放，N 的文件成孤儿。**不重不丢** |
| checkpoint N 完成后、提交前 | 状态里有 N，重启时 filterAndCommit 补提交；`RestoreAndFail…` 再故意失败一次刷新 writer（无桶 Append 表不会） |
| 提交过程中（不确定成败） | filterCommitted 按 identifier 判断：提交过就跳过，没提交就补 |
| **从很旧的 savepoint 恢复** | 引用的文件可能已被合并、过期 → 冲突或文件缺失 |

## 5. 批模式 / 有界输入
```java
// Writer：endInput
emitCommittables(true, Long.MAX_VALUE);       // waitCompaction=true

// Committer：endInput
if (streamingCheckpointEnabled) return;       // 流 + checkpoint：等最后一个 checkpoint
pollInputs();
commitUpToCheckpoint(END_INPUT_CHECKPOINT_ID); // 批：立刻 filterAndCommit
```
`INSERT OVERWRITE` 走 overwrite 分支，只允许一个 committable。

## 6. Coordinator Commit 模式
`sink.coordinator-commit.enabled=true`：不建 global committer 算子，提交在 JobManager 的 `OperatorCoordinator` 中执行。好处是 region failover 不必重启整条链路、允许 unaligned checkpoint；目前**仅支持 unaware-bucket 的 append 表流式写入**。

## 7. 实践要点
- **可见性延迟 ≈ checkpoint 间隔 + 提交耗时**。
- **checkpoint 间隔决定小文件数量**。
- **lookup / DV 表 checkpoint 更慢**（等 L0 合并）。
- **committer 并行度为 1**，大表可把过期等维护挪到独立作业（`write-only` + 独立 compaction）。
- **保持 sink 算子 UID 稳定**（`sink.operator-uid.suffix`），否则状态对不上、commitUser 丢失。

## 动手实验
- [ ] 跑 `CommitterOperatorTest`（“checkpoint 完成后重启”“重复提交同一 checkpoint”用例）。
- [ ] 断点：`prepareSnapshotPreBarrier`、`CommitterOperator.snapshotState / notifyCheckpointComplete`、`RestoreAndFailCommittableStateManager.recover`。
- [ ] 日志：committer 的 `Successfully commit snapshot {} ... with identifier {}`，identifier 与 Flink UI 的 checkpoint id 对照。

## 自测题
1. 为什么 committable 要在 `prepareSnapshotPreBarrier` 中发出？
2. 为什么 Paimon sink 不支持 unaligned checkpoint？
3. 为什么提交用 `headMap(≤N)` 而不是只提交 N？
4. 恢复时补提交成功后为什么要故意让作业失败？
5. 为什么 commitUser 不能用 job id？
