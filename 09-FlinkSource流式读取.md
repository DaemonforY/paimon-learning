# 09 FlinkSource 流式读取与 consumer-id

源码：flink 侧 `flink/source/`：`FlinkSourceBuilder`、`ContinuousFileStoreSource`、`ContinuousFileSplitEnumerator`、`FileStoreSourceReader`、`FileStoreSourceSplitReader`、`FileStoreSourceSplitState`、`ConsumerProgressCalculator`、`operator/MonitorSource`、`BaseDataTableSource`；core 侧 `table/source/DataTableStreamScan`、`snapshot/*FollowUpScanner`、`utils/NextSnapshotFetcher`、`table/ExpireSnapshotsImpl`

## 学习目标
- 理解流读的两阶段（起点 + 逐快照追踪）以及 changelog-producer 对读取内容的影响。
- 理解 Enumerator 的发现、反压、按 bucket 分配。
- 理解 checkpoint 状态、consumer-id 防过期机制、Flink SQL 的 changelog mode。

## 一句话结论
标准 FLIP-27 Source：**JobManager 上的 Enumerator 逐快照追踪、生成 split；TaskManager 上的 Reader 用 TableRead 读 split**。流读位点本质上就是一个数字 `nextSnapshotId`。

## 1. 整体结构与 Source 选择

```
JobManager                                          TaskManager × N
┌──────────────────────────────────────┐           ┌──────────────────────────────┐
│ ContinuousFileSplitEnumerator        │  splits   │ FileStoreSourceReader        │
│   每 discovery-interval(10s)：       │ ────────► │   FileStoreSourceSplitReader │
│   scan.plan() → DataTableStreamScan  │           │     TableRead.createReader   │
│   按 bucket 分配                      │ ◄──────── │   ReaderConsumeProgressEvent │
│ 状态：pending splits + nextSnapshotId │  消费进度  │ 状态：split + recordsToSkip  │
└──────────────────────────────────────┘           └──────────────────────────────┘
```

`FlinkSourceBuilder` 选择：

| 条件 | Source |
|---|---|
| 有界（批） | `StaticFileStoreSource` |
| `source.checkpoint-align.enabled` | `AlignedContinuousFileStoreSource`（一个 checkpoint 一个快照） |
| 配了 `consumer-id` 且 `consumer.mode=exactly-once`（默认） | `MonitorSource`：单并发 monitor 生成 split → 按 bucket shuffle → `ReadOperator` |
| **其它（默认）** | **`ContinuousFileStoreSource` + `ContinuousFileSplitEnumerator`** |

## 2. 核心：`DataTableStreamScan.plan()` 两阶段

```java
if (nextSnapshotId == null) return tryFirstPlan();   // 阶段一：起点
else                        return nextPlan();       // 阶段二：逐快照追踪
```
`checkpoint()` 返回 `nextSnapshotId`，`restore(id)` 设置它。

### 阶段一：`tryFirstPlan`（由 `scan.mode` 决定）

| scan.mode | StartingScanner | 行为 |
|---|---|---|
| 默认 → `latest-full` | `FullStartingScanner` | 先读最新快照全量（+I），再追增量 |
| `latest` | `ContinuousLatestStartingScanner` | 只读启动后的增量 |
| `from-timestamp` / `from-snapshot`（配 `scan.timestamp-millis`、`scan.snapshot-id`、`scan.tag-name` 等也会触发） | `ContinuousFrom...StartingScanner` | 从指定位置开始 |
| 配了 `consumer-id` 且有记录 | — | 从 consumer 的 `nextSnapshot` 继续 |

全量阶段按 changelog-producer 过滤 level：
```java
if (changelogProducer == LOOKUP)          scan(snapshotReader.withLevelFilter(level -> level > 0));
if (changelogProducer == FULL_COMPACTION) scan(snapshotReader.withLevelFilter(level -> level == maxLevel));
```
原因：L0 数据将来合并时会以 changelog 形式再出现，全量里再读一次就重复了。

### 阶段二：`nextPlan` 循环
```java
while (true) {
    Snapshot snapshot = nextSnapshotProvider.getNextSnapshot(nextSnapshotId);
    if (snapshot == null) return SnapshotNotExistPlan.INSTANCE;      // 还没产生，等
    if (boundedChecker.shouldEndInput(snapshot)) throw new EndOfScanException();  // scan.bounded.watermark
    if (OVERWRITE) → handleOverwriteSnapshot
    if (followUpScanner.shouldScanSnapshot(snapshot)) {
        plan = followUpScanner.scan(snapshot, snapshotReader);
        nextSnapshotId++;
        if (plan.splits().isEmpty()) continue;
        return plan;                                                 // 一个快照一个 plan
    } else nextSnapshotId++;
}
```

| changelog-producer | FollowUpScanner | 读哪些快照 | 读什么 |
|---|---|---|---|
| `none` | `DeltaFollowUpScanner` | 只读 `APPEND` 快照（COMPACT 不改变数据） | `ScanMode.DELTA`：本次新写的文件 |
| `input` / `lookup` / `full-compaction` | `ChangelogFollowUpScanner` | 只读有 `changelogManifestList` 的快照 | `ScanMode.CHANGELOG`：changelog 文件 |

**快照不存在时**（`NextSnapshotFetcher.rangeCheck`）：
- `nextSnapshotId > latest + 1`：表可能被重建 → `OutOfRangeException`；
- `nextSnapshotId < earliest`：**快照已过期** → `OutOfRangeException`，提示加大保留时间或用 `consumer-id`；
- changelog 独立生命周期时退回读 changelog。

## 3. Enumerator：发现、反压、分配

### 定时 + 按需发现
```java
public void start() {
    context.callAsync(this::scanNextSnapshot, this::processDiscoveredSplits, 0, discoveryInterval);
}
public void handleSplitRequest(int subtaskId, ...) {
    readersAwaitingSplit.add(subtaskId);
    assignSplits();
    if (仍然没分到 && !stopTriggerScan) {   // reader 空闲 → 立即补一次扫描
        stopTriggerScan = true;
        context.callAsync(this::scanNextSnapshot, this::processDiscoveredSplits);
    }
}
```
- 定时：`continuous.discovery-interval`（默认 10s）；
- 按需：reader 空闲时立刻补扫，追积压更快；
- 扫到 `SnapshotNotExistPlan` 时 `stopTriggerScan = true`，避免空转。

### 反压
```java
if (splitAssigner.numberOfRemainingSplits() >= splitMaxNum) return Optional.empty();
if (maxSnapshotCount > 0 && handledSnapshotCount >= maxSnapshotCount) return Optional.empty();
```
- `splitMaxNum = 并行度 × scan.max-splits-per-task`（默认 10）；
- `scan.max-snapshot.count`（默认 -1）限制一个 checkpoint 内处理的快照数。

### 按 bucket 分配，保证有序
```java
return shuffleBucketWithPartition
    ? ChannelComputer.select(split.partition(), bucketId, parallelism)
    : ChannelComputer.select(bucketId, parallelism);
```
- **同一 bucket 永远分给同一 reader**，`PreAssignSplitAssigner` 按快照顺序下发 → 同一 key 的变更有序输出（-U 在 +U 前）。
- 推论：**source 并行度超过 bucket 数是浪费**（`sourceParallelismUpperBound = bucketNum`）。
- 不需要有序的表（unaware-bucket append 表，或 `bucket-append-ordered=false`）用 `FIFOSplitAssigner`。

## 4. Reader：读 split 与断点续读

```java
public void start() {
    if (getNumberOfCurrentlyAssignedSplits() == 0) context.sendSplitRequest();
}
protected void onSplitFinished(Map<String, FileStoreSourceSplitState> finished) {
    if (getNumberOfCurrentlyAssignedSplits() == 0) context.sendSplitRequest();   // 拉模式
    ... 发送 ReaderConsumeProgressEvent(已完成 split 的最大 snapshot id)
}
```
- `LazyRecordReader` → `tableRead.createReader(split)`；流 split `isStreaming=true` → `MergeFileSplitRead.createNoMergeReader`，**不归并**，保留 RowKind。
- **split 内位点**：`FileStoreSourceSplitState.recordsToSkip` 每发一条更新；恢复时 `seek(recordsToSkip)` 跳过已发出的行。文件不可变，所以“跳过前 N 行”就能精确续读。

## 5. Checkpoint 与 exactly-once

| 组件 | 状态 | 恢复 |
|---|---|---|
| Enumerator | `PendingSplitsCheckpoint(未分配 splits, nextSnapshotId)` | `scan.restore(nextSnapshotId)` + splits 放回 assigner |
| Reader | 已分配 splits + `recordsToSkip` | 跳过已发出的行继续读 |

快照文件不可变、id 单调递增，**“下一个快照号 + 未完成 split + split 内偏移”** 即可精确恢复。

## 6. consumer-id：防止快照被提前过期

Flink checkpoint 只记录 Flink 侧位点；若表把流作业还要读的快照过期了，重启就 `OutOfRangeException`。`consumer-id` 把读取位点**告诉表**：

**① 计算进度**（`ConsumerProgressCalculator`）
```
snapshotState(cp)：
  对每个 reader：
    空闲等 split → assigner 为它准备的下一个 snapshot id
    忙碌中       → max(上报的消费 snapshot, 最近分配的 snapshot)
  minNextSnapshot[cp] = 所有 reader 的最小值
notifyCheckpointComplete(cp)：
  取 ≤ cp 的 minNextSnapshot 中的最大值 → scan.notifyCheckpointComplete(next)
```
**② 持久化**：`consumerManager.resetConsumer(consumerId, new Consumer(nextSnapshot))` 写 `consumer/consumer-<id>`。

**③ 阻止过期**：`ExpireSnapshotsImpl`
```java
maxExclusive = Math.min(maxExclusive, consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE));
```
**④ 续读**：同 consumer-id 的新作业即使没有 Flink 状态，也从 consumer 的 `nextSnapshot` 开始（`consumer.ignore-progress=true` 可忽略）。

配置：
- `consumer.expiration-time`：长期不更新的 consumer 被清理，避免死 consumer 永久阻塞过期；
- `consumer.mode`：`exactly-once`（默认）走 `MonitorSource`，进度与 checkpoint 严格对齐；`at-least-once` 走 FLIP-27 路径，进度异步上报，重启可能重读少量快照。

## 7. Flink SQL 看到的 changelog mode

`BaseDataTableSource.getChangelogMode()`：

| 情况 | ChangelogMode | 含义 |
|---|---|---|
| 批 / append 表 / first-row | `insertOnly` | 只有 +I |
| `changelog-producer ≠ none` 或 `scan.remove-normalize=true` | `all` | 完整 changelog |
| 主键表 + `changelog-producer = none` | `upsert` | **Flink 规划器插入 `ChangelogNormalize`**，用 keyed state 存所有 key 最新值补 -U，**状态很重** |
| 主键可空 + `none` | 报错 | 必须配置 changelog producer |

这就是流读主键表要选 changelog producer 的根本原因。

## 8. 常见问题

| 现象 | 原因 / 处理 |
|---|---|
| `The wanted read snapshot with id X has expired` | 停太久或消费慢；用 `consumer-id`、加大 `snapshot.time-retained` 或 changelog 独立保留 |
| 端到端延迟 | ≈ 写入端 checkpoint 间隔 + discovery-interval（最多 10s） |
| 部分 source subtask 空闲 | 并行度 > bucket 数 |
| 下游状态很大 | `changelog-producer=none` 触发 ChangelogNormalize，改用 `lookup` / `input` |

## 动手实验
- [ ] 跑 `ContinuousFileSplitEnumeratorTest`（其中包含 `ReaderConsumeProgressEvent` 消费进度的用例）、`ContinuousFileStoreITCase`。
- [ ] 断点：`DataTableStreamScan.tryFirstPlan / nextPlan`、`scanNextSnapshot`、`assignSplits`、`ConsumerProgressCalculator.computeMinNextSnapshotId`。
- [ ] 实验：设很短的 `snapshot.time-retained`，停流读作业一段时间复现快照过期；再加 `consumer-id` 对比，查 `t$consumers`。

## 自测题
1. lookup 表的流读全量阶段为什么要跳过 L0？
2. `changelog-producer=none` 时流读为什么只读 APPEND 快照？
3. 为什么同一 bucket 必须分给同一个 reader？
4. consumer 进度为什么取“所有 reader 的最小值”？
5. 为什么 `changelog-producer=none` 的主键表流读会导致下游状态很大？
