# 05 提交：FileStoreCommitImpl 的冲突检测与重试

源码：`operation/FileStoreCommitImpl.java`、`operation/commit/ConflictDetection.java`、`PrimaryKeyConflictDetection.java`、`CommitScanner.java`、`CommitRollback.java`、`catalog/RenamingSnapshotCommit.java`、`manifest/FileEntry.java`

## 学习目标
- 理解乐观并发提交模型与一次提交的 4 个步骤。
- 分清“抢快照号失败（重试）”和“真实冲突（抛异常）”。
- 能解释任意一条提交冲突异常。

## 一句话结论
不锁表，先写好 manifest，再尝试**原子创建 `snapshot-(N+1)`**，谁先创建谁赢；输的一方重读最新快照、做冲突检测后重试。**抢号失败会重试，文件冲突直接抛异常**（作业 failover）。

## 1. 整体流程

```
commit(ManifestCommittable)
 ├─ collectChanges() → 拆出 append 变更 / compact 变更
 ├─ APPEND  → tryCommit(kind=APPEND,  detectConflicts=checkAppendFiles)   ← 快照 1
 └─ COMPACT → tryCommit(kind=COMPACT, detectConflicts=true)               ← 快照 2

tryCommit()  重试循环
 while (true) {
   latest  = snapshotManager.latestSnapshot()
   changes = changesProvider.provide(latest)       // overwrite 会基于 latest 重算要删的文件
   result  = tryCommitOnce(retryResult, ..., latest)
   成功 → break
   超时 / 超次数 → 抛异常
   retryWaiter.retryWait(retryCount)               // 指数退避
 }

tryCommitOnce()  单次尝试
 ① 幂等检查（上次其实成功了吗？）
 ② 冲突检测
 ③ 构建 snapshot（合并 manifest，写 base/delta/changelog/index manifest）
 ④ 原子提交：commitSnapshotImpl → SnapshotCommit.commit
```

**为什么一个 checkpoint 可能产生两个快照**：append 只新增文件，新增不会和别人冲突，默认不检测；compact 要删除旧文件，必须检测。若 APPEND 中含 **DELETE 条目或 DV 索引文件**（如 Spark 行级 DELETE/UPDATE），`shouldBeOverwriteCommit` 会升级为 `OVERWRITE`，强制检测并允许回滚。

## 2. 重试循环

```java
if (System.currentTimeMillis() - startMillis > options.commitTimeout()
        || retryCount >= options.commitMaxRetries()) {
    throw new RuntimeException("Commit failed ... there maybe exist commit conflicts between multiple jobs.");
}
retryWaiter.retryWait(retryCount);
```

| 配置 | 默认 |
|---|---|
| `commit.max-retries` | 10 |
| `commit.timeout` | 无（Long.MAX_VALUE） |
| `commit.min-retry-wait` / `commit.max-retry-wait` | 10ms / 10s |

退避：`min(10ms × 2^n, 10s)` + 最多 20% 随机抖动，避免多作业同时重撞。

会重试的结果只有：`CommitFailRetryResult`（抢号失败或原子提交异常）和 `RetryCommitResult.forRollback`（回滚了冲突的 compact 快照）。

## 3. tryCommitOnce 四步

### ① 幂等检查
上一次原子提交抛异常（如网络超时）时，快照可能其实已写成功：
```java
for (long i = startCheckSnapshot; i <= latestSnapshot.id(); i++) {
    Snapshot snapshot = snapshotManager.snapshot(i);
    hasOverwriteSinceLastAttempt |= snapshot.commitKind() == CommitKind.OVERWRITE;
    if (snapshot.commitUser().equals(commitUser)
            && snapshot.commitIdentifier() == identifier
            && snapshot.commitKind() == commitKind) {
        return new SuccessCommitResult();
    }
}
```
**`commitUser + commitIdentifier + commitKind` 唯一标识一次提交**（Flink 中 identifier 就是 checkpointId）。

### ② 冲突检测
条件：`latestSnapshot != null && (detectConflicts || discardDuplicate)`。

**读 base（`scanBaseDataFiles`）**：
- 首次：`readAllEntriesFromChangedPartitions`，只读**本次涉及的分区**的全部文件清单。
- 重试：复用上次的 `baseDataFiles` + 期间新快照的 delta（`readIncrementalChanges`）合并，**重试很便宜**；期间若发生 OVERWRITE 则退回全量读。
- `commit.discard-duplicate-files=true`（仅 APPEND）：从 delta 中剔除 base 里已存在的文件，用于幂等重复提交。

**`ConflictDetection.checkConflicts` 四道检查**：
```
a. checkBucketKeepSame          同分区 bucket 数必须一致（OVERWRITE 除外）
b. FileEntry.mergeEntries(delta)   delta 自身不能矛盾（同文件 ADD 两次等）
c. FileEntry.mergeEntries(base + delta) → checkDeleteInEntries
d. checkTableSpecificConflicts  主键表：LSM 层级重叠检查；append 表：无
```

核心 c 依赖合并规则：
```java
case ADD:    checkState(old == null, "Trying to add file %s which is already in the map")
             map.put(id, entry);
case DELETE: if (map.containsKey(id)) map.remove(id);    // 先 ADD 后 DELETE，抵消
             else map.put(id, entry);                     // 孤立的 DELETE
```
合并后**还残留 DELETE**，说明“我要删的文件在最新快照里已经不存在”——被别人删了 → **“File deletion conflicts detected! Give up committing.”**（若残留全在过期分区，报 “You are writing data to expired partitions”）。

**主键表 d：`PrimaryKeyConflictDetection`**
```java
按 (partition, bucket, level≥1) 分组，组内按 minKey 排序，相邻 a.maxKey >= b.minKey → “LSM conflicts detected!”
```
保护“同层文件不重叠”。场景：两个作业各自合并同一 bucket 的**不同** L0 文件都输出到 L1，互不删对方文件（c 通过），但 L1 文件 key 范围重叠。

### ③ 构建 snapshot
- 读上一快照所有 data manifest，`ManifestFileMerger.merge`，或**复用上次尝试的合并结果**（`tryReuseManifestMergeResult`）。
- 写 `baseManifestList`、`deltaManifestList`、`changelogManifestList`、index manifest；继承 watermark、统计信息，构造 `Snapshot`。
- **此阶段失败**：肯定还没提交，`commitCleaner` 清理临时 manifest 后抛出。

### ④ 原子提交

| 实现 | 机制 |
|---|---|
| `RenamingSnapshotCommit` | `fileIO.tryToWriteAtomic(snapshot-N)`；对象存储（rename 非原子）配合外部 `Lock` 并先检查 `exists` |
| `CatalogSnapshotCommit` | 由 Catalog（如 REST Server）基于 base 快照 uuid 做 CAS，见 11 章 |

三种结果：

| 结果 | 含义 | 处理 |
|---|---|---|
| `true` | 抢到 snapshot-N | 成功，触发 commit callbacks（Hive 分区同步、自动 tag、Iceberg 等） |
| `false` | 被别人抢了 N | `forCommitFail(...)`，**保留临时文件**，重试时复用 base 与 manifest 合并结果 |
| 异常 | 不确定是否成功 | 不清理，返回 `forCommitFail(..., e, ...)`，下次由①判定 |

## 4. 回滚特例：DML vs 后台合并

```java
if (exception.isPresent()) {
    if (allowRollback && rollback != null && rollback.tryToRollback(latestSnapshot))
        return RetryCommitResult.forRollback(exception.get());
    throw exception.get();
}
```
`CommitRollback.tryToRollback`：**只有最新快照是 COMPACT 时才回滚到 `latest-1`**。场景：Spark `DELETE` 与流作业后台合并重写了相同文件，放弃合并结果（合并可重做），让用户 DML 重试成功。

## 5. 重启后的幂等：`filterCommitted`
```java
latestSnapshot = snapshotManager.latestSnapshotOfUser(commitUser);
只保留 committable.identifier() > latestSnapshot.commitIdentifier() 的
```
与①配合实现 exactly-once。

## 6. 常见场景

| 场景 | 结果 |
|---|---|
| 两个作业只 APPEND 同一张表 | 只会抢号，自动重试成功 |
| **两个作业写同一 bucket 且都做合并** | File deletion conflict，failover。**建议写入作业 `write-only=true` + 独立 compaction 作业** |
| 两作业各自合并不同 L0 到 L1 | LSM conflict |
| **从旧 savepoint 恢复** | 要删的文件早被合并掉 → 持续冲突。从最新 savepoint 恢复，或把表回滚到对应快照 |
| 不 overwrite 就改 bucket 数 | checkBucketKeepSame 失败，需要 `INSERT OVERWRITE` 重分布 |
| Spark DELETE vs 流合并 | 回滚 compact 快照，DML 重试成功 |

冲突异常中会打印 “Base commit user / Current commit user”：**不同**说明有别的作业在写同一分区；**相同**通常是从旧 checkpoint 恢复。

## 动手实验
- [ ] 跑 `FileStoreCommitTest` 中搜 `conflict` 的用例。
- [ ] 自己写一个测试：两个 commitUser 对同一 bucket 的同一批文件做 compact 提交，复现 File deletion conflict。
- [ ] 日志搜 `Atomic commit failed for snapshot`（抢号失败）和 `conflicts detected`（真实冲突）。

## 自测题
1. 为什么 APPEND 默认不做冲突检测？什么情况下会被升级为 OVERWRITE？
2. `mergeEntries` 后残留 DELETE 意味着什么？
3. 为什么还需要 LSM 层级重叠检查？举一个 c 检查发现不了的例子。
4. 原子提交抛异常时为什么不能清理临时文件？下次如何判断成功与否？
5. 回滚为什么只针对 COMPACT 快照？
