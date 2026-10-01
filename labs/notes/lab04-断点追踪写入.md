# 实验 4（阶段 1）：用 IDEA 断点追踪一次写入

目标：亲眼看到 `INSERT INTO trace_orders VALUES (1,'A'), (2,'B'), (1,'C')` 这 3 行数据，
如何经过 Flink 算子 → TableWrite → 分桶 → MergeTreeWriter 写缓冲 → 刷成 L0 文件 → CommitMessage → 提交快照。

对应教程：02 章（写入流程）、05 章（提交）、08 章（批模式的 endInput 提交）。

---

## 0. 原理：远程调试

实验程序以 JDWP 调试模式启动（`debug.sh`），在 5005 端口**等待调试器连接**；
在 IDEA 的 **paimon 源码工程**里用 “Remote JVM Debug” 连上去。
断点按“类名 + 行号”匹配，实验用的 jar 正是这份源码编译的，所以断点能精确命中——**不需要导入实验工程**。

> 已验证：用 jdb 在 `TableWriteImpl.java:239` 下断点，成功命中，线程名为
> `Writer : trace_orders -> Global Committer : trace_orders -> end: Writer (1/1)#0`
> （并行度 1 时 Writer 和 Committer 被 chain 在同一个线程里）。

## 1. 一次性准备

**前提**：断点行号基于 Paimon `d15d250cf`。先按 [01 章](../../01-环境搭建与源码编译.md) 在该提交上编译安装（得到 `2.2-SNAPSHOT`），运行时用 `PAIMON_VERSION=2.2-SNAPSHOT`，否则断点会错位。

### 在 IDEA 里建调试配置

在已导入的 **paimon 工程**中：
1. `Run → Edit Configurations… → + → Remote JVM Debug`
2. Name：`paimon-lab-5005`
3. Debugger mode：`Attach to remote JVM`；Host：`localhost`；Port：`5005`
4. Use module classpath：选 `paimon-flink-1.20`（让 Flink 模块的类也能对上源码）
5. 保存

## 2. 每次调试的步骤

1. **先在 IDEA 里打好断点**（见第 3 节，建议先打 ①④⑥⑧⑫ 这 5 个）。
2. 在终端启动被调试程序：
   ```bash
   cd labs && PAIMON_VERSION=2.2-SNAPSHOT ./debug.sh sql/lab04/trace-write.sql
   ```
   看到 `Listening for transport dt_socket at address: 5005` 后，程序会停住等待。
3. IDEA 中选 `paimon-lab-5005`，点 **Debug**（小虫子）。连上后程序开始跑，会依次停在断点上。
4. 每停一次：看 **Frames**（调用栈）、**Variables**，用 **Evaluate Expression（⌥F8）** 计算表达式，然后 **Resume（⌥⌘R）** 到下一个断点。
5. 调试模式已把心跳/RPC 超时调成 1 小时，**可以在断点上停很久**，不会导致作业失败。
6. 想从头再来：Stop 调试 → 终端重新执行 `./debug.sh ...`（脚本会 DROP 并重建表）。

## 3. 断点清单（按执行顺序）

路径前缀：core = `paimon-core/src/main/java/org/apache/paimon/`，flink = `paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/`

### 阶段 A：一行数据的写入（每行命中一次，共 3 次）

| # | 位置 | 看什么 / 在 Evaluate 里算什么 |
|---|---|---|
| ① | flink `sink/RowDataStoreWriteOperator.java:55` `write(element.getValue())` | 线程名；`element.getValue()` 是 Flink 转过来的 `InternalRow`。这是 Flink 世界进入 Paimon 世界的入口 |
| ② | core `table/sink/TableWriteImpl.java:239` `checkNullability(...)` | `rowKind`（都是 `+I`）；往下 Step Over 到 244 行 `toSinkRecord` |
| ③ | core `table/sink/FixedBucketRowKeyExtractor.java:79` `bucketFunction.bucket(bucketKey(), numBuckets)` | **分桶算法**：`bucketKey()`（主键去掉分区键后的 BinaryRow）、`bucketKey().hashCode()`、`numBuckets`。结果 = `Math.abs(hash % numBuckets)`（`bucket/DefaultBucketFunction`）。本例订单 1、2 都落在 bucket 0 |
| ④ | core `operation/AbstractFileStoreWrite.java:189` `getWriterWrapper(partition, bucket)` | 第一行时会进入 `createWriterContainer`（Step Into）：548 行 `latestSnapshotFromFileSystem()` 为 null（新表），`scanExistingFileMetas` 恢复已有文件（这里为空）——**这就是实验 1 里写入前出现 “Read manifest entries” 日志的原因**。第二、三行直接复用已有 writer |
| ⑤ | core `operation/KeyValueFileStoreWrite.java:227` `compactManagerFactory.create(...)` | 只在创建 writer 时命中一次。Step Into 可以看到 `UniversalCompaction` 被创建（03 章）；235 行 `new MergeTreeWriter(...)` |
| ⑥ | core `mergetree/MergeTreeWriter.java:166` `long sequenceNumber = newSequenceNumber();` | Evaluate：`kv.key().getLong(0)`、`kv.value().getString(1).toString()`、`newSequenceNumber`。3 次命中分别是 (1,A,seq 0)、(2,B,seq 1)、(1,C,seq 2)。Step Over 到 168 行 `writeBuffer.put(...)`：**只是放进内存排序缓冲，还没有任何文件** |

> 技巧：给 ⑥ 加条件断点（右键断点 → Condition）：`kv.key().getLong(0) == 1`，只看订单 1 的两次写入。

### 阶段 B：输入结束，刷盘（批作业的 endInput）

| # | 位置 | 看什么 |
|---|---|---|
| ⑦ | flink `sink/PrepareCommitOperator.java:103` `emitCommittables(true, Long.MAX_VALUE)` | 批模式没有 checkpoint，输入结束时一次性 prepareCommit；**`Long.MAX_VALUE` 就是快照里那个 `commitIdentifier = 9223372036854775807`**（08 章） |
| ⑧ | core `mergetree/MergeTreeWriter.java:226` `writeBuffer.forEach(...)` | 调用栈：`prepareCommit → flushWriteBuffer`。`writeBuffer.size()` 为 3。此时去终端 `ls warehouse/default.db/trace_orders/bucket-0/`：**还没有数据文件** |
| ⑨ | core `mergetree/SortBufferWriteBuffer.java:180` `while (mergeIterator.hasNext())` | 缓冲按 key 排序后逐 key 归并；在 ⑩ 看同 key 的合并 |
| ⑩ | core `mergetree/compact/DeduplicateMergeFunction.java:59` `return latestKv;` | 订单 1 命中时：`latestKv.value().getString(1).toString()` = `"C"`，`latestKv.sequenceNumber()` = 2。**(1,A,seq 0) 在这里被丢弃**——写缓冲刷盘时就按合并引擎合并了同 key 记录 |
| ⑪ | core `mergetree/MergeTreeWriter.java:243` `for (DataFileMeta fileMeta : dataWriter.result())` | Evaluate `dataWriter.result()`：一个 `DataFileMeta`，`level=0`、`rowCount=2`、`minSequenceNumber=1`、`maxSequenceNumber=2`、`fileName=data-...parquet`。**现在终端 ls 能看到 parquet 文件了**，但还没有快照引用它 |
| ⑫ | core `operation/AbstractFileStoreWrite.java:266` `writerContainer.writer.prepareCommit(waitCompaction)` | Step Over 后看 `increment`：`newFilesIncrement().newFiles()` 就是刚写的文件；随后被包装成 `CommitMessageImpl(partition, bucket, ...)` |

### 阶段 C：提交快照

| # | 位置 | 看什么 |
|---|---|---|
| ⑬ | flink `sink/CommitterOperator.java:191` `pollInputs()` | 批模式：committer 在 endInput 时 `commitUpToCheckpoint(END_INPUT_CHECKPOINT_ID)` |
| ⑭ | core `table/sink/TableCommitImpl.java:326` `commit.filterCommitted(sortedCommittables)` | 幂等过滤（05 章）：新表没有该 commitUser 的快照，全部保留 |
| ⑮ | core `operation/FileStoreCommitImpl.java:369` `attempts += tryCommit(...)` | `changes.appendTableFiles`：1 个 ADD 条目；commitKind = APPEND |
| ⑯ | core `operation/FileStoreCommitImpl.java:1314` `success = commitSnapshotImpl(...)` | Evaluate `newSnapshot.toJson()`：id=1、baseManifestList/deltaManifestList、totalRecordCount=2。此时 `manifest/` 目录下已经有 manifest 和 manifest-list，但 `snapshot/` 还是空的 |
| ⑰ | core `catalog/RenamingSnapshotCommit.java:66` `fileIO.tryToWriteAtomic(newSnapshotPath, snapshot.toJson())` | **提交的“原子时刻”**：Step Over 前后分别在终端 `ls warehouse/default.db/trace_orders/snapshot/`，snapshot-1 在这一行之后才出现——**从这一刻起，数据对读者可见** |

## 4. 预期结论（跑完后对照）

```
3 行输入 ──► 分桶：都在 bucket 0（hash % 2）
         ──► MergeTreeWriter.write：seq 0/1/2，只进内存缓冲
endInput ──► flushWriteBuffer：排序 + DeduplicateMergeFunction 合并同 key → 写出 1 个 L0 文件（2 行，seq 1~2）
         ──► prepareCommit → CommitMessage
         ──► FileStoreCommitImpl：写 manifest → 写 manifest-list → 原子写 snapshot-1（这一刻才可见）
```

非调试运行的结果也印证了这一点：`$files` 中 bucket 0、level 0、record_count 2、min_sequence_number 1、max_sequence_number 2；`total_record_count` = 2。

## 5. 课后练习

1. **观察分桶**：把 SQL 改成写 10 个不同订单，在 ③ 处统计各 bucket 的分布；再想想为什么流读 source 并行度超过 bucket 数没用（09 章）。
2. **观察中途刷盘**：建表时加 `'write-buffer-size' = '256 kb'`、`'page-size' = '64 kb'`，写入几千行（可用 `INSERT INTO ... SELECT` 配合 datagen 或多值 VALUES），在 `MergeTreeWriter.java:169` `if (!success)` 处断点，看缓冲满时如何提前刷出多个 L0 文件。
3. **观察第二次写入**：不 DROP 表再跑一次（注释掉 DROP/CREATE），在 ④ 处看 `scanExistingFileMetas` 恢复出上次的文件、⑥ 处 `newSequenceNumber` 从 3 开始。
4. **观察合并**：连续写 5 次以上，在 `MergeTreeCompactManager.triggerCompaction` 处断点，看 `UniversalCompaction.pick` 何时返回非空（03 章）。
5. 画出你自己的调用链图（从 `RowDataStoreWriteOperator.processElement` 到 `RenamingSnapshotCommit.commit`），放进 `notes/` 里。
