# 04 合并执行：MergeTreeCompactTask

源码：`mergetree/compact/MergeTreeCompactTask.java`、`IntervalPartition.java`、`MergeTreeCompactRewriter.java`、`mergetree/MergeTreeReaders.java`、`mergetree/MergeSorter.java`

## 学习目标
- 理解 section / sorted run 的切分（IntervalPartition）。
- 掌握“升级（只改 level）vs 重写（归并写新文件）”的判断。
- 理解归并读的三层结构、溢写、结果如何生效。

## 一句话结论
**能不重写就不重写**：key 范围不重叠的大文件只改 level（零数据 IO）；重叠的、小的文件才真正归并重写。

## 1. 从提交任务到生效

```
MergeTreeCompactManager.triggerCompaction()
  ├─ strategy.pick() → CompactUnit(outputLevel, files)
  ├─ 计算 dropDelete
  └─ submitCompaction(): unit.fileRewrite() ? FileRewriteCompactTask : MergeTreeCompactTask
       → executor.submit(task)                         异步执行，一个 bucket 同时只有一个任务
CompactTask.call()（打点、日志） → doCompact()
getCompactionResult() → levels.update(before, after)   更新内存 LSM 视图
prepareCommit() → compactIncrement → 提交时 before 记 DELETE、after 记 ADD
```

## 2. 构造时先分区：`IntervalPartition`

```java
this.partitioned = new IntervalPartition(unit.files(), keyComparator).partition();
```

1. **切 section**：按 `minKey` 排序，维护右边界 bound；下一个文件 `minKey > bound` 就开新 section。**section 之间 key 范围互不相交**，可独立处理。
2. **section 内打包成最少的 SortedRun**：优先队列按 run 尾文件 `maxKey` 排序（经典区间划分），能接上就接，否则新开 run。run 越少，归并时同时打开的 reader 越少。

结果类型：`List<List<SortedRun>>`（外层 section，内层需要归并的 run）。

## 3. `doCompact()`：逐 section 决定升级还是重写

```java
for (List<SortedRun> section : partitioned) {
    if (section.size() > 1) {
        candidate.add(section);                        // ① 有重叠 → 必须归并
    } else {
        for (DataFileMeta file : section.get(0).files()) {
            if (file.fileSize() < minFileSize) {
                candidate.add(singletonList(SortedRun.fromSingle(file)));  // ② 小文件 → 跟着重写
            } else {
                rewrite(candidate, result);            // ③ 大文件：先把之前攒的候选刷掉
                upgrade(file, result);                 //    再直接升级大文件
            }
        }
    }
}
rewrite(candidate, result);                            // ④ 收尾
result.setDeletionFile(compactDfSupplier.get());       // DV 模式附带删除向量文件
```

- `minFileSize = target-file-size × compaction.small-file-ratio`，主键表默认 `128MB × 0.7 ≈ 89.6MB`。
- **为什么遇到大文件要先刷候选**：section 按 key 顺序处理，若把大文件前后的小文件攒在一起合并，输出文件 key 范围会跨过大文件，与它重叠，破坏“同层不重叠”。

`rewrite()` 的捷径：候选只有一个 section 且只有一个 run（无重叠）时，直接对每个文件 `upgrade`，不重写。

`upgrade()` 中 3 种情况仍强制重写：
```java
if ((outputLevel == maxLevel && containsDeleteRecords(file))  // 到最高层且含 DELETE → 重写以清除删除
        || forceRewriteAllFiles                               // 强制重写（如改格式/压缩）
        || containsExpiredRecords(file)) {                    // 行级 TTL 有过期数据
    rewriteImpl(单文件);
    return;
}
if (file.level() != outputLevel) rewriter.upgrade(outputLevel, file);   // 默认只改元数据
```
默认 `AbstractCompactRewriter.upgrade` 就是 `new CompactResult(file, file.upgrade(outputLevel))`：文件名不变、只改 level。`ChangelogMergeTreeRewriter`（lookup / full-compaction）会重写该方法，升级时也产出 changelog（见 07 章）。

## 4. 真正的归并：`rewriteCompaction()`

```java
writer = writerFactory.createRollingMergeTreeFileWriter(outputLevel, FileSource.COMPACT);
reader = readerForMergeTree(sections, new ReducerMergeFunctionWrapper(mfFactory.create()));
if (dropDelete) reader = new DropDeleteReader(reader);
writer.write(new RecordReaderIterator<>(reader));
return new CompactResult(before = 所有输入文件, after = writer.result());
```
异常时 `writer.abort()` 删除写了一半的文件。

reader 的三层结构（`MergeTreeReaders`）：
```
readerForMergeTree   section 之间 → ConcatRecordReader（懒加载，同时只开一个 section）
  └ readerForSection   section 内多 run → MergeSorter.mergeSort（多路归并）
      └ readerForRun     run 内多文件 → ConcatRecordReader
```

- `MergeSorter.mergeSort`：run 数 ≤ `sort-spill-threshold`（默认 stop-trigger+1=9）直接 `SortMergeReader`（默认 `sort-engine=loser-tree`）；超过则把**最小的几个 run 先溢写到本地磁盘**，控制同时打开的 reader 数量，防 OOM。
- 归并顺序：先 key，同 key 按 sequenceNumber（或 `sequence.field`）。
- `ReducerMergeFunctionWrapper`：同 key 只有一条时直接返回，不调用合并函数（大部分 key 无重叠，省开销）。
- `RollingFileWriter`：达到 `target-file-size` 滚动新文件，输出有序且互不重叠，共同构成 outputLevel 的一个 run。

## 5. 完整例子

outputLevel = 3，minFileSize ≈ 89.6MB：

| 文件 | level | key 范围 | 大小 |
|---|---|---|---|
| A | L0 | [1, 10] | 5MB |
| B | L1 | [5, 20] | 50MB |
| C | L1 | [30, 40] | 2MB |
| D | L2 | [50, 90] | 120MB |
| E | L0 | [95, 99] | 1MB |

分区：S1 = {run[A], run[B]}（重叠），S2 = {C}，S3 = {D}，S4 = {E}。

处理：
1. S1 两个 run → `candidate = [S1]`
2. S2：C 小 → `candidate = [S1, [C]]`
3. S3：D 大 → `rewrite`：A、B、C 归并写成新的 L3 文件；`upgrade(D)`：D 从 L2 改为 L3，零 IO
4. S4：E 小 → `candidate = [[E]]`
5. 收尾：单 section 单 run → 捷径 → E 直接升级到 L3

结果：`before = {A,B,C,D,E}`，`after = {新文件, D', E'}`（D'、E' 文件名不变）。

## 6. 结果生效
- `levels.update(before, after)` 更新内存视图。
- 提交时 before → `DELETE`、after → `ADD`；升级的文件是同名的 DELETE(旧 level) + ADD(新 level)。
- 旧数据文件不会立刻删除（旧快照可能引用），在快照过期（`SnapshotDeletion`）时才物理删除。

## 相关配置

| 配置 | 默认 | 作用 |
|---|---|---|
| `target-file-size` | 主键表 128MB / append 表 256MB | 输出文件滚动大小 |
| `compaction.small-file-ratio` | 0.7 | 小文件阈值比例 |
| `sort-spill-threshold` | stop-trigger + 1 | 超过则先溢写再归并 |
| `sort-engine` | loser-tree | 多路归并算法 |

## 动手实验
- [ ] 跑 `IntervalPartitionTest`，手画每个用例的 section / run。
- [ ] 在 `MergeTreeCompactTask.doCompact`、`MergeTreeCompactRewriter.rewriteCompaction` 下断点。
- [ ] 观察 INFO 日志 `Paimon compact task finished: ... inputBytes / outputBytes`，outputBytes 远小于 inputBytes 说明多数是升级。
- [ ] 读 `SortMergeReaderWithLoserTree`，手画败者树调整过程。

## 自测题
1. section 和 sorted run 分别是什么？为什么要先分 section？
2. 为什么一个孤立的小文件会被升级而不是重写？
3. 哪 3 种情况下大文件也必须重写？
4. 升级后的文件在 manifest 中如何表示？
