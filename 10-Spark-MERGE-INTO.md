# 10 Spark MERGE INTO 的实现

源码（`paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/`）：`catalyst/analysis/PaimonMergeInto.scala`、`RowLevelHelper.scala`、`SparkTable.scala`、`commands/MergeIntoPaimonTable.scala`、`commands/PaimonRowLevelCommand.scala`、`rowops/PaimonSparkCopyOnWriteOperation.scala`、`rowops/PaimonCopyOnWriteScan.scala`、`write/PaimonBatchWriteBase.scala`

## 学习目标
- 知道一条 MERGE INTO 会走哪条路径（V1 / V2、PK / CoW / DV）。
- 理解共享的 “full outer join + 逐行判定” 引擎。
- 理解不同路径的写放大与提交类型、与流写的并发关系。

## 一句话结论
三条路径都归结为 **“source 与 target 做 join，逐行判定动作，输出变化行”**，区别在落地方式：**主键表**写 upsert/delete 记录交给 LSM 合并；**append 表**重写被触及的文件（CoW）或用删除向量标记。

## 1. 入口：分析规则 `PaimonMergeInto`

通过 `PaimonSparkSessionExtensions` 注册 post-hoc 规则，拦截目标为 Paimon 表的 `MergeIntoTable`：
```scala
checkPaimonTable(...)                 // 表类型是否支持
checkDeleteActionValidity(...)        // 主键表 + DELETE → 合并引擎必须能处理删除
checkCondition(...)                   // 条件不能含子查询
checkUpdateActionValidity(...)        // 主键表不能更新主键列
evolveTargetIfNeeded(...)             // schema 演进（内存加列，执行时提交）
val aligned = alignAllMergeActions(...)   // 把各动作赋值对齐到目标表全列
if (!shouldFallbackToV1MergeInto(aligned)) markAligned(aligned)    // 交给 Spark 原生 V2
else buildV1Command(...)              // → MergeIntoPaimonTable / MergeIntoPaimonDataEvolutionTable
```
- **不能更新主键列**，除非 merge 条件对所有主键做了等值（此时 `SET pk = source.pk` 等价无操作）。
- **主键表 + `WHEN MATCHED THEN DELETE`** 会写真实 `-D` 记录，`partial-update` 须配 `partial-update.remove-record-on-delete`，否则写入成功但**读时报错**。

## 2. 路由

`SparkTable.supportsV2RowLevelOps` 决定是否暴露 `SupportsRowLevelOperations`：

| 表类型 | 路径 | 落地方式 |
|---|---|---|
| **主键表** | V1：`performMergeForPkTable` | 写 +I/+U/-D 进 LSM |
| append 表（Spark ≥ 3.5、V2 写、无 DV/data evolution/CHAR） | **V2**：Spark `RewriteMergeIntoTable` + `PaimonSparkCopyOnWriteOperation` | 重写触及的文件 |
| append 表（回退 V1） | V1：`performMergeForNonPkTable` CoW 分支 | 重写触及的文件 |
| append 表 + DV | V1 DV 分支（unaware-bucket DV 表另有 V2 delta 路径） | DV 标记 + 新文件 |
| data evolution 表 | `MergeIntoPaimonDataEvolutionTable` | 基于 row tracking 只更新变化列 |

## 3. 共享引擎：`constructChangedRows`

```scala
targetProject = target.output :+ Alias(true, "_target_row_")
sourceProject = source.output :+ Alias(true, "_source_row_")
joinNode = Join(sourceProject, targetProject, FullOuter, Some(mergeCondition))
joinedDS.mapPartitions(MergeIntoProcessor(...).processPartition)
```
- `_source_row_` 为 null → 只有 target → **NOT MATCHED BY SOURCE**
- `_target_row_` 为 null → 只有 source → **NOT MATCHED**
- 都有 → **MATCHED**

`MergeIntoProcessor.processRow`：按顺序找第一个条件成立的动作：
```scala
if (targetRowHasNoMatch)       applyPreds(notMatchedBySourcePreds, ...)
else if (sourceRowHasNoMatch)  applyPreds(notMatchedPreds, ...)
else                           applyPreds(matchedPreds, ...)
// 没有动作命中：来自“被重写文件”(_file_touched_col_=true) 的行 → 原样保留(+I)；否则 → NOOP(-1) 丢弃
```

| 动作 | 输出 `_row_kind_` |
|---|---|
| `UPDATE SET ...` | `+U` + 新值 |
| `DELETE` | 主键表/DV：`-D` + 原行；CoW：NOOP（不写回即删除） |
| `INSERT ...` | `+I` |
| 未命中、来自重写文件 | `+I`（原样拷贝） |
| 未命中、其它 | NOOP 丢弃 |

逐行判定用代码生成的 `UnsafeProjection` / `GeneratePredicate`。

**前置检查 `checkMatchRationality`**：有 MATCHED 动作时先 inner join 统计，**一行 target 匹配多行 source 直接报错**。

**target 裁剪**：merge 条件中只涉及 target 的部分下推为 target 过滤，**仅当没有 `WHEN NOT MATCHED BY SOURCE`** 时启用。

## 4. 主键表路径：最简单、最便宜
```scala
writer.write(constructChangedRows(sparkSession, createDataset(sparkSession, filteredTargetPlan),
                                  remainDeletedRow = true))
```
- **不重写任何文件**，只写变化行，`_row_kind_` 作为 RowKind，按主键分桶写成**新的 L0 文件**。
- 剩下交给 LSM：读时按 sequence 合并，合并时清除旧版本；lookup 模式还会给流读产 -U/+U。
- **提交**：只有新增文件 → 普通 **APPEND**，默认不做冲突检测，**可与 Flink 流写并发**。

## 5. append 表 V1 CoW 路径（无 DV）

```
Step1 findCandidateDataSplits(targetOnlyCondition)   // 按分区/统计裁剪候选文件
Step2 找触及的文件（只取 __paimon_file_path 元数据列）：
      MATCHED 有 UPDATE/DELETE           → inner join     → filePathsToRewritten
      只有 NOT MATCHED(INSERT)          → inner join     → filePathsToRead（读但不重写）
      NOT MATCHED BY SOURCE 有 UPDATE/DELETE → left_anti join → filePathsToRewritten
Step3 filesToRewrittenDS 标 _file_touched_col_=true；filesToReadDS 标 false
Step4 constructChangedRows(两者 union)
Step5 writer.write(结果) → 新文件；buildDeletedCommitMessage(filesToRewritten) → 删除旧文件
```
- **为什么要 `filesToRead`**：只有 INSERT 时，判断 source 行是否 NOT MATCHED 必须看到它可能匹配的 target 行；这些行必然在 inner join 找到的文件里，所以**只读这些文件、不必重写**。
- 开启 row tracking 时，重写的行保留原 `_ROW_ID`。

## 6. append 表 + 删除向量（V1 DV 分支）
```
Step2 扫候选文件，带元数据列 __paimon_file_path、__paimon_row_index
      constructChangedRows(..., remainDeletedRow = true, extraMetadataCols = dvMetaCols)
Step3 -D 或 +U 的行 → (文件, 行号) → collectDeletionVectors → writer.persistDeletionVectors
Step4 +I 或 +U 的行 → 写新文件
Step5 提交：新数据文件 + DV 索引文件
```
**不重写任何旧文件**，写代价远低于 CoW。

## 7. V2 路径：Spark 原生 copy-on-write
```scala
newScanBuilder → PaimonCopyOnWriteScan          // 扫 target，记住读了哪些 split
requiredMetadataAttributes → __paimon_file_path（row tracking 时加 _ROW_ID / _SEQUENCE_NUMBER）
newWriteBuilder → PaimonV2WriteBuilder.overwriteFiles(copyOnWriteScan)
```
- **运行时分组过滤**：`filterAttributes()` 返回 `__paimon_file_path`，Spark 先查出含匹配行的文件，再把 `IN (文件路径)` 推给 `filter()`，只扫这些文件。
- 提交时 `buildDeletedCommitMessage(scan.scannedFiles)` 删除扫过的文件，加上新写文件，完成替换。

## 8. 提交层面（呼应 05 章）

| 路径 | CommitMessage 内容 | 提交类型 | 冲突检测 |
|---|---|---|---|
| 主键表 | 只有新文件 | `APPEND` | 默认关闭，可与流写并发 |
| append CoW（V1/V2） | 新文件 + **旧文件 DELETE** | `shouldBeOverwriteCommit` → **`OVERWRITE`** | **强制开启**，`allowRollback=true` |
| append DV | 新文件 + **DV 索引文件** | 同样升级为 **`OVERWRITE`** | 强制开启 |

若期间别的作业合并了相同文件而最新快照是 COMPACT，会**回滚该 compact 快照并重试**。快照中记录 `Snapshot.Operation.MERGE`，可在 `t$snapshots` 看到。

## 9. 对比例子
```sql
MERGE INTO t USING s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

| | 主键表 | append（CoW） | append（DV） |
|---|---|---|---|
| 读 | s 与 t full outer join | 先找触及文件，只读它们 | 候选文件 + 行号 |
| 写 | 只写 -D/+U/+I | **触及文件的全部行** + 新插入 | 只写 +U/+I + DV |
| 旧数据 | 读时/合并时消除 | 删除旧文件 | DV 标记 |
| 写放大 | 最低 | 最高 | 低 |
| 与流写并发 | 友好 | 可能冲突，compact 可回滚 | 同 CoW |

## 10. 限制与坑

| 情况 | 行为 |
|---|---|
| 一行 target 匹配多行 source | 报错，先对 source 去重 |
| 更新主键列 | 报错 |
| 条件含子查询 | 不支持，先改写成 join |
| 主键表 + DELETE 但引擎不支持删除 | 分析阶段拒绝 |
| 大 append 表只改少量行 | CoW 整文件重写，考虑 DV 或主键表 |
| `WHEN NOT MATCHED BY SOURCE` | 关闭 target 裁剪，扫全表（分区过滤内） |

## 动手实验
- [ ] `EXPLAIN EXTENDED MERGE INTO ...`：看到 `MergeIntoPaimonTable` 是 V1，看到 `ReplaceData`/`WriteDelta` 是 V2。
- [ ] 对主键表、append 表、append+DV 表执行同一条 MERGE，对比 Spark UI 的 job 数、`t$snapshots` 的 commit_kind。
- [ ] 跑 `MergeIntoTableTestBase` 中的用例。
- [ ] 延伸：`DeleteFromPaimonTableCommand`、`UpdatePaimonTableCommand`、`OptimizeMetadataOnlyDeleteFromPaimonTable`（按分区删除时只改元数据）。

## 自测题
1. 主键表的 MERGE 为什么不需要重写文件？代价在哪？
2. CoW 路径的 `_file_touched_col_` 有什么用？
3. 为什么 append 表的 MERGE 提交是 OVERWRITE 类型？与流作业合并冲突时会发生什么？
4. 为什么有 `WHEN NOT MATCHED BY SOURCE` 时不能做 target 裁剪？
