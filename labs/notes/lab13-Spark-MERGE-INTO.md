# 实验 13：Spark MERGE INTO 的三条路径

```bash
./spark.sh sql/lab13/merge-into.sql
```

对应教程 10 章、S2 第 11 讲。`spark/` 子工程：Spark 3.5.8 + `paimon-spark-3.5_2.12`（默认 2.0.0），`local[1]`、shuffle 分区 1，文件数每次相同。

每张表先分两批写 (1,a)(2,b)(3,c) 与 (4,d)(5,e)(6,f)（两个数据文件），再执行同一条 MERGE：id 2 → 'B'、删除 id 3、插入 id 7。四张表查询结果相同：1 a、2 B、4 d、5 e、6 f、7 G。

| 表 | EXPLAIN | MERGE 后的文件 | 提交 |
|---|---|---|---|
| `t_pk`（主键表） | `Execute MergeIntoPaimonTable` | 旧文件不动 + 1 个 3 条记录的 L0 文件 | APPEND，total 9 / delta 3 |
| `t_cow`（追加表） | `Execute MergeIntoPaimonTable` | 含 1~3 的文件被整体重写成 (1 a, 2 B, 7 G)，含 4~6 的不动 | OVERWRITE，total 6 / delta 0 |
| `t_dv`（追加表 + 删除向量） | `Execute MergeIntoPaimonTable` | 旧文件不动 + 新文件 (2 B, 7 G) + 33 字节删除向量索引；id 2、3 原来所在的第 1、2 行被标记 | OVERWRITE，total 8 / delta 2 |
| `t_v2`（追加表，`spark.paimon.write.use-v2-write`=true） | `ReplaceData PaimonWrite` + `PaimonCopyOnWriteScan`，`RuntimeFilters: __paimon_file_path IN subquery` | 同 t_cow | OVERWRITE |

- 行所在的文件 / 行号用元数据列 `__paimon_file_path`、`__paimon_row_index` 查。
- 一行 target 匹配多行 source：`Can't execute this MergeInto when there are some target rows that each of them match more than one source rows.`
- `SET` 的键含 `-` 要用反引号：`` SET `spark.paimon.write.use-v2-write`=true; ``，否则 `INVALID_SET_SYNTAX`。
- 追加表提交里有 DELETE 文件或删除向量索引时升级为 OVERWRITE 并强制冲突检测（`ConflictDetection.shouldBeOverwriteCommit`、`FileStoreCommitImpl` 第 351~356 行）。
