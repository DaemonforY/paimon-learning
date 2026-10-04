# 实验 10：Lookup Changelog

```bash
./run.sh sql/lab10/lookup-changelog.sql
PAIMON_VERSION=2.2-SNAPSHOT ./jdb-stacks.sh sql/lab10/lookup-changelog.sql jdb/s2-8-lookup-changelog.txt 150
```

对应教程 07 章、S2 第 8 讲。四张 `bucket = 1` 的主键表做同样的写入：① 写 (1,'a') (2,'b') (3,'c')　② 写 (1,'A') (2,'b')（1 改值，2 值不变）　③ DELETE k = 3（Paimon 2.0.0 + Flink 2.2.0）。

| | 快照 | 文件 | 读到的变更 |
|---|---|---|---|
| `cl_none`（默认） | 3 个 APPEND，`changelog_record_count` 为 NULL | 3 个 L0 | 增量读（delta）：`+I 1 A`、`+I 2 b`、`-D 3 c`，没有 -U |
| `cl_lookup` | 每次写 = APPEND + COMPACT，changelog 3 / 4 / 1 条 | ① 后 L5·3；② 后 +L4·2；③ 后 +L3·1（删除记录） | ①：3 条 +I；②：`-U 1 a` `+U 1 A` `-U 2 b` `+U 2 b`；③：`-D 3 c` |
| `cl_dedup`（+ `row-deduplicate`） | 同上 | 同上 | ②：只有 `-U 1 a` `+U 1 A` |
| `cl_agg`（aggregation sum） | 同上 | ② 后 L4·1 | ②：`-U 1 10` `+U 1 15` |

升级时文件是否重写（`$files` 按快照对比文件名）：`cl_lookup` 的 L0 文件升到 L4 后文件名不变（只改 level）；`cl_agg` 升到 L4 后是新文件名（重写成合并后的 15）。

jdb（2.2-SNAPSHOT，46 次命中）：
- `ForceUpLevel0Compaction` 每次写都命中 `forcePickL0`（runs = 1 / 2 / 3，Universal 未触发）。
- 升级策略：① 输出到 L5（最高层）→ 第 160 行 `CHANGELOG_NO_REWRITE`；② ③ 去重表 → 第 166 行 `CHANGELOG_NO_REWRITE`；聚合表 → 第 172 行 `CHANGELOG_WITH_REWRITE`。
- ② 中先命中 `LookupLevels.createLookupFile`（L5 文件），再在 `getResult` 拿到 before；本次合并单元里只有 L0 文件。
- `setChangelog`：① before = null → +I；② before 存在、after INSERT → -U/+U（key 2 值相同也进入此处，去重表由 `valueEqualiser` 过滤）；③ after DELETE → -D。

批作业结束时 `endInput` 本来就会等待合并（`PrepareCommitOperator.java:101-103`），所以本实验**不能**说明 `lookup-wait` 的作用；流作业中由 `StoreSinkWrite.java:146` 生效（源码推导）。
