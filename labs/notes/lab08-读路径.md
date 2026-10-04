# 实验 8：读路径——直接读还是合并读

```bash
./run.sh sql/lab08/read-path.sql
PAIMON_VERSION=2.2-SNAPSHOT ./jdb-stacks.sh sql/lab08/read-path.sql jdb/s2-6-read-path.txt 300
```

对应教程 06 章前半、S2 第 6 讲。5 张 `bucket = 1` 的主键表（Paimon 2.0.0 / jdb 在 2.2-SNAPSHOT）：

| 表 | 文件 | 规划（splitForBatch） | 读取 |
|---|---|---|---|
| r_one | 1 个 L0 | 1 个 section → 单文件 split | 直接读 RawFileSplitRead |
| r_disjoint | 2 个 L0，key 不重叠 | 2 个 section → 装进 1 个 split | 合并读（每个 section 只有 1 个 run） |
| r_overlap | 2 个 L0，key 重叠 | 1 个 section（2 个 run） | 合并读 |
| r_compact | 1 个 L5（全量合并后） | 整个 bucket rawConvertible | 直接读 |
| r_delete | L5 + 含删除记录的 L0 | 1 个 section（2 个 run） | 合并读 |

几个细节：
- 判断单位是 split：不重叠的小文件被 `packSplits` 装进同一个 split（默认目标 128 MB，每个 section 至少按 4 MB 计），也会走合并读。
- 重叠的 section 只下推主键过滤（`MergeFileSplitRead.withFilter`）；`SELECT * FROM r_overlap WHERE v = 'a' AND k BETWEEN 58 AND 62` 正确返回空集。
- `COUNT(*)` 会被聚合下推：所有 split 都 rawConvertible 时直接用文件元数据行数，不读文件（`EXPLAIN` 里出现 `aggregates=[...]`）。所以观察读取路径要用 `GROUP BY v` 这类不能下推的查询。
- `$files` 的删除行数列在 2.0.0 里叫 `deleteRowCount`（驼峰）。
