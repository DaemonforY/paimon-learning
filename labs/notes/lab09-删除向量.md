# 实验 9：删除向量

```bash
./run.sh sql/lab09/deletion-vectors.sql
PAIMON_VERSION=2.2-SNAPSHOT ./jdb-stacks.sh sql/lab09/deletion-vectors.sql jdb/s2-7-deletion-vectors.txt 300
```

对应教程 06 章后半、S2 第 7 讲。两张 `bucket = 1` 的主键表只差 `deletion-vectors.enabled`，依次：写 1~100、更新 51~60、删除 k = 5（Paimon 2.0.0）：

| | dv_off | dv_on |
|---|---|---|
| 文件 | 3 个 L0（100、10、1 行删除记录） | L5 · 100、L4 · 10，DV 索引文件 28 → 32 B；删除不产生新数据文件 |
| 写时标记（jdb） | — | `notifyNewDeletion` 行号 50~59（更新）、4（删除），同一个 L5 文件 |
| 读取（jdb） | 合并读 3 个文件 | 直接读，`ApplyDeletionVectorReader` 跳过 11 行 |
| COUNT(*) | 不下推 | 下推：110 − 11 = 99 |

代价：DV 表批读跳过 L0。`write-only` 写入 2 行后 COUNT(*) = 0；加 `deletion-vectors.merge-on-read = true` 为 2；`CALL sys.compact` 后为 2。
