# 实验 1 记录：第一次读写 Paimon

日期：2026-09-25　|　阶段 0

> 版本：首次采集于 Paimon 2.2-SNAPSHOT（`d15d250cf`）；2026-10-02 用 `run-all.sh` 在 **2.0.0 正式版**上复核，关键结论一致。

## 步骤与关键现象

### 步骤 1：建表 + 写入（step1-create-insert.sql）
- `orders` 只提交了 **3 个 CommitMessage**：数据落在 3 个 (分区, bucket) 上——`dt=2026-09-25` 的两条都哈希到了 bucket-0，bucket-1 为空所以没有目录。
- 目录结构：
  ```
  orders/
  ├── dt=2026-09-24/bucket-0/data-*.parquet
  ├── dt=2026-09-24/bucket-1/data-*.parquet
  ├── dt=2026-09-25/bucket-0/data-*.parquet
  ├── manifest/manifest-*          ← 1 个 manifest（3 条 ADD）
  ├── manifest/manifest-list-*-0   ← base manifest list（首次为空）
  ├── manifest/manifest-list-*-1   ← delta manifest list
  ├── schema/schema-0
  └── snapshot/{snapshot-1, LATEST, EARLIEST}
  ```
- `snapshot-1` 里：`commitKind=APPEND`、`totalRecordCount=5`、`commitIdentifier=9223372036854775807`（= Long.MAX_VALUE，批作业 endInput 的标识，见 08 章）。
- append 表 `access_log` 没有分区、没配 bucket，文件直接在 `bucket-0/` 下。

### 步骤 2：更新 + 删除（step2-update-delete.sql）
- 更新订单 1、删除订单 3 **都没有修改旧文件**，而是各自**新增 L0 文件**；快照 2、3 的 `commit_kind` 仍是 `APPEND`。
- 写入前日志出现 `Read 1 manifest entries ... Files size : 1`：writer 先从最新快照**恢复该 bucket 已有文件**，以便接续 sequence number（`FileSystemWriteRestore`）。
- `DELETE` 时 Flink 把 `dt` 条件下推给 Paimon 做分区裁剪，`order_id = 3` 下推到 Parquet 过滤，读出命中行后写入一条 **-D 记录**。
- `total_record_count` 变成 8，但查询只有 5 行：统计的是**物理记录**（含旧版本和删除标记），读时按 sequence 合并才得到 5 行。
- append 表写入重复行 → 两行都保留（没有主键就没有去重）。

### 步骤 3：系统表（step3-system-tables.sql）
- `orders$files`：6 个文件**全部在 level 0**；同一 bucket 内 sequence number 递增（订单 1 旧版本 seq=0、新版本 seq=2；订单 3 插入 seq=0、删除 seq=1）。
- 没有发生合并：每个 bucket 只有 2 个 sorted run，远小于 `num-sorted-run.compaction-trigger=5`。
- `orders$manifests`：3 个 manifest，都只有 ADD、没有 DELETE。

### 步骤 4：手动全量合并（step4-compact.sql）
- 快照 4：`commit_kind=COMPACT`，`total_record_count` 从 8 变 5，`delta_record_count=-3`。
- 三个合并任务：
  - `dt=2026-09-24/bucket-0`：2 → 1 文件（订单 1 只剩 PAID 版本）
  - `dt=2026-09-24/bucket-1`：2 → **0 文件**（订单 3 的插入和删除相互抵消；输出到最高层，`dropDelete=true`，删除记录被物理丢弃）
  - `dt=2026-09-25/bucket-0`：2 → 1 文件
- 合并后文件都在 **level 5**（`num-levels` 默认 6，maxLevel=5），`file_source=COMPACT`。
- **旧 parquet 文件仍在磁盘上**（8 个），只是不再被最新快照引用；要等快照过期才会物理删除。

## 与源码的对应
| 现象 | 源码 |
|---|---|
| 一个 bucket 一个 CommitMessage | `KeyValueFileStoreWrite` 按 (partition, bucket) 管理 writer |
| 更新/删除只追加 L0 | `MergeTreeWriter.write` → `flushWriteBuffer` |
| 查询时合并出最新值 | `MergeFileSplitRead` + `DeduplicateMergeFunction` + `DropDeleteReader` |
| 合并到 level 5、删除被丢弃 | `CompactStrategy.pickFullCompaction`、`MergeTreeCompactManager` 的 `dropDelete` |
| 旧文件未删除 | `SnapshotDeletion`（快照过期时） |

## 思考题（下次讨论）
1. 为什么订单 4、5 落在同一个 bucket？bucket 是怎么算出来的？（提示：`FixedBucketRowKeyExtractor`，按主键中去掉分区键后的字段哈希）
2. 步骤 2 的 DELETE 写的是一条什么样的记录？value 里存的是什么？
3. 如果不做步骤 4，读 `dt=2026-09-24` 时 split 是 raw 还是需要 merge？为什么？（提示：06 章 `MergeTreeSplitGenerator`）
4. 用 `SELECT * FROM orders /*+ OPTIONS('scan.snapshot-id'='1') */` 做时间旅行，能读到订单 3 吗？为什么旧文件必须保留？
5. 怎样让旧文件被真正删除？试试 `CALL sys.expire_snapshots(...)`。
