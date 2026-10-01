# 02 paimon-core 整体架构

## 学习目标
- 说出 paimon-core 的分层，以及每层的核心类。
- 画出表在存储上的元数据树（snapshot → manifest list → manifest → data file）。
- 讲清写、提交、读三条主流程经过的类。

## 一句话结论
Paimon = **按 bucket 组织的 LSM 树（数据）** + **snapshot/manifest 元数据树（版本）** + **乐观并发提交（ACID）**。主键表走 `KeyValueFileStore`（LSM），append 表走 `AppendOnlyFileStore`（不合并）。

## 1. 包结构与规模

`paimon-core/src/main/java/org/apache/paimon/` 下主要包（括号内为文件数）：

| 包 | 作用 |
|---|---|
| `table/`（264，最大） | 对外 Table 抽象：`FileStoreTable`、sink、source、系统表 |
| `mergetree/`（115） | **LSM 核心**：写缓冲、层级、合并、合并函数、lookup |
| `operation/`（92） | FileStore 的操作：scan / write / commit / read / 过期 / 清理 |
| `index/`（49）、`bucket/`、`crosspartition/` | 动态桶、跨分区更新的索引 |
| `io/`（44） | 数据文件读写、`DataFileMeta` |
| `manifest/`（39） | 元数据文件：`ManifestEntry`、`ManifestFile`、`ManifestList` |
| `append/`（37） | append 表的写入与小文件合并 |
| `iceberg/`（30） | 生成 Iceberg 兼容元数据 |
| `lookup/`、`disk/`、`sort/` | 本地 lookup 缓存、IO 管理、外部排序 |
| `catalog/`、`jdbc/`、`rest/` | Catalog 实现 |
| `deletionvectors/` | 删除向量 |
| `schema/` | Schema 管理与演进 |
| `tag/`、`consumer/` | Tag、消费者位点 |
| `globalindex/` | 全局索引、向量/全文检索等新特性 |

## 2. 分层

```
┌───────────────────────────────────────────────────────────────┐
│ Catalog 层     catalog/  jdbc/  rest/                          │ 管理库表与 Schema
├───────────────────────────────────────────────────────────────┤
│ Table 层       table/                                          │ 给 Flink/Spark 的 API
│                FileStoreTable ─┬─ PrimaryKeyFileStoreTable     │
│                                └─ AppendOnlyFileStoreTable     │
│                table/sink:   WriteBuilder→TableWrite→TableCommit│
│                table/source: ReadBuilder→TableScan→TableRead   │
├───────────────────────────────────────────────────────────────┤
│ FileStore 层   FileStore ─┬─ KeyValueFileStore  （主键表）      │ 核心引擎
│                           └─ AppendOnlyFileStore（append 表）   │
│                operation/: FileStoreScan / FileStoreWrite /    │
│                            FileStoreCommit / SplitRead         │
├───────────────────────────────────────────────────────────────┤
│ 存储结构层     mergetree/(LSM)  append/  io/  manifest/         │
│                index/  deletionvectors/  SnapshotManager  SchemaManager │
└───────────────────────────────────────────────────────────────┘
```

**入口建议**：先读 `FileStore.java`，它是整个 core 的“总控台”：`newScan()`、`newWrite()`、`newCommit()`、`newRead()`、`snapshotManager()` 等方法与各包一一对应。

**两类表**：
- 主键表：数据以 `KeyValue`（key + sequenceNumber + RowKind + value）存储，按 LSM 组织。
- append 表：无合并，文件追加；合并只做小文件合并。

## 3. 存储布局（元数据树）

```
table/
├── snapshot/snapshot-N            ← Snapshot（JSON，类定义在 paimon-api）
│     ├─ baseManifestList       ─┐    上一版本的全量文件清单
│     ├─ deltaManifestList      ─┤    本次提交新增/删除的文件
│     ├─ changelogManifestList      本次产生的 changelog（供流读）
│     └─ indexManifest              索引文件（动态桶索引、删除向量）
├── manifest/
│     ├─ manifest-list-xxx         ← ManifestList：ManifestFileMeta 列表
│     └─ manifest-xxx              ← ManifestFile：ManifestEntry(ADD/DELETE + DataFileMeta) 列表
├── schema/schema-N                ← SchemaManager
├── index/                         ← 索引文件
├── consumer/consumer-<id>         ← 消费者位点
└── pt=xx/bucket-N/data-xxx.parquet ← 数据文件（DataFileMeta 记录 level、min/max key、统计、seq 范围）
```

- **bucket 是读写并行的最小单元**，每个 bucket 一棵独立的 LSM 树。
- `DataFileMeta` 关键字段：`fileName`、`level`、`minKey/maxKey`、`minSequenceNumber/maxSequenceNumber`、`rowCount`、`deleteRowCount`、`valueStats`。
- `ManifestEntry` = `FileKind(ADD/DELETE)` + partition + bucket + `DataFileMeta`。

## 4. 三条主流程

### ① 写入：`TableWrite` → `MergeTreeWriter`

```
TableWriteImpl.write(row)
  → KeyValueFileStoreWrite 按 (partition, bucket) 找到/创建 writer
    → MergeTreeWriter.write(kv)                        mergetree/MergeTreeWriter.java
        writeBuffer.put(seq, kind, key, value)          内存排序缓冲（SortBufferWriteBuffer，可溢写）
        缓冲满 → flushWriteBuffer()
            排序后写出一个 L0 文件 → compactManager.addNewFile()
            → compactManager.triggerCompaction()        异步合并
  → prepareCommit(waitCompaction)
        返回 CommitIncrement（新文件 + 合并前后文件 + changelog）
        → 包装成 CommitMessage 交给提交端
```

### ② 提交：`TableCommit` → `FileStoreCommitImpl`
```
FileStoreCommitImpl.commit(ManifestCommittable)
  → tryCommit 重试循环 → tryCommitOnce
      1. 幂等检查、冲突检测
      2. 写 delta manifest + manifest list，必要时合并旧 manifest
      3. SnapshotCommit 原子创建 snapshot-(N+1)
```
详见 [05 章](05-FileStoreCommitImpl冲突检测与重试.md)。

### ③ 读取：`TableScan`（规划）+ `TableRead`（读取）
```
规划：DataTableScan
  → StartingScanner 决定起点；流读用 FollowUpScanner 追新快照
  → SnapshotReader → KeyValueFileStoreScan：读 manifest，按分区/bucket/统计裁剪
  → MergeTreeSplitGenerator 生成 DataSplit（标记 rawConvertible）
读取：KeyValueTableRead 选择 SplitRead
  ├ RawFileSplitRead：直接读文件（+ 删除向量过滤）
  └ MergeFileSplitRead：IntervalPartition → SortMergeReader(败者树) → MergeFunction → DropDeleteReader
```
详见 [06 章](06-读路径与删除向量.md)。

## 5. LSM 核心 `mergetree/` 速览

| 概念 | 类 | 说明 |
|---|---|---|
| 层级 | `Levels`、`SortedRun`、`LevelSortedRun` | L0 文件可重叠；≥1 层每层是一个有序 run |
| 合并策略 | `compact/UniversalCompaction` | 见 03 章 |
| 合并执行 | `MergeTreeCompactManager` → `MergeTreeCompactTask` → `*CompactRewriter` | 见 04 章 |
| 合并引擎 | `DeduplicateMergeFunction`、`PartialUpdateMergeFunction`、`aggregate/`、`FirstRowMergeFunction` | 同 key 多版本如何合并 |
| Changelog | `LookupChangelogMergeFunctionWrapper`、`FullChangelogMergeFunctionWrapper` | 见 07 章 |
| 多路归并 | `SortMergeReaderWithLoserTree` / `...WithMinHeap` | 默认败者树 |

## 动手实验
- [ ] 跑 `PrimaryKeySimpleTableTest` 任意用例，在 `MergeTreeWriter.write`、`flushWriteBuffer`、`FileStoreCommitImpl.tryCommitOnce` 下断点。
- [ ] 写几次数据后，打开表目录，用 `cat snapshot/snapshot-1` 看 JSON 内容，找到对应的 manifest list 文件。
- [ ] 查询 `t$files`，观察 level、min/max key、row count。

## 自测题
1. base / delta / changelog 三种 manifest list 各记录什么？
2. 为什么说 bucket 是并行的最小单元？
3. `KeyValue` 比普通行多了哪些字段？各有什么用？
4. 写入时，数据什么时候变成文件？什么时候对读可见？
