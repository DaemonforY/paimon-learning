-- 实验 10：Lookup Changelog（S2 第 8 讲素材，配合 jdb/s2-8-lookup-changelog.txt）
-- 同样三步写入，对比 changelog-producer = none / lookup / lookup + row-deduplicate / lookup + aggregation：
--   ① 写 (1,'a') (2,'b') (3,'c')   ② 写 (1,'A') (2,'b')：1 改了值，2 值没变   ③ DELETE k = 3
-- 观察：每次写入产生几个快照？changelog 记在哪个快照、有几条？读出来的 -U/+U 是什么？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

SET 'parallelism.default' = '1';

-- ==================== none（默认）：不产 changelog ====================
DROP TABLE IF EXISTS cl_none;
CREATE TABLE cl_none (k INT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
INSERT INTO cl_none VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO cl_none VALUES (1, 'A'), (2, 'b');
DELETE FROM cl_none WHERE k = 3;
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count, changelog_record_count FROM `cl_none$snapshots`;
SELECT level, record_count, min_key, max_key FROM `cl_none$files` ORDER BY level, min_key;
-- 增量读：没有 changelog 文件，读到的是写入的原始增量（delta）
SELECT * FROM `cl_none$audit_log` /*+ OPTIONS('incremental-between' = '1,3') */;

-- ==================== lookup ====================
DROP TABLE IF EXISTS cl_lookup;
CREATE TABLE cl_lookup (k INT, v STRING, PRIMARY KEY (k) NOT ENFORCED)
WITH ('bucket' = '1', 'changelog-producer' = 'lookup');
INSERT INTO cl_lookup VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count, changelog_record_count FROM `cl_lookup$snapshots`;
SELECT level, record_count, min_key, max_key FROM `cl_lookup$files` ORDER BY level, min_key;
INSERT INTO cl_lookup VALUES (1, 'A'), (2, 'b');
-- 升级时有没有重写文件：对比 APPEND 快照 3 里的 L0 文件名和 COMPACT 快照 4 里的文件名
SELECT level, record_count, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `cl_lookup$files` /*+ OPTIONS('scan.snapshot-id' = '3') */ ORDER BY level;
SELECT level, record_count, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `cl_lookup$files` /*+ OPTIONS('scan.snapshot-id' = '4') */ ORDER BY level;
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count, changelog_record_count FROM `cl_lookup$snapshots`;
SELECT level, record_count, min_key, max_key FROM `cl_lookup$files` ORDER BY level, min_key;
DELETE FROM cl_lookup WHERE k = 3;
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count, changelog_record_count FROM `cl_lookup$snapshots`;
SELECT level, record_count, min_key, max_key FROM `cl_lookup$files` ORDER BY level, min_key;
-- @sh echo '=== cl_lookup 的 changelog 文件 ===' && ls ${warehouse_dir}/default.db/cl_lookup/bucket-0/ | grep changelog | sed 's/-[0-9a-f-]\{36\}-/-…-/'
-- 逐个快照读 changelog（第 ①②③ 步各自产生的变更）
SELECT * FROM `cl_lookup$audit_log` /*+ OPTIONS('incremental-between' = '1,2', 'incremental-between-scan-mode' = 'changelog') */;
SELECT * FROM `cl_lookup$audit_log` /*+ OPTIONS('incremental-between' = '2,4', 'incremental-between-scan-mode' = 'changelog') */;
SELECT * FROM `cl_lookup$audit_log` /*+ OPTIONS('incremental-between' = '4,6', 'incremental-between-scan-mode' = 'changelog') */;

-- ==================== lookup + row-deduplicate：值没变就不输出 ====================
DROP TABLE IF EXISTS cl_dedup;
CREATE TABLE cl_dedup (k INT, v STRING, PRIMARY KEY (k) NOT ENFORCED)
WITH ('bucket' = '1', 'changelog-producer' = 'lookup', 'changelog-producer.row-deduplicate' = 'true');
INSERT INTO cl_dedup VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO cl_dedup VALUES (1, 'A'), (2, 'b');
SELECT * FROM `cl_dedup$audit_log` /*+ OPTIONS('incremental-between' = '2,4', 'incremental-between-scan-mode' = 'changelog') */;

-- ==================== lookup + aggregation：L0 里只有增量 ====================
DROP TABLE IF EXISTS cl_agg;
CREATE TABLE cl_agg (k INT, total BIGINT, PRIMARY KEY (k) NOT ENFORCED)
WITH ('bucket' = '1', 'changelog-producer' = 'lookup', 'merge-engine' = 'aggregation', 'fields.total.aggregate-function' = 'sum');
INSERT INTO cl_agg VALUES (1, 10), (2, 20);
INSERT INTO cl_agg VALUES (1, 5);
SELECT * FROM `cl_agg$audit_log` /*+ OPTIONS('incremental-between' = '2,4', 'incremental-between-scan-mode' = 'changelog') */;
SELECT level, record_count, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `cl_agg$files` /*+ OPTIONS('scan.snapshot-id' = '3') */ ORDER BY level;
SELECT level, record_count, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `cl_agg$files` /*+ OPTIONS('scan.snapshot-id' = '4') */ ORDER BY level;
SELECT * FROM cl_agg;
