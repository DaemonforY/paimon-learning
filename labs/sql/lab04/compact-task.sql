-- 实验 4 / 补充：合并怎么执行——升级 vs 重写（S2 第 4 讲素材，配合 jdb/s2-4-compact-task.txt）
-- 同样的 5 次提交写进两张表，只差小文件阈值：
--   ct_def：默认 compaction.small-file-ratio = 0.7，阈值 = 64 KiB × 0.7 ≈ 44.8 KiB → 所有文件都算“小文件”
--   ct    ：small-file-ratio = 0.05，阈值 = 64 KiB × 0.05 = 3.2 KiB → 约 7.4 KB 的文件算“大文件”
-- target-file-size = 64 kb：写入时每 2000 行左右滚动一个文件（按未压缩大小判断，压缩后约 7.4 KB）
-- 提交顺序（越早越旧）：
--   E：10 行，key 300001~300010（孤立的小文件）
--   A：12000 行，key 1~12000
--   B：12000 行，key 100001~112000（不与任何文件重叠）
--   C：12000 行，key 1~12000（更新 A，与 A 重叠）
--   D：12000 行，key 200001~212000（不与任何文件重叠）

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

SET 'parallelism.default' = '1';
CREATE TEMPORARY TABLE ga (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '12000', 'number-of-rows' = '12000');
CREATE TEMPORARY TABLE gb (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '100001', 'fields.id.end' = '112000', 'number-of-rows' = '12000');
CREATE TEMPORARY TABLE gd (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '200001', 'fields.id.end' = '212000', 'number-of-rows' = '12000');

-- ==================== ct_def ====================
DROP TABLE IF EXISTS ct_def;
CREATE TABLE ct_def (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED)
WITH ('bucket' = '1', 'target-file-size' = '64 kb');
-- E
INSERT INTO ct_def VALUES (300001, 'e'), (300002, 'e'), (300003, 'e'), (300004, 'e'), (300005, 'e'), (300006, 'e'), (300007, 'e'), (300008, 'e'), (300009, 'e'), (300010, 'e');
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct_def$files` ORDER BY level, min_key;
-- A
INSERT INTO ct_def SELECT id, 'a' FROM ga;
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct_def$files` ORDER BY level, min_key;
-- B
INSERT INTO ct_def SELECT id, 'b' FROM gb;
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct_def$files` ORDER BY level, min_key;
-- C（更新 A 的全部 key）
INSERT INTO ct_def SELECT id, 'c' FROM ga;
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct_def$files` ORDER BY level, min_key;
-- D
INSERT INTO ct_def SELECT id, 'd' FROM gd;
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct_def$files` ORDER BY level, min_key;
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `ct_def$snapshots`;

-- ==================== ct ====================
DROP TABLE IF EXISTS ct;
CREATE TABLE ct (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED)
WITH ('bucket' = '1', 'target-file-size' = '64 kb', 'compaction.small-file-ratio' = '0.05');
-- E
INSERT INTO ct VALUES (300001, 'e'), (300002, 'e'), (300003, 'e'), (300004, 'e'), (300005, 'e'), (300006, 'e'), (300007, 'e'), (300008, 'e'), (300009, 'e'), (300010, 'e');
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct$files` ORDER BY level, min_key;
-- A
INSERT INTO ct SELECT id, 'a' FROM ga;
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct$files` ORDER BY level, min_key;
-- B
INSERT INTO ct SELECT id, 'b' FROM gb;
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct$files` ORDER BY level, min_key;
-- C（更新 A 的全部 key）
INSERT INTO ct SELECT id, 'c' FROM ga;
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct$files` ORDER BY level, min_key;
-- D
INSERT INTO ct SELECT id, 'd' FROM gd;
SELECT level, CONCAT(REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1), '…-', REGEXP_EXTRACT(file_path, '-([0-9]+)\.parquet', 1)) AS file, file_size_in_bytes AS size_b, record_count AS cnt, min_key, max_key FROM `ct$files` ORDER BY level, min_key;
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `ct$snapshots`;
