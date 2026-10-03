-- 实验 4 / 补充：UniversalCompaction 怎么选文件（S2 第 3 讲素材，配合 jdb/s2-3-universal.txt）
-- 先用全量合并做出一个大的 L5 底座，再每次提交写 1 行，观察第几次提交触发合并、输出到哪一层
-- 默认参数：num-sorted-run.compaction-trigger = 5，num-levels = 6（maxLevel = 5），size-ratio = 1%，max-size-amplification = 200%

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

SET 'parallelism.default' = '1';

CREATE TEMPORARY TABLE gen (id BIGINT) WITH (
  'connector' = 'datagen',
  'fields.id.kind' = 'sequence',
  'fields.id.start' = '1',
  'fields.id.end' = '2000',
  'number-of-rows' = '2000'
);

DROP TABLE IF EXISTS uc;
CREATE TABLE uc (
  k BIGINT,
  v STRING,
  PRIMARY KEY (k) NOT ENFORCED
) WITH ('bucket' = '1');

-- 底座：2000 行，全量合并到 L5
INSERT INTO uc SELECT id, 'base' FROM gen;
CALL sys.compact(`table` => 'default.uc');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

-- 第 1 次小提交
INSERT INTO uc VALUES (10001, 'x');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

-- 第 2 次小提交
INSERT INTO uc VALUES (10002, 'x');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

-- 第 3 次小提交
INSERT INTO uc VALUES (10003, 'x');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

-- 第 4 次小提交
INSERT INTO uc VALUES (10004, 'x');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

-- 第 5 次小提交
INSERT INTO uc VALUES (10005, 'x');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

-- 第 6 次小提交
INSERT INTO uc VALUES (10006, 'x');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

-- 第 7 次小提交
INSERT INTO uc VALUES (10007, 'x');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

-- 第 8 次小提交
INSERT INTO uc VALUES (10008, 'x');
SELECT level, file_size_in_bytes, record_count, min_sequence_number, max_sequence_number FROM `uc$files` ORDER BY level, max_sequence_number DESC;

SELECT snapshot_id, commit_kind, total_record_count FROM `uc$snapshots`;
