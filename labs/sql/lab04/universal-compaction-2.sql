-- 实验 4 / 补充：UniversalCompaction 的另外两条规则（S2 第 3 讲素材）
--   uc_amp：底座很小 → 新数据超过底座 2 倍 → 空间放大，全量合并到 L5
--   uc_num：每次提交越来越小 → 大小比例吸收不了 → run 数超过 5 时按文件数兜底

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

SET 'parallelism.default' = '1';

-- ---------- uc_amp：空间放大 ----------
DROP TABLE IF EXISTS uc_amp;
CREATE TABLE uc_amp (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
INSERT INTO uc_amp VALUES (1, 'base');
CALL sys.compact(`table` => 'default.uc_amp');
SELECT level, file_size_in_bytes, record_count FROM `uc_amp$files` ORDER BY level, max_sequence_number DESC;
INSERT INTO uc_amp VALUES (101, 'x');
SELECT level, file_size_in_bytes, record_count FROM `uc_amp$files` ORDER BY level, max_sequence_number DESC;
INSERT INTO uc_amp VALUES (102, 'x');
SELECT level, file_size_in_bytes, record_count FROM `uc_amp$files` ORDER BY level, max_sequence_number DESC;
INSERT INTO uc_amp VALUES (103, 'x');
SELECT level, file_size_in_bytes, record_count FROM `uc_amp$files` ORDER BY level, max_sequence_number DESC;
INSERT INTO uc_amp VALUES (104, 'x');
SELECT level, file_size_in_bytes, record_count FROM `uc_amp$files` ORDER BY level, max_sequence_number DESC;

-- ---------- uc_num：文件数兜底 ----------
DROP TABLE IF EXISTS uc_num;
CREATE TABLE uc_num (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
CREATE TEMPORARY TABLE base (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '2000', 'number-of-rows' = '2000');
CREATE TEMPORARY TABLE g1 (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '10001', 'fields.id.end' = '10400', 'number-of-rows' = '400');
CREATE TEMPORARY TABLE g2 (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '20001', 'fields.id.end' = '20200', 'number-of-rows' = '200');
CREATE TEMPORARY TABLE g3 (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '30001', 'fields.id.end' = '30100', 'number-of-rows' = '100');
CREATE TEMPORARY TABLE g4 (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '40001', 'fields.id.end' = '40050', 'number-of-rows' = '50');
CREATE TEMPORARY TABLE g5 (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '50001', 'fields.id.end' = '50025', 'number-of-rows' = '25');
INSERT INTO uc_num SELECT id, 'base' FROM base;
CALL sys.compact(`table` => 'default.uc_num');
SELECT level, file_size_in_bytes, record_count FROM `uc_num$files` ORDER BY level, max_sequence_number DESC;
-- 第 1 次提交（行数递减，越新越小）
INSERT INTO uc_num SELECT id, 'x' FROM g1;
SELECT level, file_size_in_bytes, record_count FROM `uc_num$files` ORDER BY level, max_sequence_number DESC;
-- 第 2 次提交（行数递减，越新越小）
INSERT INTO uc_num SELECT id, 'x' FROM g2;
SELECT level, file_size_in_bytes, record_count FROM `uc_num$files` ORDER BY level, max_sequence_number DESC;
-- 第 3 次提交（行数递减，越新越小）
INSERT INTO uc_num SELECT id, 'x' FROM g3;
SELECT level, file_size_in_bytes, record_count FROM `uc_num$files` ORDER BY level, max_sequence_number DESC;
-- 第 4 次提交（行数递减，越新越小）
INSERT INTO uc_num SELECT id, 'x' FROM g4;
SELECT level, file_size_in_bytes, record_count FROM `uc_num$files` ORDER BY level, max_sequence_number DESC;
-- 第 5 次提交（行数递减，越新越小）
INSERT INTO uc_num SELECT id, 'x' FROM g5;
SELECT level, file_size_in_bytes, record_count FROM `uc_num$files` ORDER BY level, max_sequence_number DESC;
