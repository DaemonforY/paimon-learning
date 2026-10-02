-- 实验 4 / 对照实验：写缓冲装不下时（S1 第 5 期素材）
-- 观察点：同样 50000 行、只有 10 个主键，写缓冲压到最小（256 kb）：
--   可溢写（默认 write-buffer-spillable=true）vs 不可溢写，各写出几个文件、多少条记录？
-- 关闭自动 compaction（write-only），只看写缓冲本身的效果

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- 单并行度：保证 50000 行按 id 顺序到达，结果可复现
SET 'parallelism.default' = '1';

CREATE TEMPORARY TABLE gen (
  id BIGINT
) WITH (
  'connector' = 'datagen',
  'fields.id.kind' = 'sequence',
  'fields.id.start' = '1',
  'fields.id.end' = '50000',
  'number-of-rows' = '50000'
);

DROP TABLE IF EXISTS wb_spill;
CREATE TABLE wb_spill (
  k BIGINT,
  v BIGINT,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'write-only' = 'true',
  'write-buffer-size' = '256 kb',
  'page-size' = '64 kb'
);

DROP TABLE IF EXISTS wb_nospill;
CREATE TABLE wb_nospill (
  k BIGINT,
  v BIGINT,
  PRIMARY KEY (k) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'write-only' = 'true',
  'write-buffer-size' = '256 kb',
  'page-size' = '64 kb',
  'write-buffer-spillable' = 'false'
);

INSERT INTO wb_spill   SELECT id % 10, id FROM gen;
INSERT INTO wb_nospill SELECT id % 10, id FROM gen;

SELECT 'spill' AS t, COUNT(*) AS files, SUM(record_count) AS records FROM `wb_spill$files`;
SELECT 'nospill' AS t, COUNT(*) AS files, SUM(record_count) AS records FROM `wb_nospill$files`;
SELECT record_count, min_sequence_number, max_sequence_number FROM `wb_nospill$files` ORDER BY min_sequence_number;
SELECT * FROM wb_nospill ORDER BY k;
