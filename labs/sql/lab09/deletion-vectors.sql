-- 实验 9：删除向量（S2 第 7 讲素材，配合 jdb/s2-7-deletion-vectors.txt）
-- 两张 bucket = 1 的主键表写入完全相同的数据，只差 'deletion-vectors.enabled'：
--   ① 写 1~100（v='a'）  ② 更新 51~60（v='b'）  ③ DELETE k = 5
-- 观察：文件在哪一层？有没有删除向量索引文件？读的时候是直接读还是合并读？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

SET 'parallelism.default' = '1';

CREATE TEMPORARY TABLE g_all (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '100', 'number-of-rows' = '100');
CREATE TEMPORARY TABLE g_upd (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '51', 'fields.id.end' = '60', 'number-of-rows' = '10');

-- ==================== dv_off ====================
DROP TABLE IF EXISTS dv_off;
CREATE TABLE dv_off (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
-- ① 写入
INSERT INTO dv_off SELECT id, 'a' FROM g_all;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `dv_off$files` ORDER BY level, min_key;
-- ② 更新 10 行
INSERT INTO dv_off SELECT id, 'b' FROM g_upd;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `dv_off$files` ORDER BY level, min_key;
-- ③ 删除 1 行
DELETE FROM dv_off WHERE k = 5;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `dv_off$files` ORDER BY level, min_key;

-- ==================== dv_on ====================
DROP TABLE IF EXISTS dv_on;
CREATE TABLE dv_on (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1', 'deletion-vectors.enabled' = 'true');
-- ① 写入
INSERT INTO dv_on SELECT id, 'a' FROM g_all;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `dv_on$files` ORDER BY level, min_key;
-- ② 更新 10 行
INSERT INTO dv_on SELECT id, 'b' FROM g_upd;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `dv_on$files` ORDER BY level, min_key;
SELECT index_type, row_count, file_size FROM `dv_on$table_indexes`;
-- ③ 删除 1 行
DELETE FROM dv_on WHERE k = 5;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `dv_on$files` ORDER BY level, min_key;
SELECT index_type, row_count, file_size FROM `dv_on$table_indexes`;

-- @sh echo '=== dv_on 的 index 目录 ===' && ls ${warehouse_dir}/default.db/dv_on/index/ | sed 's/-[0-9a-f-]*-/-…-/'

-- ---------- 读：GROUP BY v 强制读文件（jdb 看直接读还是合并读） ----------
SELECT v, COUNT(*) AS cnt FROM dv_off GROUP BY v;
SELECT v, COUNT(*) AS cnt FROM dv_on GROUP BY v;

-- COUNT(*) 下推：dv_on 的文件行数减去删除向量里的行数
EXPLAIN SELECT COUNT(*) FROM dv_off;
EXPLAIN SELECT COUNT(*) FROM dv_on;
SELECT COUNT(*) AS cnt FROM dv_on;

-- ---------- 代价：L0 在合并之前不可见 ----------
-- write-only：只写不合并，新数据停在 L0
DROP TABLE IF EXISTS dv_vis;
CREATE TABLE dv_vis (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED)
WITH ('bucket' = '1', 'deletion-vectors.enabled' = 'true', 'write-only' = 'true');
INSERT INTO dv_vis VALUES (1, 'x'), (2, 'y');
SELECT level, record_count FROM `dv_vis$files`;
SELECT COUNT(*) AS visible_before_compact FROM dv_vis;
-- 打开 merge-on-read：L0 也读，但要走合并读
SELECT COUNT(*) AS visible_merge_on_read FROM dv_vis /*+ OPTIONS('deletion-vectors.merge-on-read' = 'true') */;
CALL sys.compact(`table` => 'default.dv_vis');
SELECT level, record_count FROM `dv_vis$files`;
SELECT COUNT(*) AS visible_after_compact FROM dv_vis;
