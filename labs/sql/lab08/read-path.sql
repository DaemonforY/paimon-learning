-- 实验 8：读路径——什么时候直接读文件、什么时候合并读（S2 第 6 讲素材，配合 jdb/s2-6-read-path.txt）
-- 五张表，每张表一种文件状态，最后都 SELECT 一次：
--   r_one      ：1 次写入 → 1 个 L0 文件
--   r_disjoint ：2 次写入，key 不重叠（1~100、101~200）→ 2 个 L0 文件
--   r_overlap  ：2 次写入，key 重叠（1~100、51~150）→ 2 个 L0 文件
--   r_compact  ：同 r_overlap，再全量合并 → 1 个 L5 文件
--   r_delete   ：写入后全量合并，再 DELETE 一行 → L5 + 一个含删除记录的 L0 文件

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

SET 'parallelism.default' = '1';

CREATE TEMPORARY TABLE g1 (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '1', 'fields.id.end' = '100', 'number-of-rows' = '100');
CREATE TEMPORARY TABLE g2 (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '101', 'fields.id.end' = '200', 'number-of-rows' = '100');
CREATE TEMPORARY TABLE g3 (id BIGINT) WITH ('connector' = 'datagen', 'fields.id.kind' = 'sequence', 'fields.id.start' = '51', 'fields.id.end' = '150', 'number-of-rows' = '100');

-- r_one
DROP TABLE IF EXISTS r_one;
CREATE TABLE r_one (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
INSERT INTO r_one SELECT id, 'a' FROM g1;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `r_one$files` ORDER BY level, min_key;

-- r_disjoint
DROP TABLE IF EXISTS r_disjoint;
CREATE TABLE r_disjoint (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
INSERT INTO r_disjoint SELECT id, 'a' FROM g1;
INSERT INTO r_disjoint SELECT id, 'b' FROM g2;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `r_disjoint$files` ORDER BY level, min_key;

-- r_overlap
DROP TABLE IF EXISTS r_overlap;
CREATE TABLE r_overlap (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
INSERT INTO r_overlap SELECT id, 'a' FROM g1;
INSERT INTO r_overlap SELECT id, 'b' FROM g3;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `r_overlap$files` ORDER BY level, min_key;

-- r_compact
DROP TABLE IF EXISTS r_compact;
CREATE TABLE r_compact (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
INSERT INTO r_compact SELECT id, 'a' FROM g1;
INSERT INTO r_compact SELECT id, 'b' FROM g3;
CALL sys.compact(`table` => 'default.r_compact');
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `r_compact$files` ORDER BY level, min_key;

-- r_delete
DROP TABLE IF EXISTS r_delete;
CREATE TABLE r_delete (k BIGINT, v STRING, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1');
INSERT INTO r_delete SELECT id, 'a' FROM g1;
CALL sys.compact(`table` => 'default.r_delete');
DELETE FROM r_delete WHERE k = 5;
SELECT level, record_count, min_key, max_key, `deleteRowCount` FROM `r_delete$files` ORDER BY level, min_key;

-- ---------- 读：每张表一次，看 jdb 命中的是直接读还是合并读 ----------
-- 用 GROUP BY v（非分区字段）强制真正读文件；纯 COUNT(*) 可能被下推成“只看元数据”，见下面的 EXPLAIN
SELECT v, COUNT(*) AS cnt FROM r_one GROUP BY v;
SELECT v, COUNT(*) AS cnt FROM r_disjoint GROUP BY v;
SELECT v, COUNT(*) AS cnt FROM r_overlap GROUP BY v;
SELECT v, COUNT(*) AS cnt FROM r_compact GROUP BY v;
SELECT v, COUNT(*) AS cnt FROM r_delete GROUP BY v;

-- COUNT(*) 下推：所有 split 都能直接读时，直接用文件元数据里的行数，不读文件
EXPLAIN SELECT COUNT(*) FROM r_compact;
EXPLAIN SELECT COUNT(*) FROM r_overlap;

-- value 过滤：重叠的数据里，k = 60 的最新值是 'b'；按 v = 'a' 过滤不能把旧版本误读出来
SELECT * FROM r_overlap WHERE v = 'a' AND k BETWEEN 58 AND 62;
