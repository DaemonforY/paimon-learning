-- 实验 13：Spark MERGE INTO 的三条路径（S2 第 11 讲素材；用 ./spark.sh 运行）
-- 三张表各先分两批写入 (1~3)、(4~6)，得到两个数据文件；再执行同一条 MERGE：
--   id 2 更新为 'B'、id 3 删除、id 7 插入
-- 观察：执行计划走哪条路径？哪些文件被重写、哪些原样保留？快照是什么类型？

USE paimon.default;

CREATE TEMPORARY VIEW s AS SELECT * FROM VALUES (2, 'B', 'U'), (3, 'C', 'D'), (7, 'G', 'I') AS s(id, v, op);

-- ==================== 主键表 ====================
DROP TABLE IF EXISTS t_pk;
CREATE TABLE t_pk (id INT, v STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1');
INSERT INTO t_pk VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_pk VALUES (4, 'd'), (5, 'e'), (6, 'f');
SELECT level, record_count, regexp_extract(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `t_pk$files` ORDER BY level, file;
EXPLAIN MERGE INTO t_pk t USING s ON t.id = s.id
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
MERGE INTO t_pk t USING s ON t.id = s.id
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
SELECT level, record_count, regexp_extract(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `t_pk$files` ORDER BY level, file;
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `t_pk$snapshots`;
SELECT * FROM `t_pk$audit_log` VERSION AS OF 3 ORDER BY id;
SELECT * FROM t_pk ORDER BY id;

-- ==================== Append 表（copy-on-write） ====================
DROP TABLE IF EXISTS t_cow;
CREATE TABLE t_cow (id INT, v STRING);
INSERT INTO t_cow VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_cow VALUES (4, 'd'), (5, 'e'), (6, 'f');
SELECT record_count, regexp_extract(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `t_cow$files` ORDER BY file;
SELECT id, v, regexp_extract(__paimon_file_path, 'data-([0-9a-f]{8})', 1) AS file FROM t_cow ORDER BY id;
EXPLAIN MERGE INTO t_cow t USING s ON t.id = s.id
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
MERGE INTO t_cow t USING s ON t.id = s.id
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
SELECT record_count, regexp_extract(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `t_cow$files` ORDER BY file;
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `t_cow$snapshots`;
SELECT id, v, regexp_extract(__paimon_file_path, 'data-([0-9a-f]{8})', 1) AS file FROM t_cow ORDER BY id;

-- ==================== Append 表 + 删除向量 ====================
DROP TABLE IF EXISTS t_dv;
CREATE TABLE t_dv (id INT, v STRING) TBLPROPERTIES ('deletion-vectors.enabled' = 'true');
INSERT INTO t_dv VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_dv VALUES (4, 'd'), (5, 'e'), (6, 'f');
SELECT record_count, regexp_extract(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `t_dv$files` ORDER BY file;
SELECT id, v, regexp_extract(__paimon_file_path, 'data-([0-9a-f]{8})', 1) AS file, __paimon_row_index AS row_index FROM t_dv ORDER BY id;
EXPLAIN MERGE INTO t_dv t USING s ON t.id = s.id
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
MERGE INTO t_dv t USING s ON t.id = s.id
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
SELECT record_count, regexp_extract(file_path, 'data-([0-9a-f]{8})', 1) AS file FROM `t_dv$files` ORDER BY file;
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `t_dv$snapshots`;
SELECT index_type, row_count, file_size FROM `t_dv$table_indexes`;
SELECT id, v, regexp_extract(__paimon_file_path, 'data-([0-9a-f]{8})', 1) AS file, __paimon_row_index AS row_index FROM t_dv ORDER BY id;

-- ==================== Append 表 + V2 写（Spark 原生 copy-on-write） ====================
-- 默认 spark.paimon.write.use-v2-write = false，Append 表也走 V1；打开后才交给 Spark 的 V2 行级操作
SET `spark.paimon.write.use-v2-write`=true;
DROP TABLE IF EXISTS t_v2;
CREATE TABLE t_v2 (id INT, v STRING);
INSERT INTO t_v2 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_v2 VALUES (4, 'd'), (5, 'e'), (6, 'f');
SELECT id, v, regexp_extract(__paimon_file_path, 'data-([0-9a-f]{8})', 1) AS file FROM t_v2 ORDER BY id;
EXPLAIN MERGE INTO t_v2 t USING s ON t.id = s.id
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
MERGE INTO t_v2 t USING s ON t.id = s.id
  WHEN MATCHED AND s.op = 'D' THEN DELETE
  WHEN MATCHED THEN UPDATE SET v = s.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);
SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `t_v2$snapshots`;
SELECT id, v, regexp_extract(__paimon_file_path, 'data-([0-9a-f]{8})', 1) AS file FROM t_v2 ORDER BY id;
SET `spark.paimon.write.use-v2-write`=false;

-- ==================== 坑：一行 target 匹配多行 source ====================
CREATE TEMPORARY VIEW s_dup AS SELECT * FROM VALUES (2, 'X'), (2, 'Y') AS s(id, v);
-- @expect-error
MERGE INTO t_cow t USING s_dup ON t.id = s_dup.id WHEN MATCHED THEN UPDATE SET v = s_dup.v;
