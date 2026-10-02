-- 实验 4 / 对照实验：写缓冲刷盘时按主键合并（S1 第 5 期素材）
-- 观察点：
--   1) 一条 INSERT 写 3 行（订单 1 出现两次）→ 文件里有几行？
--   2) 同样 3 行分两次 INSERT → 文件里有几行？查询结果呢？
--   3) 换成 aggregation 合并引擎 → 同 key 的两行是“丢一行”还是“加起来”？
--   4) changelog-producer = input → 被合并掉的 'A' 还能在哪里找到？
--   （写缓冲装不下的情况见 compare-buffer-full.sql）

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- ---------- 1) 一次写入 ----------
DROP TABLE IF EXISTS wb_one;
CREATE TABLE wb_one (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1');

INSERT INTO wb_one VALUES (1, 'A'), (2, 'B'), (1, 'C');

SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `wb_one$snapshots`;
SELECT level, record_count, min_sequence_number, max_sequence_number FROM `wb_one$files`;
-- 快照 1 是唯一的历史版本：订单 1 的 'A' 能查到吗？
SELECT * FROM `wb_one$audit_log` /*+ OPTIONS('scan.snapshot-id' = '1') */;
-- @sh echo '=== wb_one 的数据文件 ===' && find ${warehouse_dir}/default.db/wb_one -name '*.parquet' | sed 's|.*/wb_one/||' | sort

-- ---------- 2) 分两次写入 ----------
DROP TABLE IF EXISTS wb_two;
CREATE TABLE wb_two (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1');

INSERT INTO wb_two VALUES (1, 'A'), (2, 'B');
INSERT INTO wb_two VALUES (1, 'C');

SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `wb_two$snapshots`;
SELECT level, record_count, min_sequence_number, max_sequence_number FROM `wb_two$files`;
SELECT * FROM wb_two;

-- ---------- 3) aggregation 合并引擎 ----------
DROP TABLE IF EXISTS wb_agg;
CREATE TABLE wb_agg (
  order_id BIGINT,
  amount   INT,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'merge-engine' = 'aggregation',
  'fields.amount.aggregate-function' = 'sum'
);

INSERT INTO wb_agg VALUES (1, 10), (2, 20), (1, 5);

SELECT level, record_count FROM `wb_agg$files`;
SELECT * FROM wb_agg;

-- ---------- 4) changelog-producer = input ----------
DROP TABLE IF EXISTS wb_input;
CREATE TABLE wb_input (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'changelog-producer' = 'input'
);

INSERT INTO wb_input VALUES (1, 'A'), (2, 'B'), (1, 'C');

-- 数据文件：和 wb_one 一样只有 2 行
SELECT level, record_count FROM `wb_input$files`;
-- changelog 文件：原样保留 3 行
SELECT * FROM `wb_input$audit_log` /*+ OPTIONS('incremental-between' = '0,1', 'incremental-between-scan-mode' = 'changelog') */;
-- @sh echo '=== wb_input 的文件 ===' && find ${warehouse_dir}/default.db/wb_input -name '*.parquet' | sed 's|.*/wb_input/||' | sort
