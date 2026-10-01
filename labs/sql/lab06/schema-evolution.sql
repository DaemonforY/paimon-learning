-- 实验 6：Schema 演进——Paimon 靠“字段 id”而不是“列名”对应新旧文件里的列
-- 观察点：
--   1) 每次 ALTER 都会生成一个新的 schema-N 文件，字段 id 如何分配？
--   2) 加列后，旧数据文件里没有这一列，读出来是什么？
--   3) 改列名后，旧数据还能读出来吗？
--   4) 删掉一列再加回同名列，旧数据会“回来”吗？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS schema_demo;
CREATE TABLE schema_demo (
  order_id BIGINT,
  status   STRING,
  amount   DECIMAL(10, 2),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1');

INSERT INTO schema_demo VALUES (1, 'CREATED', 99.90), (2, 'PAID', 15.00);
-- @sh echo '=== schema-0 的字段 ===' && grep -E '"(id|name|highestFieldId)"' ${warehouse_dir}/default.db/schema_demo/schema/schema-0

-- ① 加列：新列 remark 拿到新的字段 id = 3
ALTER TABLE schema_demo ADD remark STRING;
-- @sh echo '=== schema 目录 ===' && ls ${warehouse_dir}/default.db/schema_demo/schema/ && echo '=== schema-1 的字段 ===' && grep -E '"(id|name|highestFieldId)"' ${warehouse_dir}/default.db/schema_demo/schema/schema-1
INSERT INTO schema_demo VALUES (3, 'CREATED', 8.80, 'VIP');
-- 旧数据文件（schema 0 写的）里没有 remark，读出来是 NULL
SELECT * FROM schema_demo ORDER BY order_id;

-- ② 改列名：status → order_status，字段 id 不变（仍是 1），旧数据照常读出
ALTER TABLE schema_demo RENAME status TO order_status;
-- @sh echo '=== schema-2 的字段 ===' && grep -E '"(id|name|highestFieldId)"' ${warehouse_dir}/default.db/schema_demo/schema/schema-2
SELECT * FROM schema_demo ORDER BY order_id;

-- ③ 删列再加回同名列：新的 amount 拿到新 id，旧文件里 id=2 的数据不再对应它
ALTER TABLE schema_demo DROP amount;
ALTER TABLE schema_demo ADD amount DECIMAL(10, 2);
-- @sh echo '=== schema-4 的字段 ===' && grep -E '"(id|name|highestFieldId)"' ${warehouse_dir}/default.db/schema_demo/schema/schema-4
SELECT * FROM schema_demo ORDER BY order_id;

-- 每个数据文件记录了写入时的 schema_id
SELECT schema_id, record_count, min_key, max_key FROM `schema_demo$files` ORDER BY schema_id;
SELECT schema_id, fields FROM `schema_demo$schemas`;
