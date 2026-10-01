-- 实验 3 / A：流读 changelog-producer = none 的表
-- 观察点：执行计划里有没有 ChangelogNormalize？输出的 RowKind 是谁生成的？
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

EXPLAIN SELECT * FROM orders_none /*+ OPTIONS('continuous.discovery-interval' = '1s') */;
SELECT * FROM orders_none /*+ OPTIONS('continuous.discovery-interval' = '1s') */;
