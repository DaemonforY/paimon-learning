-- 实验 3 / A：流读 changelog-producer = lookup 的表
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

EXPLAIN SELECT * FROM orders_lookup /*+ OPTIONS('continuous.discovery-interval' = '1s') */;
SELECT * FROM orders_lookup /*+ OPTIONS('continuous.discovery-interval' = '1s') */;
