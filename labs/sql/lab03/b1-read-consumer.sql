-- 实验 3 / B1 与 B3：带 consumer-id 的流读（B1 首次启动，B3 用同一个 consumer-id 重启）
-- 观察点：首次启动读到什么？重启后（没有任何 Flink 状态）从哪里继续？
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

SELECT * FROM orders_c /*+ OPTIONS('consumer-id' = 'lab03', 'continuous.discovery-interval' = '1s') */;
