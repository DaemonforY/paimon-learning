-- 实验 3 / B4：对照组。没有 consumer 的表，从“停下时的位置”（快照 3）开始流读
-- 观察点：快照 3 已被过期，会报错吗？读到的数据完整吗？
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

SELECT * FROM orders_nc /*+ OPTIONS('scan.snapshot-id' = '3', 'continuous.discovery-interval' = '1s') */;
