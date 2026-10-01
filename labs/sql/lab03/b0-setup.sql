-- 实验 3 / B0：两张完全相同的 lookup 表
--   orders_c  ：会被 consumer-id = 'lab03' 的流作业读取
--   orders_nc ：对照组，没有 consumer
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS orders_c;
DROP TABLE IF EXISTS orders_nc;

-- 使用 consumer-id 必须配置 consumer.expiration-time（FlinkSourceBuilder 中强制校验），
-- 否则一个被遗弃的 consumer 会让快照永远无法过期
CREATE TABLE orders_c (
  order_id BIGINT, status STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1', 'changelog-producer' = 'lookup', 'consumer.expiration-time' = '1 h');

CREATE TABLE orders_nc (
  order_id BIGINT, status STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1', 'changelog-producer' = 'lookup');

-- 第 1 轮
INSERT INTO orders_c  VALUES (1, 'CREATED'), (2, 'CREATED'), (3, 'CREATED');
INSERT INTO orders_nc VALUES (1, 'CREATED'), (2, 'CREATED'), (3, 'CREATED');
