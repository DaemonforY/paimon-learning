-- 实验 3 / A0：建两张只差 changelog-producer 的主键表
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS orders_none;
DROP TABLE IF EXISTS orders_lookup;

CREATE TABLE orders_none (
  order_id BIGINT, status STRING, amount DECIMAL(10, 2),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1', 'changelog-producer' = 'none');

CREATE TABLE orders_lookup (
  order_id BIGINT, status STRING, amount DECIMAL(10, 2),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1', 'changelog-producer' = 'lookup');

-- 第 1 轮：在流读启动之前就写入（流读启动后会先“全量读”到这两行）
INSERT INTO orders_none   VALUES (1, 'CREATED', 99.90), (2, 'CREATED', 15.00);
INSERT INTO orders_lookup VALUES (1, 'CREATED', 99.90), (2, 'CREATED', 15.00);
