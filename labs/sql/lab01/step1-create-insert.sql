-- 实验 1 / 步骤 1：建表并写入第一批数据
-- 观察点：写入后表目录里出现了哪些文件？snapshot-1 的内容是什么？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- 主键表：按 dt 分区，每个分区 2 个 bucket
CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT,
  user_id  BIGINT,
  amount   DECIMAL(10, 2),
  status   STRING,
  dt       STRING,
  PRIMARY KEY (dt, order_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '2'
);

-- append 表：没有主键
CREATE TABLE IF NOT EXISTS access_log (
  user_id BIGINT,
  url     STRING,
  ts      TIMESTAMP(3)
);

INSERT INTO orders VALUES
  (1, 101, 99.90,  'CREATED', '2026-09-24'),
  (2, 102, 15.00,  'CREATED', '2026-09-24'),
  (3, 103, 250.00, 'CREATED', '2026-09-24'),
  (4, 101, 8.80,   'CREATED', '2026-09-25'),
  (5, 104, 66.60,  'CREATED', '2026-09-25');

INSERT INTO access_log VALUES
  (101, '/home',   TIMESTAMP '2026-09-25 10:00:00'),
  (102, '/item/1', TIMESTAMP '2026-09-25 10:00:05'),
  (101, '/cart',   TIMESTAMP '2026-09-25 10:01:00');

SELECT * FROM orders ORDER BY order_id;
SELECT * FROM access_log;
