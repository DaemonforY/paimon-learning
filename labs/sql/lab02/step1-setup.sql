-- 实验 2 / 步骤 1：准备一段已知的快照历史（与实验 1 相同的 4 次提交，换一张新表 orders_tt）
--   snapshot 1  APPEND   插入订单 1~5
--   snapshot 2  APPEND   订单 1 → PAID，新增订单 6
--   snapshot 3  APPEND   删除订单 3
--   snapshot 4  COMPACT  全量合并

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS orders_tt;

CREATE TABLE orders_tt (
  order_id BIGINT,
  user_id  BIGINT,
  amount   DECIMAL(10, 2),
  status   STRING,
  dt       STRING,
  PRIMARY KEY (dt, order_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '2'
);

INSERT INTO orders_tt VALUES
  (1, 101, 99.90,  'CREATED', '2026-09-24'),
  (2, 102, 15.00,  'CREATED', '2026-09-24'),
  (3, 103, 250.00, 'CREATED', '2026-09-24'),
  (4, 101, 8.80,   'CREATED', '2026-09-25'),
  (5, 104, 66.60,  'CREATED', '2026-09-25');

INSERT INTO orders_tt VALUES
  (1, 101, 99.90, 'PAID',    '2026-09-24'),
  (6, 105, 12.30, 'CREATED', '2026-09-25');

DELETE FROM orders_tt WHERE dt = '2026-09-24' AND order_id = 3;

CALL sys.compact(`table` => 'default.orders_tt');

SELECT snapshot_id, commit_kind, total_record_count, delta_record_count, commit_time
FROM `orders_tt$snapshots`;

-- @sh find ${warehouse_dir}/default.db/orders_tt -name '*.parquet' | sed 's|.*/orders_tt/||' | sort
