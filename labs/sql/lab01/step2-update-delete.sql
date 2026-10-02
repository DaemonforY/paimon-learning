-- 实验 1 / 步骤 2：更新与删除
-- 观察点：
--   1) 主键表写入相同主键 = 更新（upsert）；append 表写入重复行 = 两行都保留
--   2) DELETE 之后，旧的数据文件还在吗？多了什么文件？snapshot 变成了几个？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- 订单 1 改为已支付；新增订单 6
INSERT INTO orders VALUES
  (1, 101, 99.90, 'PAID',    '2026-09-24'),
  (6, 105, 12.30, 'CREATED', '2026-09-25');

-- @sh echo '=== DELETE 之前的数据文件 ===' && find ${warehouse_dir}/default.db/orders -name '*.parquet' | sed 's|.*/orders/||' | sort

-- 删除订单 3（批模式下 Paimon 主键表支持 DELETE）
DELETE FROM orders WHERE dt = '2026-09-24' AND order_id = 3;

-- @sh echo '=== DELETE 之后的数据文件 ===' && find ${warehouse_dir}/default.db/orders -name '*.parquet' | sed 's|.*/orders/||' | sort

SELECT * FROM orders ORDER BY order_id;

-- append 表：再写一条和之前一模一样的记录
INSERT INTO access_log VALUES (101, '/home', TIMESTAMP '2026-09-25 10:00:00');
SELECT * FROM access_log;
