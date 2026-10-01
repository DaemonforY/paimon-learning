-- 实验 3 / A：流读作业运行期间，对两张表做完全相同的更新和删除
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- 第 2 轮：更新
-- @sh echo "[writer $(date +%T)] 第 2 轮：订单 1 改为 PAID"
INSERT INTO orders_none   VALUES (1, 'PAID', 99.90);
INSERT INTO orders_lookup VALUES (1, 'PAID', 99.90);
-- @sh sleep 10

-- 第 3 轮：删除
-- @sh echo "[writer $(date +%T)] 第 3 轮：删除订单 2"
DELETE FROM orders_none   WHERE order_id = 2;
DELETE FROM orders_lookup WHERE order_id = 2;

-- 两张表的快照对比：注意 commit_kind 和 changelog_record_count
SELECT snapshot_id, commit_kind, delta_record_count, changelog_record_count FROM `orders_none$snapshots`;
SELECT snapshot_id, commit_kind, delta_record_count, changelog_record_count FROM `orders_lookup$snapshots`;
