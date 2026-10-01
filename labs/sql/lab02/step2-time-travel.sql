-- 实验 2 / 步骤 2：时间旅行、Tag、增量读取
-- 观察点：
--   1) 读历史快照时，扫描用的是哪些文件？（日志里的 "Files size"）
--   2) Tag 本质上是什么文件？
--   3) 增量读取（incremental-between）读出来的是什么？RowKind 是什么？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- ① 按快照号时间旅行：订单 3 在快照 1、2 中还存在，快照 3 中已被删除
SELECT * FROM orders_tt /*+ OPTIONS('scan.snapshot-id' = '1') */ ORDER BY order_id;
SELECT * FROM orders_tt /*+ OPTIONS('scan.snapshot-id' = '2') */ ORDER BY order_id;
SELECT * FROM orders_tt /*+ OPTIONS('scan.snapshot-id' = '3') */ ORDER BY order_id;

-- ② 读一个不存在的快照
-- @expect-error
SELECT * FROM orders_tt /*+ OPTIONS('scan.snapshot-id' = '99') */;

-- ③ Tag：给快照 1 打一个标签（相当于给这个版本起名字并长期保留）
CALL sys.create_tag(`table` => 'default.orders_tt', tag => 'v1', snapshot_id => 1);
SELECT tag_name, snapshot_id, record_count, create_time FROM `orders_tt$tags`;
SELECT * FROM orders_tt /*+ OPTIONS('scan.tag-name' = 'v1') */ ORDER BY order_id;

-- @sh ls ${warehouse_dir}/default.db/orders_tt/tag/ && echo '--- tag/tag-v1 内容 ---' && cat ${warehouse_dir}/default.db/orders_tt/tag/tag-v1 && echo && echo '--- snapshot/snapshot-1 内容 ---' && cat ${warehouse_dir}/default.db/orders_tt/snapshot/snapshot-1

-- ④ 增量读取：快照 1 之后到快照 3（含）之间发生了什么变化
--    audit_log 系统表会把每行的 RowKind 显示在 rowkind 列
SELECT * FROM `orders_tt$audit_log` /*+ OPTIONS('incremental-between' = '1,3') */ ORDER BY order_id;
