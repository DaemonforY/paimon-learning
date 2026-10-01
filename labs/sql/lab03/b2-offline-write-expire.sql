-- 实验 3 / B2：流读作业已停止。继续写入，然后激进地过期快照
-- 观察点：consumer 记录的 next_snapshot_id 是多少？过期时两张表的快照分别剩下哪些？
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- 流作业停下时记录的消费位点
SELECT * FROM `orders_c$consumers`;
-- @sh echo '--- consumer 文件 ---' && ls ${warehouse_dir}/default.db/orders_c/consumer/ && cat ${warehouse_dir}/default.db/orders_c/consumer/consumer-lab03 && echo

-- 第 2 轮：更新；第 3 轮：删除 + 新增（两张表完全相同）
INSERT INTO orders_c  VALUES (1, 'PAID');
INSERT INTO orders_nc VALUES (1, 'PAID');
DELETE FROM orders_c  WHERE order_id = 2;
DELETE FROM orders_nc WHERE order_id = 2;
INSERT INTO orders_c  VALUES (4, 'CREATED');
INSERT INTO orders_nc VALUES (4, 'CREATED');

SELECT snapshot_id, commit_kind, changelog_record_count FROM `orders_c$snapshots`;

-- 激进过期：只保留最近 1 个、不看时间
CALL sys.expire_snapshots(`table` => 'default.orders_c',  retain_min => 1, options => 'snapshot.time-retained=1ms');
CALL sys.expire_snapshots(`table` => 'default.orders_nc', retain_min => 1, options => 'snapshot.time-retained=1ms');

-- 对比：orders_c 被 consumer 保护，orders_nc 只剩最新快照
SELECT snapshot_id, commit_kind, changelog_record_count FROM `orders_c$snapshots`;
SELECT snapshot_id, commit_kind, changelog_record_count FROM `orders_nc$snapshots`;
