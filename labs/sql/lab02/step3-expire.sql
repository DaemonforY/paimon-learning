-- 实验 2 / 步骤 3：快照过期（三道保险）与 Tag 保护
--   snapshot.num-retained.min  默认 10     至少保留最近 N 个快照
--   snapshot.num-retained.max  默认 无上限  最多保留 N 个（超出的不看时间直接过期）
--   snapshot.time-retained     默认 1 小时  介于 min 和 max 之间的快照，要“足够老”才过期
-- 观察点：每次过期了几个快照？哪些数据文件被物理删除了？Tag 引用的文件为什么还在？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- ⓪ 先弄清每个快照引用了哪些数据文件（$files 也支持时间旅行），后面好判断删得对不对
SELECT 1 AS snapshot_id, `partition`, bucket, level, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file
FROM `orders_tt$files` /*+ OPTIONS('scan.snapshot-id' = '1') */
UNION ALL
SELECT 2, `partition`, bucket, level, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1)
FROM `orders_tt$files` /*+ OPTIONS('scan.snapshot-id' = '2') */
UNION ALL
SELECT 3, `partition`, bucket, level, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1)
FROM `orders_tt$files` /*+ OPTIONS('scan.snapshot-id' = '3') */
UNION ALL
SELECT 4, `partition`, bucket, level, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1)
FROM `orders_tt$files` /*+ OPTIONS('scan.snapshot-id' = '4') */;

-- @sh echo '=== 过期前：snapshot 目录 ===' && ls ${warehouse_dir}/default.db/orders_tt/snapshot/ && echo '=== 过期前：数据文件 ===' && find ${warehouse_dir}/default.db/orders_tt -name '*.parquet' | sed 's|.*/orders_tt/||' | sort

-- ① 只给 retain_max=2：retain_min 仍是默认 10，违反 max >= min，直接报错
-- @expect-error
CALL sys.expire_snapshots(`table` => 'default.orders_tt', retain_max => 2);

-- ② retain_max=3, retain_min=1：超出 max 的快照 1 被强制过期；
--    快照 2、3 在 max 之内，但它们还不到 1 小时（time-retained），受保护
CALL sys.expire_snapshots(`table` => 'default.orders_tt', retain_max => 3, retain_min => 1);
SELECT snapshot_id, commit_kind FROM `orders_tt$snapshots`;
-- @sh echo '=== 第一次过期后：snapshot 目录 ===' && ls ${warehouse_dir}/default.db/orders_tt/snapshot/ && echo '=== 数据文件 ===' && find ${warehouse_dir}/default.db/orders_tt -name '*.parquet' | sed 's|.*/orders_tt/||' | sort

-- ③ 再临时把 time-retained 调成 1ms：只保留最新 1 个快照
CALL sys.expire_snapshots(`table` => 'default.orders_tt', retain_min => 1, options => 'snapshot.time-retained=1ms');
SELECT snapshot_id, commit_kind FROM `orders_tt$snapshots`;
-- @sh echo '=== 第二次过期后：snapshot 目录 ===' && ls ${warehouse_dir}/default.db/orders_tt/snapshot/ && echo '=== 数据文件 ===' && find ${warehouse_dir}/default.db/orders_tt -name '*.parquet' | sed 's|.*/orders_tt/||' | sort

-- ④ 快照 1 已经过期，按快照号读不到了；但 Tag v1 指向的还是快照 1 的内容，依然能读
-- @expect-error
SELECT * FROM orders_tt /*+ OPTIONS('scan.snapshot-id' = '1') */;
SELECT * FROM orders_tt /*+ OPTIONS('scan.tag-name' = 'v1') */ ORDER BY order_id;

-- ⑤ 删除 Tag：它独占的数据文件随之被删除
CALL sys.delete_tag(`table` => 'default.orders_tt', tag => 'v1');
-- @sh echo '=== 删除 Tag 后：数据文件 ===' && find ${warehouse_dir}/default.db/orders_tt -name '*.parquet' | sed 's|.*/orders_tt/||' | sort && echo '=== manifest 目录 ===' && ls ${warehouse_dir}/default.db/orders_tt/manifest/

SELECT * FROM orders_tt ORDER BY order_id;
