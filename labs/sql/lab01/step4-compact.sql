-- 实验 1 / 步骤 4：手动触发全量合并
-- 观察点：合并后文件数、level、record_count 如何变化？被删除的订单 3 还占空间吗？快照多了哪种 commit_kind？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- @sh echo '=== 合并之前的数据文件 ===' && find ${warehouse_dir}/default.db/orders -name '*.parquet' | sed 's|.*/orders/||' | sort

CALL sys.compact(`table` => 'default.orders');

-- @sh echo '=== 合并之后的数据文件（磁盘上） ===' && find ${warehouse_dir}/default.db/orders -name '*.parquet' | sed 's|.*/orders/||' | sort

SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `orders$snapshots`;

SELECT `partition`, bucket, level, record_count, min_key, max_key,
       min_sequence_number, max_sequence_number, file_source,
       REGEXP_EXTRACT(file_path, '([^/]+)$', 1) AS file_name
FROM `orders$files`
ORDER BY `partition`, bucket, level;

SELECT * FROM orders ORDER BY order_id;
