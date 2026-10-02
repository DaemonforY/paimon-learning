-- 实验 4 / 补充：一条数据的写入之旅，写两次（S2 第 2 讲素材，配合 jdb/s2-2-write-journey.txt）
-- 观察点：
--   1) 每行数据落到哪个 bucket？（hash % 桶数）
--   2) 第一次写：writer 从哪里恢复已有文件？sequence number 从几开始？
--   3) 第二次写：同一个 bucket 的 writer 恢复出上次的文件，sequence number 接着上次往后数
--   4) 刷盘时同 key 合并，生成的 DataFileMeta、CommitMessage 长什么样？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS journey;
CREATE TABLE journey (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '2');

-- 第一次写入：订单 1 写两次
INSERT INTO journey VALUES (1, 'A'), (2, 'B'), (1, 'C');
-- 第二次写入：订单 1 再更新一次，新增订单 3
INSERT INTO journey VALUES (1, 'D'), (3, 'E');

SELECT snapshot_id, commit_kind, total_record_count, delta_record_count FROM `journey$snapshots`;
SELECT bucket, level, record_count, min_sequence_number, max_sequence_number, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file
FROM `journey$files` ORDER BY bucket, min_sequence_number;
SELECT * FROM journey ORDER BY order_id;
