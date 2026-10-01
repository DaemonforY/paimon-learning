-- 实验 4（阶段 1）：用断点追踪一次写入
-- 一条 INSERT 写 3 行，其中订单 1 出现两次（A → C）。
-- 观察点：3 行分别落到哪个 bucket？同一个 key 的两行在写缓冲里怎么处理？最终 L0 文件有几行？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS trace_orders;
CREATE TABLE trace_orders (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '2');

INSERT INTO trace_orders VALUES (1, 'A'), (2, 'B'), (1, 'C');

SELECT snapshot_id, commit_kind, total_record_count FROM `trace_orders$snapshots`;
SELECT bucket, level, record_count, min_key, max_key, min_sequence_number, max_sequence_number
FROM `trace_orders$files`;
SELECT * FROM trace_orders;
