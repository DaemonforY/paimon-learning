-- 实验 2 / 补充：按时间旅行，快照之间共享文件（S1 第 6 期素材）
-- 观察点：
--   1) 每个快照引用了哪些数据文件？新快照是复制了一份数据，还是复用旧文件？
--   2) 快照文件里记录了什么？base / delta manifest list 是什么关系？
--   3) 按时间戳查询时，命中的是哪个快照？早于第一个快照会怎样？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS tt_time;
CREATE TABLE tt_time (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1');

-- 三次提交，每次间隔 3 秒
INSERT INTO tt_time VALUES (1, 'CREATED'), (2, 'CREATED'), (3, 'CREATED');
-- @sh sleep 3
INSERT INTO tt_time VALUES (1, 'PAID');
-- @sh sleep 3
INSERT INTO tt_time VALUES (4, 'CREATED');

SELECT snapshot_id, commit_kind, total_record_count, delta_record_count, commit_time FROM `tt_time$snapshots`;

-- ① 每个快照引用的数据文件（$files 也支持时间旅行；文件名只取前 8 位）
SELECT 1 AS snapshot_id, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file, record_count FROM `tt_time$files` /*+ OPTIONS('scan.snapshot-id' = '1') */;
SELECT 2 AS snapshot_id, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file, record_count FROM `tt_time$files` /*+ OPTIONS('scan.snapshot-id' = '2') */;
SELECT 3 AS snapshot_id, REGEXP_EXTRACT(file_path, 'data-([0-9a-f]{8})', 1) AS file, record_count FROM `tt_time$files` /*+ OPTIONS('scan.snapshot-id' = '3') */;

-- @sh echo '=== 磁盘上的数据文件 ===' && find ${warehouse_dir}/default.db/tt_time -name '*.parquet' | sed 's|.*/tt_time/||' | sort

-- ② 快照文件本身：只是一个小 JSON，记录 manifest list 的文件名
-- @sh echo '=== snapshot-2 ===' && cat ${warehouse_dir}/default.db/tt_time/snapshot/snapshot-2 && echo && echo '=== snapshot-3 ===' && cat ${warehouse_dir}/default.db/tt_time/snapshot/snapshot-3 && echo
-- 每个快照的 manifest 文件（$manifests 也支持时间旅行）
SELECT 1 AS snapshot_id, REGEXP_EXTRACT(file_name, 'manifest-([0-9a-f]{8})', 1) AS manifest, num_added_files, num_deleted_files FROM `tt_time$manifests` /*+ OPTIONS('scan.snapshot-id' = '1') */;
SELECT 2 AS snapshot_id, REGEXP_EXTRACT(file_name, 'manifest-([0-9a-f]{8})', 1) AS manifest, num_added_files, num_deleted_files FROM `tt_time$manifests` /*+ OPTIONS('scan.snapshot-id' = '2') */;
SELECT 3 AS snapshot_id, REGEXP_EXTRACT(file_name, 'manifest-([0-9a-f]{8})', 1) AS manifest, num_added_files, num_deleted_files FROM `tt_time$manifests` /*+ OPTIONS('scan.snapshot-id' = '3') */;

-- ③ 按快照号时间旅行
SELECT * FROM tt_time /*+ OPTIONS('scan.snapshot-id' = '1') */ ORDER BY order_id;
SELECT * FROM tt_time /*+ OPTIONS('scan.snapshot-id' = '2') */ ORDER BY order_id;

-- ④ 按时间旅行：从快照文件里读出真实的提交时间（毫秒）
-- @set t1 grep -o '"timeMillis" : [0-9]*' ${warehouse_dir}/default.db/tt_time/snapshot/snapshot-1 | grep -o '[0-9]*$'
-- @set t2 grep -o '"timeMillis" : [0-9]*' ${warehouse_dir}/default.db/tt_time/snapshot/snapshot-2 | grep -o '[0-9]*$'
-- @set t3 grep -o '"timeMillis" : [0-9]*' ${warehouse_dir}/default.db/tt_time/snapshot/snapshot-3 | grep -o '[0-9]*$'
-- 快照 2 和快照 3 之间的某个时刻
-- @set mid echo $(( (${t2} + ${t3}) / 2 ))
-- 同一时刻的本地时间字符串，给 FOR SYSTEM_TIME AS OF 用（兼容 macOS 与 Linux 的 date）
-- @set mid_ts s=$(( ${mid} / 1000 )); ms=$(printf '%03d' $(( ${mid} % 1000 ))); (date -r $s '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -d @$s '+%Y-%m-%d %H:%M:%S') | tr -d '\n'; echo ".$ms"

-- 正好等于快照 2 的提交时间 → 快照 2
SELECT * FROM tt_time /*+ OPTIONS('scan.timestamp-millis' = '${t2}') */ ORDER BY order_id;
-- 快照 2 和 3 之间 → 取“提交时间 <= 该时刻”的最新快照，即快照 2
SELECT * FROM tt_time /*+ OPTIONS('scan.timestamp-millis' = '${mid}') */ ORDER BY order_id;
-- 同一时刻，用标准 SQL 语法
SELECT * FROM tt_time FOR SYSTEM_TIME AS OF TIMESTAMP '${mid_ts}' ORDER BY order_id;
-- 比快照 1 还早 1 毫秒 → 报错
-- @set before_t1 echo $(( ${t1} - 1 ))
-- @expect-error
SELECT * FROM tt_time /*+ OPTIONS('scan.timestamp-millis' = '${before_t1}') */;
