-- 实验 2 / 补充：Tag 是什么、怎么用（S1 第 8 期素材）
-- 观察点：
--   1) Tag 文件和快照文件有什么区别？
--   2) 按 Tag 读、给 Tag 设保留时间、回滚到 Tag 分别发生了什么？
--   3) 自动创建 Tag：batch 模式和 process-time 模式各生成什么名字的 Tag？

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS tg;
CREATE TABLE tg (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1');

-- 快照 1
INSERT INTO tg VALUES (1, 'CREATED'), (2, 'CREATED');
-- 快照 2
INSERT INTO tg VALUES (1, 'PAID');
-- 快照 3
INSERT INTO tg VALUES (3, 'CREATED');

-- ① 手动创建 Tag：指定快照 / 不指定（默认最新快照）
CALL sys.create_tag(`table` => 'default.tg', tag => 'v1', snapshot_id => 1);
CALL sys.create_tag(`table` => 'default.tg', tag => 'release');
SELECT tag_name, snapshot_id, commit_time, record_count, create_time, time_retained FROM `tg$tags`;

-- Tag 文件就是快照 JSON 的拷贝
-- @sh ls ${warehouse_dir}/default.db/tg/tag/
-- @sh cmp ${warehouse_dir}/default.db/tg/tag/tag-v1 ${warehouse_dir}/default.db/tg/snapshot/snapshot-1 && echo 'tag-v1 与 snapshot-1 逐字节相同'
-- @sh wc -c ${warehouse_dir}/default.db/tg/tag/tag-v1 ${warehouse_dir}/default.db/tg/snapshot/snapshot-1 | sed 's|[^ ]*/tg/||'

-- ② 按 Tag 读
SELECT * FROM tg /*+ OPTIONS('scan.tag-name' = 'v1') */ ORDER BY order_id;

-- ③ 带保留时间的 Tag：多了两个字段
CALL sys.create_tag(`table` => 'default.tg', tag => 'tmp', snapshot_id => 2, time_retained => '2 s');
-- @sh diff ${warehouse_dir}/default.db/tg/snapshot/snapshot-2 ${warehouse_dir}/default.db/tg/tag/tag-tmp; echo "(diff 退出码 $?)"
SELECT tag_name, snapshot_id, time_retained FROM `tg$tags`;
-- 等它到期，再提交一次（Tag 过期在提交后的维护阶段执行）
-- @sh sleep 3
-- 快照 4
INSERT INTO tg VALUES (4, 'CREATED');
SELECT tag_name, snapshot_id, time_retained FROM `tg$tags`;

-- ④ 回滚到 Tag v1
-- @sh echo '=== 回滚前：snapshot 目录 ===' && ls ${warehouse_dir}/default.db/tg/snapshot/ && echo '=== 回滚前：数据文件 ===' && find ${warehouse_dir}/default.db/tg -name '*.parquet' | wc -l
CALL sys.rollback_to(`table` => 'default.tg', tag => 'v1');
SELECT snapshot_id, commit_kind, total_record_count FROM `tg$snapshots`;
SELECT tag_name, snapshot_id FROM `tg$tags`;
SELECT * FROM tg ORDER BY order_id;
-- @sh echo '=== 回滚后：snapshot 目录 ===' && ls ${warehouse_dir}/default.db/tg/snapshot/ && echo '=== 回滚后：数据文件 ===' && find ${warehouse_dir}/default.db/tg -name '*.parquet' | wc -l
-- 回滚后继续写入：新快照编号从哪里接着来？
INSERT INTO tg VALUES (5, 'CREATED');
SELECT snapshot_id, commit_kind, total_record_count FROM `tg$snapshots`;
-- 回滚只删了快照和 Tag 文件，被丢弃的快照写下的数据文件还留在磁盘上（孤儿文件）
-- @sh echo '=== 回滚并写入后：数据文件 / manifest 目录 ===' && find ${warehouse_dir}/default.db/tg -name '*.parquet' | wc -l && ls ${warehouse_dir}/default.db/tg/manifest | wc -l
-- remove_orphan_files 只删“早于 older_than 且不被任何快照 / Tag 引用”的文件；这里用当前时间。返回：删除的文件数、总字节数
-- @sh sleep 1
-- @set now_ts date '+%Y-%m-%d %H:%M:%S'
CALL sys.remove_orphan_files(`table` => 'default.tg', older_than => '${now_ts}', dry_run => true);
CALL sys.remove_orphan_files(`table` => 'default.tg', older_than => '${now_ts}');
-- @sh echo '=== 清理孤儿文件后：数据文件 / manifest 目录 ===' && find ${warehouse_dir}/default.db/tg -name '*.parquet' | wc -l && ls ${warehouse_dir}/default.db/tg/manifest | wc -l
SELECT * FROM tg ORDER BY order_id;

-- ⑤ 自动创建 Tag：batch 模式（批作业结束后为最新快照打 Tag）
DROP TABLE IF EXISTS tg_batch;
CREATE TABLE tg_batch (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1', 'tag.automatic-creation' = 'batch');
INSERT INTO tg_batch VALUES (1, 'CREATED');
SELECT tag_name, snapshot_id FROM `tg_batch$tags`;
-- 同一天再跑一次：同名 Tag 被替换为新快照
INSERT INTO tg_batch VALUES (2, 'CREATED');
SELECT tag_name, snapshot_id FROM `tg_batch$tags`;

-- ⑥ 自动创建 Tag：process-time 模式，按天
DROP TABLE IF EXISTS tg_daily;
CREATE TABLE tg_daily (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('bucket' = '1', 'tag.automatic-creation' = 'process-time', 'tag.creation-period' = 'daily');
INSERT INTO tg_daily VALUES (1, 'CREATED');
INSERT INTO tg_daily VALUES (2, 'CREATED');
SELECT tag_name, snapshot_id, commit_time FROM `tg_daily$tags`;
-- @sh date '+今天是 %Y-%m-%d'
