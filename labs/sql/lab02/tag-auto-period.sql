-- 实验 2 / 补充：process-time 自动 Tag 什么时候创建、指向哪个快照（S1 第 8 期素材，约 2 分钟）
-- 'tag.creation-period' = 'daily' 要等一天才能看到第二个 Tag，这里用 1 分钟的周期代替，规则相同：
--   1) 第一次提交：立刻为“上一个周期”创建 Tag，指向这次提交的快照
--   2) 同一周期内的后续提交：不创建
--   3) 跨过周期边界后的第一次提交：为刚结束的那个周期创建 Tag，指向这次提交的快照

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

DROP TABLE IF EXISTS tg_min;
CREATE TABLE tg_min (
  order_id BIGINT,
  status   STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'tag.automatic-creation' = 'process-time',
  'tag.creation-period-duration' = '1 min'
);

-- 等到下一分钟的第 1 秒，保证下面两次提交落在同一分钟
-- @sh sleep $(( 61 - 10#$(date +%S) )); date '+现在是 %H:%M:%S'
INSERT INTO tg_min VALUES (1, 'CREATED');
INSERT INTO tg_min VALUES (2, 'CREATED');
SELECT snapshot_id, commit_time FROM `tg_min$snapshots`;
SELECT tag_name, snapshot_id FROM `tg_min$tags`;

-- 跨过下一个分钟边界后再提交一次
-- @sh sleep $(( 61 - 10#$(date +%S) )); date '+现在是 %H:%M:%S'
INSERT INTO tg_min VALUES (3, 'CREATED');
SELECT snapshot_id, commit_time FROM `tg_min$snapshots`;
SELECT tag_name, snapshot_id FROM `tg_min$tags`;
-- 刚结束那一分钟的 Tag 里有订单 3 吗？
-- @set last_tag ls ${warehouse_dir}/default.db/tg_min/tag/ | sort | tail -1 | sed 's/^tag-//'
SELECT * FROM tg_min /*+ OPTIONS('scan.tag-name' = '${last_tag}') */ ORDER BY order_id;
