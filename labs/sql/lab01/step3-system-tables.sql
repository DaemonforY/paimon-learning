-- 实验 1 / 步骤 3：用系统表观察元数据
-- 观察点：每个快照的 commit_kind / 记录数；每个数据文件的 level、key 范围、sequence 范围

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${warehouse}'
);
USE CATALOG paimon;

-- 快照历史
SELECT snapshot_id, commit_kind, commit_identifier, total_record_count, delta_record_count,
       base_manifest_list, delta_manifest_list
FROM `orders$snapshots`;

-- 当前快照下的所有数据文件（注意 level 和 sequence number）
SELECT `partition`, bucket, level, record_count, min_key, max_key,
       min_sequence_number, max_sequence_number, file_source,
       REGEXP_EXTRACT(file_path, '([^/]+)$', 1) AS file_name
FROM `orders$files`
ORDER BY `partition`, bucket, level, min_sequence_number;

-- DELETE 到底写了什么：增量读取快照 3（删除订单 3 的那次提交），rowkind 列显示 -D
SELECT * FROM `orders$audit_log` /*+ OPTIONS('incremental-between' = '2,3') */;

-- manifest 文件：每个 manifest 新增/删除了几个文件
SELECT file_name, num_added_files, num_deleted_files FROM `orders$manifests`;

-- 表结构与参数
SELECT * FROM `orders$schemas`;
SELECT * FROM `orders$options`;

-- append 表的文件
SELECT bucket, level, record_count, REGEXP_EXTRACT(file_path, '([^/]+)$', 1) AS file_name
FROM `access_log$files`;
