#!/usr/bin/env bash
# 冒烟测试：在一个全新的 warehouse 里依次运行全部实验，并断言实验记录（notes/）中的关键结论。
# 用法：./run-all.sh            （默认 Paimon 2.0.0 + Flink 2.2.0）
#       FLINK_VERSION=1.20.1 ./run-all.sh   （对照 Flink 1.20 的结果）
#       PAIMON_VERSION=2.2-SNAPSHOT ./run-all.sh
# 全部通过时退出码为 0。耗时约 12~15 分钟（实验 3 含流式作业）。
set -uo pipefail
cd "$(dirname "$0")"
source ./lab-common.sh

export WAREHOUSE=${WAREHOUSE:-target/smoke-warehouse}
LOG=logs/smoke
rm -rf "$WAREHOUSE" "$LOG"
mkdir -p "$LOG"
PASS=0
FAIL=0

check() {   # check <描述> <日志文件> <grep 正则>
  if grep -qE -- "$3" "$2"; then PASS=$((PASS + 1)); echo "  ✅ $1"
  else FAIL=$((FAIL + 1)); echo "  ❌ $1   （在 $2 中未找到：$3）"; fi
}
check_not() {   # check_not <描述> <日志文件> <grep 正则>
  if grep -qE -- "$3" "$2"; then FAIL=$((FAIL + 1)); echo "  ❌ $1   （$2 中不应出现：$3）"
  else PASS=$((PASS + 1)); echo "  ✅ $1"; fi
}
run() {   # run <sql> <日志名>
  ./run.sh "$1" > "$LOG/$2.log" 2>&1 || echo "  ⚠️  $1 退出码非 0，见 $LOG/$2.log"
}

echo "Paimon 版本：${PAIMON_VERSION:-2.0.0}　Flink 版本：${FLINK_VERSION:-2.2.0}　warehouse：$WAREHOUSE"
build_once

echo "== 实验 1：建表、更新删除、系统表、合并 =="
run sql/lab01/step1-create-insert.sql lab01-1
run sql/lab01/step2-update-delete.sql lab01-2
run sql/lab01/step3-system-tables.sql lab01-3
run sql/lab01/step4-compact.sql       lab01-4
check "首次写入 3 个 CommitMessage（3 个有数据的 bucket）" $LOG/lab01-1.log "number of commit messages: 3"
check "批作业提交标识为 Long.MAX_VALUE"                    $LOG/lab01-1.log "identifier 9223372036854775807 and kind APPEND"
check "主键表 upsert：订单 1 变为 PAID"                     $LOG/lab01-2.log "\|\s+1 \|\s+101 \|\s+99.90 \|\s+PAID"
check "append 表保留重复行：4 行"                           $LOG/lab01-2.log "^4 rows in set"
check "快照 3 物理记录数 8（查询只有 5 行）"                $LOG/lab01-3.log "\|\s+3 \|\s+APPEND \|\s+9223372036854775807 \|\s+8 \|"
check "合并前全部文件在 level 0（6 个文件）"                $LOG/lab01-3.log "^6 rows in set"
check "合并时订单 3 的插入与删除抵消：输出 0 个文件"         $LOG/lab01-4.log "inputFiles=2, inputBytes=[0-9]+, outputFiles=0"
check "合并产生 COMPACT 快照，总记录数降为 5"               $LOG/lab01-4.log "\|\s+4 \|\s+COMPACT \|\s+5 \|\s+-3 \|"

echo "== 实验 2：时间旅行、Tag、增量读取、快照过期 =="
run sql/lab02/step1-setup.sql       lab02-1
run sql/lab02/step2-time-travel.sql lab02-2
run sql/lab02/step3-expire.sql      lab02-3
check "读不存在的快照报 out of range"                       $LOG/lab02-2.log "EXPECTED ERROR.*snapshotId 99 is out of available snapshotId range \[1, 4\]"
UUID_LINES=$(grep -c '"uuid"' $LOG/lab02-2.log || true)
UUID_DISTINCT=$(grep '"uuid"' $LOG/lab02-2.log | sort -u | wc -l | tr -d ' ')
[ "$UUID_LINES" = "2" ] && [ "$UUID_DISTINCT" = "1" ] && { PASS=$((PASS + 1)); echo "  ✅ Tag 文件与 snapshot-1 的 uuid 相同（Tag 是快照的拷贝）"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ Tag 与 snapshot-1 的 uuid 应相同：共 $UUID_LINES 行，去重后 $UUID_DISTINCT 个"; }
check "增量读取得到订单 3 的 -D"                            $LOG/lab02-2.log "\|\s+-D \|\s+3 \|"
check "retain_max < retain_min 报错"                       $LOG/lab02-3.log "EXPECTED ERROR.*retainMax \(2\) must not be less than retainMin \(10\)"
check "第一次过期只过期 1 个快照"                           $LOG/lab02-3.log "^1$"
check "第二次过期再过期 2 个快照"                           $LOG/lab02-3.log "^2$"
check "快照 1 过期后按 id 读不到"                           $LOG/lab02-3.log "EXPECTED ERROR.*snapshotId 1 is out of available snapshotId range \[4, 4\]"
check "Tag v1 仍可读出快照 1 的 5 行（含订单 3）"           $LOG/lab02-3.log "\|\s+3 \|\s+103 \|\s+250.00 \|\s+CREATED"
DEL_TAG_FILES=$(sed -n '/删除 Tag 后：数据文件/,/manifest 目录/p' $LOG/lab02-3.log | grep -c '\.parquet$' || true)
[ "$DEL_TAG_FILES" = "2" ] && { PASS=$((PASS + 1)); echo "  ✅ 删除 Tag 后只剩快照 4 的 2 个数据文件"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ 删除 Tag 后应剩 2 个数据文件，实际 $DEL_TAG_FILES"; }
run sql/lab02/time-travel-by-time.sql lab02-time
TT_FILES=$(sed -n '/=== 磁盘上的数据文件 ===/,/^$/p' $LOG/lab02-time.log | grep -c '\.parquet$' || true)
[ "$TT_FILES" = "3" ] && { PASS=$((PASS + 1)); echo "  ✅ 3 个快照共享文件：磁盘上只有 3 个数据文件"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ 磁盘上应有 3 个数据文件，实际 $TT_FILES"; }
S3_FILES=$(sed -n "/SELECT 3 AS snapshot_id, REGEXP_EXTRACT(file_path/,/ in set$/p" $LOG/lab02-time.log | tail -1)
[ "$S3_FILES" = "3 rows in set" ] && { PASS=$((PASS + 1)); echo "  ✅ 快照 3 引用 3 个数据文件"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ 快照 3 应引用 3 个数据文件，实际：$S3_FILES"; }
TT_SYS=$(sed -n '/FOR SYSTEM_TIME AS OF/,/rows in set/p' $LOG/lab02-time.log)
echo "$TT_SYS" | grep -qE '\|\s+1 \|\s+PAID \|' && echo "$TT_SYS" | grep -q '^3 rows in set' \
  && { PASS=$((PASS + 1)); echo "  ✅ FOR SYSTEM_TIME AS OF 快照 2、3 之间：读到快照 2"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ FOR SYSTEM_TIME AS OF 应读到快照 2（订单 1 = PAID，3 行）"; }
check "早于第一个快照：报 no snapshot earlier than"          $LOG/lab02-time.log "EXPECTED ERROR.*no snapshot earlier than or equal to timestamp"
run sql/lab02/tag-basics.sql      lab02-tag
run sql/lab02/tag-auto-period.sql lab02-tag-period
check "Tag 文件与快照文件逐字节相同"                         $LOG/lab02-tag.log "tag-v1 与 snapshot-1 逐字节相同"
check "设保留时间的 Tag 多出 tagTimeRetained 字段"           $LOG/lab02-tag.log '"tagTimeRetained" : 2'
TMP_LEFT=$(sed -n "/INSERT INTO tg VALUES (4, 'CREATED')/,/ in set$/p" $LOG/lab02-tag.log | grep -c '|\s*tmp |' || true)
[ "$TMP_LEFT" = "0" ] && { PASS=$((PASS + 1)); echo "  ✅ Tag tmp 到期后在下一次提交时被删除"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ Tag tmp 应已过期删除"; }
check "rollback_to 返回 (4, 1)"                              $LOG/lab02-tag.log "^4, 1$"
REL_LEFT=$(sed -n "/CALL sys.rollback_to/,/SELECT \* FROM tg ORDER BY order_id/p" $LOG/lab02-tag.log | grep -c '|\s*release |' || true)
[ "$REL_LEFT" = "0" ] && { PASS=$((PASS + 1)); echo "  ✅ 回滚后比目标新的 Tag release 被删除"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ 回滚后 Tag release 应被删除"; }
ORPHAN_LEFT=$(grep -A1 '^=== 清理孤儿文件后' $LOG/lab02-tag.log | sed -n 2p | tr -d ' ')
[ "$ORPHAN_LEFT" = "2" ] && { PASS=$((PASS + 1)); echo "  ✅ remove_orphan_files 后只剩 2 个数据文件"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ 清理孤儿文件后应剩 2 个数据文件，实际 $ORPHAN_LEFT"; }
check "batch 自动 Tag 同一天被替换为快照 2"                   $LOG/lab02-tag.log "\|\s+batch-write-[0-9-]+ \|\s+2 \|"
check "process-time daily 首次提交即生成日期 Tag"            $LOG/lab02-tag.log "\|\s+[0-9]{4}-[0-9]{2}-[0-9]{2} \|\s+1 \|"
check "1 分钟周期：跨边界后的提交生成上一分钟的 Tag → 快照 3" $LOG/lab02-tag-period.log "\|\s+[0-9]{12} \|\s+3 \|"
check "该 Tag 包含边界之后写入的订单 3"                       $LOG/lab02-tag-period.log "\|\s+3 \|\s+CREATED \|"

echo "== 实验 3A：changelog-producer none vs lookup（含流式作业，约 1.5 分钟） =="
./lab03-a.sh > $LOG/lab03-a.log 2>&1
cp logs/lab03-a-*.log $LOG/
check "none 表执行计划含 ChangelogNormalize"                $LOG/lab03-a-read-none.log "ChangelogNormalize\(key=\[order_id\]\)"
check_not "lookup 表执行计划不含 ChangelogNormalize"         $LOG/lab03-a-read-lookup.log "ChangelogNormalize"
for t in none lookup; do
  check "$t 表流读输出 -U/+U"                                $LOG/lab03-a-read-$t.log "\-U\[1, CREATED, 99.90\]"
  check "$t 表流读输出 -D"                                   $LOG/lab03-a-read-$t.log "\-D\[2, CREATED, 15.00\]"
done
check "lookup 表 COMPACT 快照带 changelog"                   $LOG/lab03-a-write.log "\|\s+[0-9]+ \|\s+COMPACT \|\s+[-0-9]+ \|\s+2 \|"

echo "== 实验 3B：consumer-id（约 2.5 分钟） =="
./lab03-b.sh > $LOG/lab03-b.log 2>&1
cp logs/lab03-b*.log $LOG/
check "consumer 记录 nextSnapshot = 3"                      $LOG/lab03-b2-write-expire.log '"nextSnapshot" : 3'
check "无 consumer 的表过期后只剩快照 8"                     $LOG/lab03-b2-write-expire.log "^7$"
check "同 consumer-id 重启：从断点续读到订单 1 的 +U"        $LOG/lab03-b3-resume.log "\+U\[1, PAID\]"
check "同 consumer-id 重启：读到订单 2 的 -D"               $LOG/lab03-b3-resume.log "\-D\[2, CREATED\]"
check_not "同 consumer-id 重启：不重复全量（无 +I[1, CREATED]）" $LOG/lab03-b3-resume.log "\+I\[1, CREATED\]"
check "对照组从过期位置启动：只读到 +I[4]"                  $LOG/lab03-b4-old-position.log "\+I\[4, CREATED\]"
check_not "对照组静默丢失订单 1 的更新"                      $LOG/lab03-b4-old-position.log "\+U\[1, PAID\]"
check_not "对照组静默丢失订单 2 的删除，且不报错"            $LOG/lab03-b4-old-position.log "\-D\[2, CREATED\]|Exception"

echo "== 实验 4：写入链路（非调试模式运行） =="
JAVA_PROPS="-Ddebug=true" run sql/lab04/trace-write.sql lab04
check "3 行写入合并为 2 条记录"                              $LOG/lab04.log "\|\s+1 \|\s+APPEND \|\s+2 \|"
check "订单 1 保留最后写入的 C"                              $LOG/lab04.log "\|\s+1 \|\s+C \|"
run sql/lab04/compare-write-buffer.sql lab04-compare
run sql/lab04/compare-buffer-full.sql  lab04-full
check "分两次写：2 个文件共 3 条记录（快照 2 total=3）"          $LOG/lab04-compare.log "\|\s+2 \|\s+APPEND \|\s+3 \|\s+1 \|"
check "aggregation sum：订单 1 = 15"                          $LOG/lab04-compare.log "\|\s+1 \|\s+15 \|"
check "changelog-producer=input：changelog 保留被合并的 A"     $LOG/lab04-compare.log "\|\s+\+I \|\s+1 \|\s+A \|"
check "可溢写：1 个文件 10 条"                                  $LOG/lab04-full.log "\|\s+spill \|\s+1 \|\s+10 \|"
check "关闭溢写：20 个文件 200 条"                              $LOG/lab04-full.log "\|\s+nospill \|\s+20 \|\s+200 \|"
run sql/lab04/universal-compaction.sql   lab04-uc
run sql/lab04/universal-compaction-2.sql lab04-uc2
check "UniversalCompaction：第 4 次单行提交合并成 L4（4 行）"     $LOG/lab04-uc.log "\|\s+4 \|\s+[0-9]+ \|\s+4 \|\s+2000 \|\s+2003 \|"
check "UniversalCompaction：第 7 次提交雪球吸收 L4（7 行）"       $LOG/lab04-uc.log "\|\s+4 \|\s+[0-9]+ \|\s+7 \|\s+2000 \|\s+2006 \|"
check "空间放大：uc_amp 全量合并到 L5（5 行）"                     $LOG/lab04-uc2.log "\|\s+5 \|\s+[0-9]+ \|\s+5 \|"
check "文件数兜底：uc_num 连底座全量合并到 L5（2775 行）"           $LOG/lab04-uc2.log "\|\s+5 \|\s+[0-9]+ \|\s+2775 \|"
run sql/lab04/compact-task.sql lab04-ct
check "默认阈值：C 之后的合并全部重写（19 → 13）"               $LOG/lab04-ct.log "inputFiles=19, .*outputFiles=13"
check "调小阈值：C 之后的合并只重写 A+C（18 → 12）"            $LOG/lab04-ct.log "inputFiles=18, .*outputFiles=12"
# ct 表最后一次查询 $files：重写的 A+C、升级的 B、E、L4 的 D → 4 个不同的文件名前缀；ct_def 全部重写 → 2 个
CT_PREFIX=$(awk '/Flink SQL> CREATE TABLE ct /{f=1} f' $LOG/lab04-ct.log | grep -E '^\|\s+[45] \|\s+[0-9a-f]{8}…' | tail -19 | awk -F'|' '{gsub(/ /,"",$3); split($3,a,"…"); print a[1]}' | sort -u | wc -l | tr -d ' ')
[ "$CT_PREFIX" = "4" ] && { PASS=$((PASS + 1)); echo "  ✅ 升级的文件保留原名（ct 最终 4 个文件名前缀）"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ ct 最终应有 4 个文件名前缀，实际 $CT_PREFIX"; }

echo "== 实验 5：缺依赖的 4 个报错 =="
./lab05.sh > $LOG/lab05.log 2>&1
cp logs/lab05-[0-9].log $LOG/ 2>/dev/null
check "缺 shaded hadoop：INSERT 报 hadoop Configuration"      $LOG/lab05.log "ClassNotFoundException: org.apache.hadoop.conf.Configuration"
check "缺 log4j-1.2-api：INSERT 报 org.apache.log4j.Level"    $LOG/lab05.log "ClassNotFoundException: org.apache.log4j.Level"
check "缺 connector-files：SELECT 报 SingleThreadMultiplexSourceReaderBase" $LOG/lab05.log "ClassNotFoundException: org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase"
check "只补 connector-base：SELECT 报 BulkFormat\$RecordIterator" $LOG/lab05.log "(ClassNotFoundException: org\.apache\.flink\.connector\.file\.src\.reader\.|NoClassDefFoundError: org/apache/flink/connector/file/src/reader/)BulkFormat"
check "依赖齐全（不含 connector-base）：读写成功"              $LOG/lab05.log "场景 5.*退出码 0"

echo "== 实验 6：Schema 演进 =="
run sql/lab06/schema-evolution.sql lab06
check "加列：remark 拿到字段 id 3"                           $LOG/lab06.log '"id" : 3,'
check "加列：旧数据的新列为 NULL"                            $LOG/lab06.log "\|\s+1 \|\s+CREATED \|\s+99.90 \|\s+<NULL> \|"
check "改名：order_status 读出旧数据"                        $LOG/lab06.log "order_status"
check "删列后加回同名列：新 amount 拿到字段 id 4"            $LOG/lab06.log '"highestFieldId" : 4'
check "删列后加回：旧数据不会回来（amount 全为 NULL）"       $LOG/lab06.log "\|\s+1 \|\s+CREATED \|\s+<NULL> \|\s+<NULL> \|"
check "旧文件 schema_id=0，新文件 schema_id=1"               $LOG/lab06.log "\|\s+1 \|\s+1 \|\s+\[3\] \|\s+\[3\] \|"

echo "== 实验 7：并发提交 =="
MAIN_CLASS=learning.paimon.CommitLab ./run.sh > $LOG/lab07.log 2>&1 || echo "  ⚠️  CommitLab 退出码非 0，见 $LOG/lab07.log"
check "并发 APPEND：60 次提交全部成功"                       $LOG/lab07.log "最新快照 id = 60"
check "并发 APPEND：两个作业各 30 个快照、0 失败"             $LOG/lab07.log "失败的作业数 = 0"
check "两个作业合并同一批文件：File deletion conflict"        $LOG/lab07.log "\[CONFLICT\] File deletion conflicts detected"
check "重启后重复提交同一 identifier 被过滤"                  $LOG/lab07.log "filterAndCommit\(identifier 5\) 实际提交了 0 个"

echo "== 实验 8：读路径 =="
run sql/lab08/read-path.sql lab08
check "合并读结果正确：r_overlap 最新值 b 100 行、a 50 行"     $LOG/lab08.log "\|\s+a \|\s+50 \|"
check "删除在读时生效：r_delete 剩 99 行"                     $LOG/lab08.log "\|\s+a \|\s+99 \|"
check "全量合并后 COUNT(*) 下推为元数据计数"                   $LOG/lab08.log "r_compact, (project=\[k\], )?aggregates=\[grouping=\[\], aggFunctions=\[Count1AggFunction\(\)\]\]"
check "重叠的表 COUNT(*) 不能下推"                             $LOG/lab08.log "r_overlap(, project=\[k\])?\]\], fields=\[k"
check "value 过滤不会读到旧版本（Empty set）"                  $LOG/lab08.log "^Empty set"

echo "== 实验 9：删除向量 =="
run sql/lab09/deletion-vectors.sql lab09
check "删除向量表结果与默认表一致（a 89 行）"                  $LOG/lab09.log "\|\s+a \|\s+89 \|"
check "删除一行后 DV 索引文件 32 字节"                         $LOG/lab09.log "\|\s+DELETION_VECTORS \|\s+1 \|\s+32 \|"
check "DV 表 COUNT(*) 下推"                                    $LOG/lab09.log "dv_on, (project=\[k\], )?aggregates="
check "DV 表 L0 在合并前不可见（0 行）"                         $LOG/lab09.log "^\|\s+0 \|$"
check "merge-on-read / 合并后可见（2 行）"                     $LOG/lab09.log "^\|\s+2 \|$"

echo "== 截图安全：输出中不出现本机绝对路径 =="
LEAKS=$(grep -lE "/Users/|/home/[a-z]" $LOG/lab0*.log 2>/dev/null | xargs -n1 basename 2>/dev/null | paste -sd, -)
[ -z "$LEAKS" ] && { PASS=$((PASS + 1)); echo "  ✅ 所有实验日志中都没有本机绝对路径"; } \
  || { FAIL=$((FAIL + 1)); echo "  ❌ 以下日志含本机绝对路径：$LEAKS"; }

echo
echo "通过 $PASS 项，失败 $FAIL 项。日志在 $LOG/"
[ "$FAIL" = "0" ]
