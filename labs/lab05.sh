#!/usr/bin/env bash
# 实验 5：嵌入式运行 Paimon 时缺依赖的 4 个典型报错。
# 同一段 SQL（建表 → 写入 → 读取），每个场景改动一处 classpath，观察报错出现在哪一步、缺的是哪个类。
# 每个场景的完整日志在 logs/lab05-*.log，摘要打印到终端。
set -uo pipefail
cd "$(dirname "$0")"
source ./lab-common.sh
mkdir -p logs
build_once
# 每个场景都会清空 warehouse，所以固定用独立目录，不复用调用方（如 run-all.sh）的 WAREHOUSE
export WAREHOUSE=target/lab05-warehouse

FLINK_VERSION=1.20.1
MVN_SETTINGS=()
[ "${MAVEN_MIRROR:-}" = "aliyun" ] && MVN_SETTINGS=(-s maven-settings-aliyun.xml)

scenario() {   # scenario <编号> <说明> <排除的 jar 正则> [额外 jar]
  local id=$1 desc=$2 exclude=$3 extra=${4:-} log=logs/lab05-$1.log
  rm -rf "$WAREHOUSE"
  EXCLUDE_JARS="$exclude" EXTRA_JARS="$extra" ./run.sh sql/lab05/write-read.sql > "$log" 2>&1
  local code=$?
  echo "=== 场景 ${id}：${desc}（退出码 ${code}）==="
  if grep -q '^2 rows in set' "$log"; then
    echo "  结果：写入、读取全部成功（2 rows in set）"
  else
    echo "  失败语句：$(grep '^Flink SQL> ' "$log" | tail -1 | cut -c12- | cut -c1-60)"
    # 根因：最后一个 Caused by；没有则取第一个异常行
    local cause
    cause=$(grep -E '^Caused by: ' "$log" | tail -1)
    [ -z "$cause" ] && cause=$(grep -m1 -E '^[a-zA-Z.]+(Exception|Error)' "$log")
    echo "  根因：${cause}"
  fi
  echo
}

scenario 1 "缺 flink-shaded-hadoop-2-uber"                 "flink-shaded-hadoop-2-uber"
scenario 2 "缺 log4j-1.2-api"                              "log4j-1\.2-api"
scenario 3 "缺 flink-connector-files"                      "flink-connector-files"

# 场景 4：模拟“看到场景 3 的报错后只补了 flink-connector-base”
mvn -q ${MVN_SETTINGS[@]+"${MVN_SETTINGS[@]}"} dependency:get -Dartifact=org.apache.flink:flink-connector-base:$FLINK_VERSION > /dev/null 2>&1
BASE_JAR=$(mvn -q ${MVN_SETTINGS[@]+"${MVN_SETTINGS[@]}"} help:evaluate -Dexpression=settings.localRepository -DforceStdout 2>/dev/null)/org/apache/flink/flink-connector-base/$FLINK_VERSION/flink-connector-base-$FLINK_VERSION.jar
if [ -f "$BASE_JAR" ]; then
  scenario 4 "缺 flink-connector-files，但补了 flink-connector-base" "flink-connector-files" "$BASE_JAR"
else
  echo "=== 场景 4：跳过（未能下载 flink-connector-base，可设置 MAVEN_MIRROR=aliyun 重试）==="; echo
fi

scenario 5 "依赖齐全（只需 flink-connector-files，它已包含 connector-base 的类）" "__none__"
