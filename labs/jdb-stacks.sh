#!/usr/bin/env bash
# 用法：PAIMON_VERSION=2.2-SNAPSHOT ./jdb-stacks.sh <sql 文件> <断点清单文件> [最多命中次数，默认 20]
# 不开 IDEA，用 JDK 自带的 jdb 在断点处打印真实调用栈（where），输出到 logs/stacks.log（可用 STACKS_OUT 指定）。
# 断点清单每行一条 jdb 命令，例如：
#   stop in org.apache.paimon.mergetree.MergeTreeWriter.write
#   stop at org.apache.paimon.operation.MergeFileSplitRead:253
# 行号基于你编译安装的 Paimon 源码版本，务必和 PAIMON_VERSION 对应。
set -euo pipefail
cd "$(dirname "$0")"
source ./lab-common.sh

SQL=$1
BPS=$2
HITS=${3:-20}
OUT=${STACKS_OUT:-logs/stacks.log}
mkdir -p logs
# 和 run.sh 一样：未设置 JAVA_HOME 时在 macOS 上自动查找 JDK 17 / 11
if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
  JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || /usr/libexec/java_home -v 11 2>/dev/null || true)
fi
JDB="${JAVA_HOME:+$JAVA_HOME/bin/}jdb"

build_once
SKIP_BUILD=1 ./debug.sh "$SQL" > logs/stacks-program.log 2>&1 &
PROG=$!
# 等 JVM 在 5005 端口就绪
for _ in $(seq 1 60); do
  grep -q "Listening for transport" logs/stacks-program.log 2>/dev/null && break
  sleep 1
done

{
  sleep 2
  grep -v '^\s*#' "$BPS" | grep -v '^\s*$'
  echo run
  # 每隔几秒处理一次命中：打印调用栈后继续
  for _ in $(seq 1 "$HITS"); do
    sleep 6
    echo where
    echo cont
  done
  echo exit
} | "$JDB" -J-Duser.language=en -J-Duser.country=US -attach localhost:5005 > "$OUT" 2>&1 || true

wait "$PROG" 2>/dev/null || true
echo "调用栈：${OUT}　程序输出：logs/stacks-program.log"
