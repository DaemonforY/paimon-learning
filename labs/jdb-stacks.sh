#!/usr/bin/env bash
# 用法：PAIMON_VERSION=2.2-SNAPSHOT ./jdb-stacks.sh <sql 文件> <断点清单文件> [最多命中次数，默认 20]
# 不开 IDEA，用 JDK 自带的 jdb 在断点处打印真实调用栈（where）和变量，输出到 logs/stacks.log（可用 STACKS_OUT 指定）。
# 断点清单：每个断点一行 jdb 命令；紧跟其后、以空格缩进的行是“命中这个断点时额外执行的命令”，例如：
#   stop at org.apache.paimon.mergetree.MergeTreeWriter:166
#     print kv.key().getLong(0)
#   stop in org.apache.paimon.catalog.RenamingSnapshotCommit.commit
# 命中次数达到上限后清除全部断点，让程序跑完。
# 行号基于你编译安装的 Paimon 源码版本，务必和 PAIMON_VERSION 对应。
set -euo pipefail
cd "$(dirname "$0")"
source ./lab-common.sh

SQL=$1
BPS=$2
HITS=${3:-20}
OUT=${STACKS_OUT:-logs/stacks.log}
PAUSE=${JDB_PAUSE:-1}   # 每条 jdb 命令之间等待的秒数
mkdir -p logs "$(dirname "$OUT")"
# 和 run.sh 一样：未设置 JAVA_HOME 时在 macOS 上自动查找 JDK 17 / 11
if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
  JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || /usr/libexec/java_home -v 11 2>/dev/null || true)
fi
JDB="${JAVA_HOME:+$JAVA_HOME/bin/}jdb"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# 断点的“键”：stop at 用 “类名:行号”，stop in 用 “类名.方法名”（与 jdb 命中时打印的位置对应）
keyof() { echo "$1" | tr -c 'A-Za-z0-9.:_\n' '_'; }

# 解析断点清单：断点命令 → $WORK/stops；额外命令 → $WORK/cmd.<键>
key=""
: > "$WORK/stops"
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    ''|'#'*) continue ;;
    ' '*|$'\t'*)
      cmd=$(echo "$line" | sed 's/^[[:space:]]*//')
      [ -n "$key" ] && [ -n "$cmd" ] && echo "$cmd" >> "$WORK/cmd.$key" ;;
    *)
      echo "$line" >> "$WORK/stops"
      key=$(keyof "$(echo "$line" | awk '{print $3}')") ;;
  esac
done < "$BPS"

build_once
SKIP_BUILD=1 ./debug.sh "$SQL" > logs/stacks-program.log 2>&1 &
PROG=$!
# 等 JVM 在 5005 端口就绪
for _ in $(seq 1 60); do
  grep -q "Listening for transport" logs/stacks-program.log 2>/dev/null && break
  sleep 1
done

mkfifo "$WORK/in" "$WORK/events"
: > "$OUT"
"$JDB" -J-Duser.language=en -J-Duser.country=US -attach localhost:5005 < "$WORK/in" >> "$OUT" 2>&1 &
JDBPID=$!
exec 3> "$WORK/in"
sleep 2
cat "$WORK/stops" >&3
echo run >&3

tail -n +1 -f "$OUT" > "$WORK/events" &
TAILPID=$!
( sleep 1200; kill "$TAILPID" 2>/dev/null ) &
GUARD=$!

# 边读 jdb 输出边应答：命中断点 → 执行该断点的额外命令 + where → 继续
hits=0
while IFS= read -r line; do
  case "$line" in
    *"Breakpoint hit:"*)
      hits=$((hits + 1))
      method=$(echo "$line" | sed -E 's/.*", ([^ ]+)\(\), line=.*/\1/')
      lineno=$(echo "$line" | sed -E 's/.*line=([0-9,]+).*/\1/' | tr -d ',')
      cls=${method%.*}
      # jdb 的 print / where 是异步执行的：每条命令之间稍等，否则 cont 会抢在前面让线程继续跑
      for k in "$cls:$lineno" "$method"; do
        f="$WORK/cmd.$(keyof "$k")"
        if [ -f "$f" ]; then
          while IFS= read -r c; do echo "$c" >&3; sleep "$PAUSE"; done < "$f"
        fi
      done
      echo where >&3
      sleep "$PAUSE"
      [ "$hits" -ge "$HITS" ] && sed 's/^stop [a-z]* /clear /' "$WORK/stops" >&3
      echo cont >&3 ;;
    *"Stopping due to deferred breakpoint errors"*)
      echo cont >&3 ;;
    *"The application exited"*|*"application disconnected"*)
      break ;;
  esac
done < "$WORK/events"

kill "$TAILPID" "$GUARD" 2>/dev/null || true
exec 3>&-
wait "$JDBPID" 2>/dev/null || true
wait "$PROG" 2>/dev/null || true
echo "调用栈：${OUT}　程序输出：logs/stacks-program.log　命中断点 ${hits} 次"
