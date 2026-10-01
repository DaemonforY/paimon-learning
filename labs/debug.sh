#!/usr/bin/env bash
# 用法：./debug.sh <sql 文件>
# 以调试模式启动 SqlRunner：JVM 在 5005 端口等待调试器连接（suspend=y），连上之前不会执行任何代码。
# 在 IDEA 的 paimon 源码工程里用 “Remote JVM Debug”（localhost:5005）连接即可命中 Paimon 源码断点。
set -euo pipefail
cd "$(dirname "$0")"
echo "[debug] JVM 将在 localhost:5005 等待 IDEA 连接..."
JAVA_PROPS="-Ddebug=true -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005" exec ./run.sh "$1"
