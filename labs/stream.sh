#!/usr/bin/env bash
# 用法：./stream.sh <sql 文件> [秒数，默认 30]
# 流模式运行 SqlRunner：脚本最后一条 SELECT 以流方式持续读取指定秒数，逐行打印 RowKind。
set -euo pipefail
cd "$(dirname "$0")"
JAVA_PROPS="-Dmode=stream -Dstream.seconds=${2:-30}" exec ./run.sh "$1"
