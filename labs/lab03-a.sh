#!/usr/bin/env bash
# 实验 3 / A：两个流读作业在后台运行，同时写入作业做三轮变更。日志在 logs/lab03-a-*.log
set -euo pipefail
cd "$(dirname "$0")"
source ./lab-common.sh
mkdir -p logs
build_once

./run.sh sql/lab03/a0-setup.sql > logs/lab03-a-setup.log 2>&1
echo "[lab03-a] 表已创建，启动两个流读作业（各 75s）..."
./stream.sh sql/lab03/a-read-none.sql 75   > logs/lab03-a-read-none.log 2>&1 &
./stream.sh sql/lab03/a-read-lookup.sql 75 > logs/lab03-a-read-lookup.log 2>&1 &
wait_ready logs/lab03-a-read-none.log
wait_ready logs/lab03-a-read-lookup.log
echo "[lab03-a] 流读已就绪，开始写入..."
./run.sh sql/lab03/a-write.sql > logs/lab03-a-write.log 2>&1
echo "[lab03-a] 写入完成，等待流读作业结束..."
wait
echo "[lab03-a] 完成"
