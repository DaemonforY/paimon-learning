#!/usr/bin/env bash
# 实验 3 / B：consumer-id。日志在 logs/lab03-b-*.log
set -euo pipefail
cd "$(dirname "$0")"
source ./lab-common.sh
mkdir -p logs
build_once

./run.sh sql/lab03/b0-setup.sql > logs/lab03-b0-setup.log 2>&1
echo "[lab03-b] B1：带 consumer-id 流读 orders_c（30s，期间会做若干次 checkpoint）"
./stream.sh sql/lab03/b1-read-consumer.sql 30 > logs/lab03-b1-read.log 2>&1

echo "[lab03-b] B2：流作业已停止，继续写入并过期快照"
./run.sh sql/lab03/b2-offline-write-expire.sql > logs/lab03-b2-write-expire.log 2>&1

echo "[lab03-b] B3：同一个 consumer-id 重启（没有 Flink 状态），30s"
./stream.sh sql/lab03/b1-read-consumer.sql 30 > logs/lab03-b3-resume.log 2>&1

echo "[lab03-b] B4：对照组 orders_nc 从旧位置流读，30s"
./stream.sh sql/lab03/b4-read-old-position.sql 30 > logs/lab03-b4-old-position.log 2>&1
echo "[lab03-b] 完成"
