#!/usr/bin/env bash
# 重跑实验 1 的步骤 1~3，把输出和表目录快照留档到 logs/lab01-*（用于核对实验记录中的数字）
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs
DB=${WAREHOUSE:-warehouse}/default.db
rm -rf "$DB/orders" "$DB/access_log"

echo "[capture] 步骤 1：建表 + 首次写入"
./run.sh sql/lab01/step1-create-insert.sql > logs/lab01-step1.log 2>&1

{
  echo "=== orders 目录树（步骤 1 之后） ==="
  (cd "$DB/orders" && find . -type f | sort)
  echo; echo "=== access_log 目录树 ==="
  (cd "$DB/access_log" && find . -type f | sort)
  echo; echo "=== snapshot/LATEST ==="; cat "$DB/orders/snapshot/LATEST"; echo
  echo "=== snapshot/EARLIEST ==="; cat "$DB/orders/snapshot/EARLIEST"; echo
  echo "=== snapshot/snapshot-1 ==="; cat "$DB/orders/snapshot/snapshot-1"; echo
  echo "=== schema/schema-0 ==="; cat "$DB/orders/schema/schema-0"; echo
  echo "=== 文件大小 ==="; (cd "$DB/orders" && find . -type f -exec ls -l {} \; | awk '{print $5, $9}' | sort -k2)
} > logs/lab01-tree-after-step1.txt 2>&1

echo "[capture] 步骤 2：更新 / 删除 / append 重复写入"
./run.sh sql/lab01/step2-update-delete.sql > logs/lab01-step2.log 2>&1
echo "[capture] 步骤 3：系统表"
./run.sh sql/lab01/step3-system-tables.sql > logs/lab01-step3.log 2>&1
echo "[capture] 完成，素材在 logs/lab01-*"
