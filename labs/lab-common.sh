#!/usr/bin/env bash
# 实验编排脚本的公共函数（被 lab03-*.sh 引用）

# 先统一编译一次，之后并发启动的作业都 SKIP_BUILD
build_once() {
  ./run.sh /dev/null > /dev/null 2>&1 || true
  export SKIP_BUILD=1
}

# 等待日志中出现 [READY] 标记（流作业已提交），再多等几秒让 source 完成首次规划
wait_ready() {
  local log=$1
  for _ in $(seq 1 120); do
    if grep -q "\[READY\]" "$log" 2>/dev/null; then
      sleep 8
      return 0
    fi
    sleep 1
  done
  echo "等待 $log 就绪超时" >&2
  return 1
}
