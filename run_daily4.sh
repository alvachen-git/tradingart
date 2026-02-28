#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update.log"
LOCK_FILE="/tmp/run_daily4.lock"
PYTHON_BIN="${APP_DIR}/.venv311/bin/python"
FAILED_STEPS=0

# 1. 进入项目目录
cd "${APP_DIR}" || exit 1

# 2. 防重入：同一时间只允许一个 run_daily4 在跑
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "⏭️ run_daily4 已在运行，跳过本次: $(date)" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 0
fi

# 3. Python 优先使用项目虚拟环境，找不到再回退系统 Python
if [ ! -x "${PYTHON_BIN}" ]; then
  PYTHON_BIN="/usr/bin/python3"
fi

run_step() {
  local idx="$1"
  local total="$2"
  local title="$3"
  local script="$4"
  shift 4

  echo ">>> [${idx}/${total}] 开始${title}..." >> "${LOG_FILE}"
  /usr/bin/time -v "${PYTHON_BIN}" "${script}" "$@" >> "${LOG_FILE}" 2>&1
  local rc=$?
  echo "<<< [${idx}/${total}] 结束${title} (rc=${rc})" >> "${LOG_FILE}"
  echo "" >> "${LOG_FILE}"

  if [ ${rc} -ne 0 ]; then
    FAILED_STEPS=$((FAILED_STEPS + 1))
    echo "❌ ${script} 执行失败，但继续执行后续步骤: $(date)" >> "${LOG_FILE}"
  fi
}

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "⏰ 任务开始: $(date)" >> "${LOG_FILE}"
echo "🐍 Python: ${PYTHON_BIN}" >> "${LOG_FILE}"

run_step 1 2 "计算商品IV数据" "update_commodity_iv_daily_old.py" --auto-latest-common-date
run_step 2 2 "更新港股数据" "update_hk_daily.py"

if [ ${FAILED_STEPS} -gt 0 ]; then
  echo "⚠️ 本次任务失败步骤数: ${FAILED_STEPS}" >> "${LOG_FILE}"
else
  echo "✅ 本次任务全部步骤成功" >> "${LOG_FILE}"
fi

echo "✅ 任务结束: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
