#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update.log"
PYTHON_BIN="${APP_DIR}/venv/bin/python"
FAILED_STEPS=0

cd "${APP_DIR}" || exit 1

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "❌ 任务开始: $(date)" >> "${LOG_FILE}"
  echo "❌ 未找到虚拟环境解释器: ${PYTHON_BIN}" >> "${LOG_FILE}"
  echo "❌ 任务终止，请先创建/修复 venv 环境" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

run_step() {
  local idx="$1"
  local total="$2"
  local title="$3"
  local script="$4"

  echo ">>> [${idx}/${total}] 开始${title}..." >> "${LOG_FILE}"
  "${PYTHON_BIN}" "${script}" >> "${LOG_FILE}" 2>&1
  local rc=$?
  echo "<<< [${idx}/${total}] 结束${title} (rc=${rc})" >> "${LOG_FILE}"
  echo "" >> "${LOG_FILE}"

  if [ ${rc} -ne 0 ]; then
    FAILED_STEPS=$((FAILED_STEPS + 1))
  fi
}

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "⏰ 任务开始: $(date)" >> "${LOG_FILE}"
echo "🐍 Python: ${PYTHON_BIN}" >> "${LOG_FILE}"

run_step 1 6 "更新期货席位数据" "update_open_oneday.py"
run_step 2 6 "更新股票资金流数据" "update_stock_moneyflow_daily.py"
run_step 3 6 "更新美股价格数据" "update_stock_tiingo.py"
run_step 4 6 "更新债券收益数据" "update_bond_data.py"
run_step 5 6 "更新热搜数据" "trend_monitor.py"
run_step 6 6 "更新宏观数据" "update_micro_daily.py"

if [ ${FAILED_STEPS} -gt 0 ]; then
  echo "⚠️ 本次任务失败步骤数: ${FAILED_STEPS}" >> "${LOG_FILE}"
else
  echo "✅ 本次任务全部步骤成功" >> "${LOG_FILE}"
fi

echo "✅ 任务结束: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
