#!/bin/bash

set -u

APP_DIR="${APP_DIR:-/root/finance_app/future-app}"
LOG_FILE="${LOG_FILE:-${APP_DIR}/update.log}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
FAILED_STEPS=0

cd "${APP_DIR}" || exit 1

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

run_step 1 2 "更新中午期货数据" "update_noon_price_daily.py"
run_step 2 2 "更新中午ETF期权标的实时价" "update_noon_etf_underlying.py"

if [ ${FAILED_STEPS} -gt 0 ]; then
  echo "⚠️ 本次任务失败步骤数: ${FAILED_STEPS}" >> "${LOG_FILE}"
  echo "❌ 任务结束: $(date)" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

echo "✅ 本次任务全部步骤成功" >> "${LOG_FILE}"
echo "✅ 任务结束: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
