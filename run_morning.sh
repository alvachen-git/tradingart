#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update.log"
LOCK_FILE="/tmp/futures_price_update.lock"
PYTHON_BIN="${APP_DIR}/venv/bin/python"

cd "${APP_DIR}" || exit 1

if [ ! -x "${PYTHON_BIN}" ]; then
  if [ -x "${APP_DIR}/.venv311/bin/python" ]; then
    PYTHON_BIN="${APP_DIR}/.venv311/bin/python"
  else
    PYTHON_BIN="/usr/bin/python3"
  fi
fi

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "⏭️ 晨更任务跳过: $(date)" >> "${LOG_FILE}"
  echo "⏭️ 检测到期货更新锁占用: ${LOCK_FILE}" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 0
fi

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "⏰ 晨更任务开始: $(date)" >> "${LOG_FILE}"
echo "🔒 Lock: ${LOCK_FILE}" >> "${LOG_FILE}"
echo "🐍 Python: ${PYTHON_BIN}" >> "${LOG_FILE}"

"${PYTHON_BIN}" update_morning_price_daily.py >> "${LOG_FILE}" 2>&1
rc=$?
if [ "${rc}" -ne 0 ]; then
  echo "❌ 晨更任务失败(rc=${rc}): $(date)" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit "${rc}"
fi

echo "✅ 晨更任务结束: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
