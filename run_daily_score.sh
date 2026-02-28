#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update_stock_score.log"
LOCK_FILE="/tmp/run_daily_score.lock"
PYTHON_BIN="${APP_DIR}/.venv311/bin/python"

cd "${APP_DIR}" || exit 1

# 防重入：评分任务同一时间只跑一个实例
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "⏭️ run_daily_score 已在运行，跳过本次: $(date)" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 0
fi

if [ ! -x "${PYTHON_BIN}" ]; then
  PYTHON_BIN="/usr/bin/python3"
fi

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "⏰ 评分任务开始: $(date)" >> "${LOG_FILE}"
echo "🐍 Python: ${PYTHON_BIN}" >> "${LOG_FILE}"

/usr/bin/time -v "${PYTHON_BIN}" update_stock_score.py >> "${LOG_FILE}" 2>&1
RC=$?

if [ ${RC} -ne 0 ]; then
  echo "❌ update_stock_score.py 执行失败 (rc=${RC}): $(date)" >> "${LOG_FILE}"
else
  echo "✅ update_stock_score.py 执行成功: $(date)" >> "${LOG_FILE}"
fi

echo "🏁 评分任务结束 (rc=${RC}): $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"

exit ${RC}
