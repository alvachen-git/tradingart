#!/bin/bash

APP_DIR="/root/finance_app/future-app"
PYTHON_BIN="${APP_DIR}/venv/bin/python"
LOG_FILE="${APP_DIR}/update.log"

cd "${APP_DIR}" || exit 1

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "[START] evening report job: $(date)" >> "${LOG_FILE}"

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "[ERR] python venv not found: ${PYTHON_BIN}" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

echo ">>> [1/2] START daily report" >> "${LOG_FILE}"
"${PYTHON_BIN}" daily_report_generator.py >> "${LOG_FILE}" 2>&1
if [ $? -ne 0 ]; then
  echo "[ERR] daily report failed: $(date)" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

echo ">>> [2/2] START safe stock report" >> "${LOG_FILE}"
"${PYTHON_BIN}" safe_stock_report_generator.py --publish >> "${LOG_FILE}" 2>&1
if [ $? -ne 0 ]; then
  echo "[ERR] safe stock report failed: $(date)" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

echo "[END] evening report job success: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
