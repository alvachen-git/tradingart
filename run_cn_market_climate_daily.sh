#!/usr/bin/env bash

set -u

# Cron (Asia/Shanghai, Mon-Fri 20:45):
# 45 20 * * 1-5 /bin/bash /root/finance_app/future-app/run_cn_market_climate_daily.sh

APP_DIR="/root/finance_app/future-app"
LOG_DIR="${APP_DIR}/logs"
LOG_FILE="${LOG_DIR}/cn_market_climate_daily.log"
LOCK_FILE="/tmp/cn_market_climate_daily.lock"
ATTEMPTS="${CLIMATE_UPDATE_ATTEMPTS:-3}"
RETRY_SLEEP_SECONDS="${CLIMATE_RETRY_SLEEP_SECONDS:-600}"
TIMEOUT_SECONDS="${CLIMATE_UPDATE_TIMEOUT_SECONDS:-900}"

cd "${APP_DIR}" || exit 1
mkdir -p "${LOG_DIR}"
export TZ=Asia/Shanghai

if [ -x "${APP_DIR}/venv/bin/python" ]; then
  PYTHON_BIN="${APP_DIR}/venv/bin/python"
elif [ -x "${APP_DIR}/.venv311/bin/python" ]; then
  PYTHON_BIN="${APP_DIR}/.venv311/bin/python"
else
  echo "ERROR: project Python venv not found" >> "${LOG_FILE}"
  exit 1
fi

if ! command -v timeout >/dev/null 2>&1; then
  echo "ERROR: timeout command not found" >> "${LOG_FILE}"
  exit 1
fi

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "WARN: climate update skipped because another instance is running: $(date '+%F %T %Z')" >> "${LOG_FILE}"
  exit 0
fi

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "CLIMATE_UPDATE_START: $(date '+%F %T %Z')" >> "${LOG_FILE}"
echo "PYTHON_BIN: ${PYTHON_BIN}" >> "${LOG_FILE}"
echo "ATTEMPTS: ${ATTEMPTS}, RETRY_SLEEP_SECONDS: ${RETRY_SLEEP_SECONDS}, TIMEOUT_SECONDS: ${TIMEOUT_SECONDS}" >> "${LOG_FILE}"

attempt=1
last_rc=1
while [ "${attempt}" -le "${ATTEMPTS}" ]; do
  echo ">>> attempt ${attempt}/${ATTEMPTS}: $(date '+%F %T %Z')" >> "${LOG_FILE}"
  timeout --signal=TERM --kill-after=30 "${TIMEOUT_SECONDS}" \
    "${PYTHON_BIN}" -u update_cn_market_climate_daily.py --require-core-date \
    >> "${LOG_FILE}" 2>&1
  last_rc=$?
  echo "<<< attempt ${attempt}/${ATTEMPTS} rc=${last_rc}: $(date '+%F %T %Z')" >> "${LOG_FILE}"

  if [ "${last_rc}" -eq 0 ]; then
    echo "CLIMATE_UPDATE_OK: $(date '+%F %T %Z')" >> "${LOG_FILE}"
    echo "========================================" >> "${LOG_FILE}"
    exit 0
  fi
  if [ "${attempt}" -lt "${ATTEMPTS}" ]; then
    echo "WARN: core metrics not ready; retry in ${RETRY_SLEEP_SECONDS}s" >> "${LOG_FILE}"
    sleep "${RETRY_SLEEP_SECONDS}"
  fi
  attempt=$((attempt + 1))
done

echo "CLIMATE_UPDATE_FAILED rc=${last_rc}: $(date '+%F %T %Z')" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
exit "${last_rc}"
