#!/usr/bin/env bash
set -Eeuo pipefail

# Cron (Asia/Shanghai, Mon-Fri 20:30):
# 30 20 * * 1-5 /bin/bash /root/finance_app/future-app/run_ai_simulation_weekday_2030.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export TZ=Asia/Shanghai

LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/ai_simulation_daily.log"
LOCK_FILE="${LOG_DIR}/ai_simulation_daily.lock"

# Pick python from common venv layouts first.
if [[ -x "${SCRIPT_DIR}/venv/bin/python" ]]; then
  PY_BIN="${SCRIPT_DIR}/venv/bin/python"
elif [[ -x "${SCRIPT_DIR}/.venv311/bin/python" ]]; then
  PY_BIN="${SCRIPT_DIR}/.venv311/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PY_BIN="$(command -v python)"
else
  {
    echo ""
    echo "========================================"
    echo "AI simulation job start: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "ERROR: python interpreter not found"
    echo "========================================"
  } >> "${LOG_FILE}" 2>&1
  exit 1
fi

# Prevent overlapping runs.
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  {
    echo ""
    echo "========================================"
    echo "AI simulation skipped: another instance is running ($(date '+%Y-%m-%d %H:%M:%S %Z'))"
    echo "========================================"
  } >> "${LOG_FILE}" 2>&1
  exit 0
fi

rc=0
{
  echo ""
  echo "========================================"
  echo "AI simulation job start: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "script_dir: ${SCRIPT_DIR}"
  echo "python: ${PY_BIN}"

  if "${PY_BIN}" ai_simulation_service.py; then
    rc=0
  else
    rc=$?
  fi

  echo "AI simulation job end: $(date '+%Y-%m-%d %H:%M:%S %Z') (exit=${rc})"
  echo "========================================"
} >> "${LOG_FILE}" 2>&1

exit "${rc}"
