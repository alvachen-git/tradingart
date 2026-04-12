#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export TZ=Asia/Shanghai

LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/macro_radar_2030.log"
LOCK_FILE="${LOG_DIR}/macro_radar_2030.lock"

if [[ -x "${SCRIPT_DIR}/.venv311/bin/python" ]]; then
  PY_BIN="${SCRIPT_DIR}/.venv311/bin/python"
elif [[ -x "${SCRIPT_DIR}/venv/bin/python" ]]; then
  PY_BIN="${SCRIPT_DIR}/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY_BIN="$(command -v python3)"
else
  echo "[macro_radar] python interpreter not found" >> "${LOG_FILE}" 2>&1
  exit 1
fi

if command -v flock >/dev/null 2>&1; then
  exec 9>"${LOCK_FILE}"
  if ! flock -n 9; then
    echo "[macro_radar] skipped: another instance is running ($(date '+%Y-%m-%d %H:%M:%S %Z'))" >> "${LOG_FILE}" 2>&1
    exit 0
  fi
fi

rc=0
{
  echo ""
  echo "========================================"
  echo "[macro_radar] start: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "[macro_radar] python: ${PY_BIN}"

  if "${PY_BIN}" macro_risk_radar_generator.py; then
    rc=0
  else
    rc=$?
  fi

  echo "[macro_radar] end: $(date '+%Y-%m-%d %H:%M:%S %Z') (exit=${rc})"
  echo "========================================"
} >> "${LOG_FILE}" 2>&1

exit "${rc}"

