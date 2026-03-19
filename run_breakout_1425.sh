#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export TZ=Asia/Shanghai

LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/breakout_1425.log"
LOCK_FILE="${LOG_DIR}/breakout_1425.lock"
LOCK_DIR_FALLBACK="${LOCK_FILE}.d"

if [[ -x "${SCRIPT_DIR}/.venv311/bin/python" ]]; then
  PY_BIN="${SCRIPT_DIR}/.venv311/bin/python"
elif [[ -x "${SCRIPT_DIR}/venv/bin/python" ]]; then
  PY_BIN="${SCRIPT_DIR}/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY_BIN="$(command -v python3)"
else
  {
    echo ""
    echo "========================================"
    echo "breakout job start: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "ERROR: python interpreter not found"
    echo "========================================"
  } >> "${LOG_FILE}" 2>&1
  exit 1
fi

if command -v flock >/dev/null 2>&1; then
  exec 9>"${LOCK_FILE}"
  if ! flock -n 9; then
    {
      echo ""
      echo "========================================"
      echo "breakout job skipped: another instance is running ($(date '+%Y-%m-%d %H:%M:%S %Z'))"
      echo "========================================"
    } >> "${LOG_FILE}" 2>&1
    exit 0
  fi
else
  # macOS 等环境通常不带 flock，降级为 lock-dir 方案。
  if ! mkdir "${LOCK_DIR_FALLBACK}" 2>/dev/null; then
    {
      echo ""
      echo "========================================"
      echo "breakout job skipped: lock dir exists (${LOCK_DIR_FALLBACK}) ($(date '+%Y-%m-%d %H:%M:%S %Z'))"
      echo "========================================"
    } >> "${LOG_FILE}" 2>&1
    exit 0
  fi
  trap 'rmdir "${LOCK_DIR_FALLBACK}" 2>/dev/null || true' EXIT
fi

rc=0
{
  echo ""
  echo "========================================"
  echo "breakout job start: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "script_dir: ${SCRIPT_DIR}"
  echo "python: ${PY_BIN}"

  if "${PY_BIN}" breakout_alert_job.py; then
    rc=0
  else
    rc=$?
  fi

  echo "breakout job end: $(date '+%Y-%m-%d %H:%M:%S %Z') (exit=${rc})"
  echo "========================================"
} >> "${LOG_FILE}" 2>&1

exit "${rc}"
