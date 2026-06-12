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

  prepare_qfq_accounting() {
    local pid="$1"
    "${PY_BIN}" update_stock_price_qfq.py \
      --date latest \
      --lookback-days 460 \
      --portfolio-id "${pid}" \
      --portfolio-symbol-scope trades_positions \
      --sleep-sec 0.05
  }

  reprice_qfq_accounting() {
    local pid="$1"
    "${PY_BIN}" reprice_ai_sim_qfq.py --portfolio-id "${pid}"
  }

  echo "phase 1/5: preflight qfq prices for AI stock 1号/2号"
  if prepare_qfq_accounting "official_cn_a_etf_v1" && prepare_qfq_accounting "official_cn_a_etf_v2"; then
    rc=0
  else
    rc=$?
  fi

  if [[ ${rc} -eq 0 ]]; then
    echo "phase 2/5: run AI stock 1号/2号"
    if AI_SIM_SKIP_V3=1 "${PY_BIN}" ai_simulation_service.py; then
      rc=0
    else
      rc=$?
    fi
  else
    echo "skip AI stock 1号/2号 because qfq preflight failed (exit=${rc})"
  fi

  if [[ ${rc} -eq 0 ]]; then
    echo "phase 3/5: refresh and reprice AI stock 1号/2号 qfq accounting"
    if prepare_qfq_accounting "official_cn_a_etf_v1" \
      && prepare_qfq_accounting "official_cn_a_etf_v2" \
      && reprice_qfq_accounting "official_cn_a_etf_v1" \
      && reprice_qfq_accounting "official_cn_a_etf_v2"; then
      rc=0
    else
      rc=$?
    fi
  fi

  if [[ ${rc} -eq 0 ]]; then
    echo "phase 4/5: prepare AI stock 3号 sector OHLC"
    if "${PY_BIN}" update_sector_index_price.py --lookback-days 10; then
      rc=0
    else
      rc=$?
    fi

    if [[ ${rc} -eq 0 ]]; then
      echo "phase 5/5: run AI stock 3号"
      if "${PY_BIN}" run_ai_simulation_v3_daily.py --decision-mode llm_fallback --force; then
        rc=0
      else
        rc=$?
      fi
    else
      echo "skip AI stock 3号 because sector OHLC prepare failed (exit=${rc})"
    fi
  else
    echo "skip AI stock 3号 because 1号/2号 qfq accounting phase failed (exit=${rc})"
  fi

  echo "AI simulation job end: $(date '+%Y-%m-%d %H:%M:%S %Z') (exit=${rc})"
  echo "========================================"
} >> "${LOG_FILE}" 2>&1

exit "${rc}"
