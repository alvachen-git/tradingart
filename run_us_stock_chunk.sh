#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update.log"
PYTHON_BIN="${APP_DIR}/venv/bin/python"
LOCK_FILE="/tmp/run_us_stock_chunk.lock"
US_CHUNK_STATE_FILE="${APP_DIR}/.us_chunk_state"

US_CHUNK_TIMEOUT_SECONDS="${US_CHUNK_TIMEOUT_SECONDS:-2400}"
US_SYMBOL_CHUNK_TOTAL="${US_SYMBOL_CHUNK_TOTAL:-4}"
US_ENABLE_BACKFILL="${US_ENABLE_BACKFILL:-true}"
US_BACKFILL_DAYS_PER_RUN="${US_BACKFILL_DAYS_PER_RUN:-60}"
US_MAX_SYMBOLS_PER_RUN="${US_MAX_SYMBOLS_PER_RUN:-0}"

cd "${APP_DIR}" || exit 1

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "US_CHUNK_RUN ERROR: start $(date)" >> "${LOG_FILE}"
  echo "US_CHUNK_RUN ERROR: python venv not found: ${PYTHON_BIN}" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "US_CHUNK_RUN WARN: skip duplicated run at $(date) (lock=${LOCK_FILE})" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 0
fi

normalize_positive_int() {
  local raw="$1"
  local fallback="$2"
  if ! [[ "${raw}" =~ ^[0-9]+$ ]] || [ "${raw}" -lt 1 ]; then
    echo "${fallback}"
    return
  fi
  echo "${raw}"
}

normalize_chunk_index() {
  local raw="$1"
  local total="$2"
  local idx
  if ! [[ "${raw}" =~ ^-?[0-9]+$ ]]; then
    raw=0
  fi
  idx=$((raw % total))
  if [ "${idx}" -lt 0 ]; then
    idx=$((idx + total))
  fi
  echo "${idx}"
}

CHUNK_TOTAL="$(normalize_positive_int "${US_SYMBOL_CHUNK_TOTAL}" 4)"
TIMEOUT_SECONDS="$(normalize_positive_int "${US_CHUNK_TIMEOUT_SECONDS}" 2400)"

CHUNK_SOURCE="rotate"
if [ -n "${US_SYMBOL_CHUNK_INDEX:-}" ]; then
  CHUNK_SOURCE="external"
  CHUNK_INDEX="$(normalize_chunk_index "${US_SYMBOL_CHUNK_INDEX}" "${CHUNK_TOTAL}")"
else
  LAST_INDEX=-1
  if [ -f "${US_CHUNK_STATE_FILE}" ]; then
    LAST_INDEX="$(cat "${US_CHUNK_STATE_FILE}" 2>/dev/null || echo "-1")"
    if ! [[ "${LAST_INDEX}" =~ ^-?[0-9]+$ ]]; then
      LAST_INDEX=-1
    fi
  fi
  CHUNK_INDEX=$(((LAST_INDEX + 1 + CHUNK_TOTAL) % CHUNK_TOTAL))
fi

# Advance pointer before run: failures still roll to next chunk on next schedule.
printf "%s\n" "${CHUNK_INDEX}" > "${US_CHUNK_STATE_FILE}"

export US_SYMBOL_CHUNK_TOTAL="${CHUNK_TOTAL}"
export US_SYMBOL_CHUNK_INDEX="${CHUNK_INDEX}"
export US_ENABLE_BACKFILL="${US_ENABLE_BACKFILL}"
export US_BACKFILL_DAYS_PER_RUN="${US_BACKFILL_DAYS_PER_RUN}"
export US_MAX_SYMBOLS_PER_RUN="${US_MAX_SYMBOLS_PER_RUN}"

TMP_LOG="$(mktemp /tmp/us_chunk_run.XXXXXX.log)"
RC=0

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "US_CHUNK_RUN START: $(date)" >> "${LOG_FILE}"
echo "US_CHUNK_RUN CONFIG: chunk_index=$((CHUNK_INDEX + 1))/${CHUNK_TOTAL}, source=${CHUNK_SOURCE}, timeout=${TIMEOUT_SECONDS}s, backfill=${US_ENABLE_BACKFILL}, backfill_days=${US_BACKFILL_DAYS_PER_RUN}, max_symbols=${US_MAX_SYMBOLS_PER_RUN}" >> "${LOG_FILE}"

if command -v timeout >/dev/null 2>&1; then
  timeout --signal=TERM --kill-after=30 "${TIMEOUT_SECONDS}" "${PYTHON_BIN}" -u "update_stock_tiingo.py" > "${TMP_LOG}" 2>&1
  RC=$?
else
  "${PYTHON_BIN}" -u "update_stock_tiingo.py" > "${TMP_LOG}" 2>&1
  RC=$?
fi

cat "${TMP_LOG}" >> "${LOG_FILE}"
SUMMARY_LINE="$(grep -E "symbol_success=|backfill_saved=|saved_rows=" "${TMP_LOG}" | tail -n 1 || true)"
if [ -n "${SUMMARY_LINE}" ]; then
  echo "US_CHUNK_RUN SUMMARY: ${SUMMARY_LINE}" >> "${LOG_FILE}"
fi

if [ "${RC}" -eq 124 ]; then
  echo "US_CHUNK_RUN WARN: timeout(${TIMEOUT_SECONDS}s), next schedule will continue with next chunk" >> "${LOG_FILE}"
elif [ "${RC}" -ne 0 ]; then
  echo "US_CHUNK_RUN WARN: rc=${RC}, next schedule will continue with next chunk" >> "${LOG_FILE}"
else
  echo "US_CHUNK_RUN OK: rc=0" >> "${LOG_FILE}"
fi

echo "US_CHUNK_RUN END: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"

rm -f "${TMP_LOG}"
exit "${RC}"
