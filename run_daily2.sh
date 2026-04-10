#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update.log"
PYTHON_BIN="${APP_DIR}/venv/bin/python"
FAILED_STEPS=0
US_CHUNK_STATE_FILE="${APP_DIR}/.us_chunk_state"
LOCK_FILE="/tmp/run_daily2.lock"

# Step timeout controls (seconds)
DEFAULT_STEP_TIMEOUT_SECONDS="${DEFAULT_STEP_TIMEOUT_SECONDS:-2400}"
STEP1_TIMEOUT_SECONDS="${STEP1_TIMEOUT_SECONDS:-2400}"
STEP2_TIMEOUT_SECONDS="${STEP2_TIMEOUT_SECONDS:-2400}"
STEP3_TIMEOUT_SECONDS="${STEP3_TIMEOUT_SECONDS:-1800}"
STEP4_TIMEOUT_SECONDS="${STEP4_TIMEOUT_SECONDS:-1800}"
STEP5_TIMEOUT_SECONDS="${STEP5_TIMEOUT_SECONDS:-1800}"

# DXY guard defaults for update_micro_daily.py
DXY_REQUIRED="${DXY_REQUIRED:-1}"
DXY_MAX_STALE_DAYS="${DXY_MAX_STALE_DAYS:-3}"
DXY_FETCH_ROUNDS="${DXY_FETCH_ROUNDS:-4}"
DXY_RETRY_SLEEP_SECONDS="${DXY_RETRY_SLEEP_SECONDS:-60}"
DXY_BACKFILL_DAYS="${DXY_BACKFILL_DAYS:-60}"
DXY_FRED_FETCH_DAYS="${DXY_FRED_FETCH_DAYS:-180}"
DXY_FRED_SERIES_CANDIDATES="${DXY_FRED_SERIES_CANDIDATES:-DTWEXBGS,DTWEXAFEGS,DTWEXEMEGS,DTWEXM}"

# Disable DCE LG patch by default to prevent step-1 hanging.
ENABLE_DCE_LG_PATCH="${ENABLE_DCE_LG_PATCH:-0}"

cd "${APP_DIR}" || exit 1

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "ERROR: task start $(date)" >> "${LOG_FILE}"
  echo "ERROR: python venv not found: ${PYTHON_BIN}" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "WARN: skip duplicated run at $(date) (lock=${LOCK_FILE})" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 0
fi

normalize_timeout() {
  local raw="$1"
  if ! [[ "${raw}" =~ ^[0-9]+$ ]] || [ "${raw}" -lt 1 ]; then
    echo "${DEFAULT_STEP_TIMEOUT_SECONDS}"
    return
  fi
  echo "${raw}"
}

run_step() {
  local idx="$1"
  local total="$2"
  local title="$3"
  local script="$4"
  local timeout_seconds
  local rc
  local started_at ended_at elapsed

  timeout_seconds="$(normalize_timeout "${5:-${DEFAULT_STEP_TIMEOUT_SECONDS}}")"
  started_at=$(date +%s)
  echo ">>> [${idx}/${total}] START ${title} (timeout=${timeout_seconds}s)" >> "${LOG_FILE}"

  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=TERM --kill-after=30 "${timeout_seconds}" "${PYTHON_BIN}" -u "${script}" >> "${LOG_FILE}" 2>&1
    rc=$?
  else
    echo "WARN: timeout command not found, run without timeout protection" >> "${LOG_FILE}"
    "${PYTHON_BIN}" -u "${script}" >> "${LOG_FILE}" 2>&1
    rc=$?
  fi

  ended_at=$(date +%s)
  elapsed=$((ended_at - started_at))
  echo "<<< [${idx}/${total}] END ${title} (rc=${rc}, elapsed=${elapsed}s)" >> "${LOG_FILE}"

  if [ "${rc}" -eq 124 ]; then
    echo "WARN: [${idx}/${total}] ${title} timeout(${timeout_seconds}s), continue next step" >> "${LOG_FILE}"
  elif [ "${rc}" -eq 137 ]; then
    echo "WARN: [${idx}/${total}] ${title} killed(SIGKILL), continue next step" >> "${LOG_FILE}"
  fi
  echo "" >> "${LOG_FILE}"

  if [ "${rc}" -ne 0 ]; then
    FAILED_STEPS=$((FAILED_STEPS + 1))
  fi
}

prepare_us_chunk_env() {
  local chunk_total chunk_index_raw chunk_index last_idx

  chunk_total="${US_SYMBOL_CHUNK_TOTAL:-4}"
  if ! [[ "${chunk_total}" =~ ^[0-9]+$ ]] || [ "${chunk_total}" -lt 1 ]; then
    chunk_total=4
  fi

  if [ -n "${US_SYMBOL_CHUNK_INDEX:-}" ]; then
    chunk_index_raw="${US_SYMBOL_CHUNK_INDEX}"
    if ! [[ "${chunk_index_raw}" =~ ^-?[0-9]+$ ]]; then
      chunk_index_raw=0
    fi
    chunk_index=$((chunk_index_raw % chunk_total))
    if [ "${chunk_index}" -lt 0 ]; then
      chunk_index=$((chunk_index + chunk_total))
    fi
    echo "INFO: US chunk use external index=${chunk_index}, total=${chunk_total}" >> "${LOG_FILE}"
  else
    last_idx=-1
    if [ -f "${US_CHUNK_STATE_FILE}" ]; then
      last_idx=$(cat "${US_CHUNK_STATE_FILE}" 2>/dev/null || echo "-1")
      if ! [[ "${last_idx}" =~ ^-?[0-9]+$ ]]; then
        last_idx=-1
      fi
    fi
    chunk_index=$(((last_idx + 1 + chunk_total) % chunk_total))
    printf "%s\n" "${chunk_index}" > "${US_CHUNK_STATE_FILE}"
    echo "INFO: US chunk rotate index=${chunk_index}, total=${chunk_total}" >> "${LOG_FILE}"
  fi

  export US_SYMBOL_CHUNK_TOTAL="${chunk_total}"
  export US_SYMBOL_CHUNK_INDEX="${chunk_index}"
}

check_dxy_freshness() {
  local source_line backfill_line latest_line age_line
  local dxy_source dxy_backfilled latest_date age_days
  local now_epoch latest_epoch

  source_line=$(grep -E "DXY_SOURCE=" "${LOG_FILE}" | tail -n 1 || true)
  backfill_line=$(grep -E "DXY_BACKFILLED_DATES=" "${LOG_FILE}" | tail -n 1 || true)
  latest_line=$(grep -E "DXY_LATEST_DATE=" "${LOG_FILE}" | tail -n 1 || true)
  age_line=$(grep -E "DXY_AGE_DAYS=" "${LOG_FILE}" | tail -n 1 || true)

  dxy_source="${source_line#*=}"
  dxy_backfilled="${backfill_line#*=}"
  latest_date="${latest_line#*=}"
  age_days="${age_line#*=}"

  if [ -z "${latest_line}" ] || [ -z "${latest_date}" ] || [ "${latest_date}" = "NONE" ]; then
    echo "WARN: DXY freshness check missing DXY_LATEST_DATE" >> "${LOG_FILE}"
    return 0
  fi

  if ! [[ "${age_days}" =~ ^-?[0-9]+$ ]]; then
    latest_epoch=$(date -d "${latest_date}" +%s 2>/dev/null || echo "")
    now_epoch=$(date +%s)
    if [ -z "${latest_epoch}" ]; then
      echo "WARN: DXY freshness check cannot parse date ${latest_date}" >> "${LOG_FILE}"
      return 0
    fi
    age_days=$(((now_epoch - latest_epoch) / 86400))
  fi

  echo "INFO: DXY source=${dxy_source:-unknown}, backfilled=${dxy_backfilled:-0}, latest=${latest_date}, age_days=${age_days}, max_stale_days=${DXY_MAX_STALE_DAYS}" >> "${LOG_FILE}"
  if [ "${age_days}" -gt "${DXY_MAX_STALE_DAYS}" ]; then
    echo "WARN: DXY stale ${age_days} days (> ${DXY_MAX_STALE_DAYS})" >> "${LOG_FILE}"
  fi
}

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "TASK_START: $(date)" >> "${LOG_FILE}"
echo "PYTHON_BIN: ${PYTHON_BIN}" >> "${LOG_FILE}"
echo "LOCK_FILE: ${LOCK_FILE}" >> "${LOG_FILE}"
echo "STEP_TIMEOUT_DEFAULT: ${DEFAULT_STEP_TIMEOUT_SECONDS}s" >> "${LOG_FILE}"
echo "DCE_LG_PATCH_CONFIG: ENABLE_DCE_LG_PATCH=${ENABLE_DCE_LG_PATCH}" >> "${LOG_FILE}"
echo "DXY_GUARD_CONFIG: required=${DXY_REQUIRED}, max_stale_days=${DXY_MAX_STALE_DAYS}, rounds=${DXY_FETCH_ROUNDS}, retry_sleep=${DXY_RETRY_SLEEP_SECONDS}s, backfill_days=${DXY_BACKFILL_DAYS}, fred_fetch_days=${DXY_FRED_FETCH_DAYS}, fred_candidates=${DXY_FRED_SERIES_CANDIDATES}" >> "${LOG_FILE}"

export ENABLE_DCE_LG_PATCH
run_step 1 5 "update futures holding" "update_open_oneday.py" "${STEP1_TIMEOUT_SECONDS}"
prepare_us_chunk_env
run_step 2 5 "update us stocks" "update_stock_tiingo.py" "${STEP2_TIMEOUT_SECONDS}"
run_step 3 5 "update bonds" "update_bond_data.py" "${STEP3_TIMEOUT_SECONDS}"
run_step 4 5 "update trend monitor" "trend_monitor.py" "${STEP4_TIMEOUT_SECONDS}"

export DXY_REQUIRED
export DXY_MAX_STALE_DAYS
export DXY_FETCH_ROUNDS
export DXY_RETRY_SLEEP_SECONDS
export DXY_BACKFILL_DAYS
export DXY_FRED_FETCH_DAYS
export DXY_FRED_SERIES_CANDIDATES
run_step 5 5 "update macro" "update_micro_daily.py" "${STEP5_TIMEOUT_SECONDS}"

check_dxy_freshness

if [ "${FAILED_STEPS}" -gt 0 ]; then
  echo "WARN: failed steps count=${FAILED_STEPS}" >> "${LOG_FILE}"
else
  echo "OK: all steps success" >> "${LOG_FILE}"
fi

echo "TASK_END: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
