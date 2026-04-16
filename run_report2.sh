#!/bin/bash

set -u
set -o pipefail

APP_DIR="/root/finance_app/future-app"
PYTHON_BIN="$APP_DIR/venv/bin/python"
LOG_FILE="$APP_DIR/update.log"
FAILED_COUNT=0
FAILED_STEPS=""

log_line() {
  echo "$1" >> "$LOG_FILE"
}

run_job() {
  local step="$1"
  shift

  log_line ">>> [$step] START: $*"

  if "$PYTHON_BIN" "$@" >> "$LOG_FILE" 2>&1; then
    log_line "[OK] [$step] SUCCESS: $*"
  else
    local code=$?
    log_line "[ERR] [$step] FAILED: $* (exit=${code})"
    FAILED_COUNT=$((FAILED_COUNT + 1))
    if [ -z "$FAILED_STEPS" ]; then
      FAILED_STEPS="$step:$*"
    else
      FAILED_STEPS="$FAILED_STEPS | $step:$*"
    fi
  fi
}

# 1) Enter app directory
if ! cd "$APP_DIR"; then
  echo "[ERR] Cannot enter directory: $APP_DIR" >&2
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  log_line "[ERR] Python interpreter not executable: $PYTHON_BIN"
  exit 1
fi

# 2) Start log
log_line ""
log_line "========================================"
log_line "[START] REPORT JOB START: $(date)"

# 3) Trading-day gate
TODAY=$(date +%Y%m%d)
LATEST_DB_DATE=$("$PYTHON_BIN" -c "from data_engine import get_latest_data_date; d=get_latest_data_date(); s=''.join(ch for ch in str(d) if ch.isdigit())[:8]; print(s)" 2>>"$LOG_FILE" || true)

if [ -z "$LATEST_DB_DATE" ]; then
  log_line "[SKIP] cannot get latest trading date from DB"
  log_line "[END] REPORT JOB END: $(date)"
  log_line "========================================"
  exit 0
fi

if [ "$LATEST_DB_DATE" != "$TODAY" ]; then
  log_line "[SKIP] non-trading day or data not ready (today=$TODAY latest_db=$LATEST_DB_DATE)"
  log_line "[END] REPORT JOB END: $(date)"
  log_line "========================================"
  exit 0
fi

# 4) Execute in sequence (continue on error)
# Performance rule: only update today's cross-asset IV index (no historical backfill in cron).
run_job "1/4" update_cross_asset_iv_index_daily.py
run_job "2/4" fund_flow_report_generator.py
run_job "3/4" broker_position_generator.py
run_job "4/4" expiry_option_generator.py

# 5) End
if [ "$FAILED_COUNT" -gt 0 ]; then
  log_line "[WARN] REPORT JOB END WITH ERRORS: $(date) | failed_count=$FAILED_COUNT | failed_steps=$FAILED_STEPS"
  log_line "========================================"
  exit 1
fi

log_line "[END] REPORT JOB END: $(date)"
log_line "========================================"
