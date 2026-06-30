#!/bin/bash

set -u

APP_DIR="${APP_DIR:-/root/finance_app/future-app}"
LOG_FILE="${US_OPTIONS_DAILY_LOG_FILE:-${APP_DIR}/update_us_options_daily.log}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/venv/bin/python}"
LOCK_FILE="${US_OPTIONS_DAILY_LOCK_FILE:-/tmp/run_us_options_daily.lock}"

US_OPTIONS_TIMEOUT_SECONDS="${US_OPTIONS_TIMEOUT_SECONDS:-3600}"
US_OPTIONS_UNDERLYINGS="${US_OPTIONS_UNDERLYINGS:-SPY,QQQ,IWM}"
US_OPTIONS_SHORT_STRIKE_BAND_PCT="${US_OPTIONS_SHORT_STRIKE_BAND_PCT:-5}"
US_OPTIONS_EXTRA_ARGS="${US_OPTIONS_EXTRA_ARGS:-}"

cd "${APP_DIR}" || exit 1

load_env_file() {
  ENV_PATH="$1"
  if [ ! -f "${ENV_PATH}" ]; then
    return 0
  fi
  while IFS= read -r ENV_LINE || [ -n "${ENV_LINE}" ]; do
    case "${ENV_LINE}" in
      ""|\#*) continue ;;
    esac
    case "${ENV_LINE}" in
      *=*) ;;
      *) continue ;;
    esac
    ENV_KEY="${ENV_LINE%%=*}"
    ENV_VALUE="${ENV_LINE#*=}"
    case "${ENV_KEY}" in
      ""|*[!A-Za-z0-9_]*|[0-9]*) continue ;;
    esac
    ENV_VALUE="${ENV_VALUE%$'\r'}"
    if [ "${#ENV_VALUE}" -ge 2 ]; then
      FIRST_CHAR="${ENV_VALUE:0:1}"
      LAST_CHAR="${ENV_VALUE: -1}"
      if { [ "${FIRST_CHAR}" = "\"" ] && [ "${LAST_CHAR}" = "\"" ]; } || { [ "${FIRST_CHAR}" = "'" ] && [ "${LAST_CHAR}" = "'" ]; }; then
        ENV_VALUE="${ENV_VALUE:1:${#ENV_VALUE}-2}"
      fi
    fi
    export "${ENV_KEY}=${ENV_VALUE}"
  done < "${ENV_PATH}"
}

load_env_file "${APP_DIR}/../.env"
load_env_file "${APP_DIR}/.env"

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "US_OPTIONS_DAILY ERROR: start $(date)" >> "${LOG_FILE}"
  echo "US_OPTIONS_DAILY ERROR: python venv not found: ${PYTHON_BIN}" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "US_OPTIONS_DAILY WARN: skip duplicated run at $(date) (lock=${LOCK_FILE})" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 0
fi

TMP_LOG="$(mktemp /tmp/us_options_daily.XXXXXX.log)"
RC=0

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "US_OPTIONS_DAILY START: $(date)" >> "${LOG_FILE}"
echo "US_OPTIONS_DAILY CONFIG: underlyings=${US_OPTIONS_UNDERLYINGS}, timeout=${US_OPTIONS_TIMEOUT_SECONDS}s, short_band=${US_OPTIONS_SHORT_STRIKE_BAND_PCT}" >> "${LOG_FILE}"
if [ -n "${MASSIVE_API_KEY:-${POLYGON_API_KEY:-}}" ]; then
  echo "US_OPTIONS_DAILY ENV: api_key_present=true" >> "${LOG_FILE}"
else
  echo "US_OPTIONS_DAILY ENV: api_key_present=false" >> "${LOG_FILE}"
fi

if command -v timeout >/dev/null 2>&1; then
  timeout --signal=TERM --kill-after=30 "${US_OPTIONS_TIMEOUT_SECONDS}" \
    "${PYTHON_BIN}" -u "run_us_options_daily.py" \
    --underlyings "${US_OPTIONS_UNDERLYINGS}" \
    --short-strike-band-pct "${US_OPTIONS_SHORT_STRIKE_BAND_PCT}" \
    ${US_OPTIONS_EXTRA_ARGS} > "${TMP_LOG}" 2>&1
  RC=$?
else
  "${PYTHON_BIN}" -u "run_us_options_daily.py" \
    --underlyings "${US_OPTIONS_UNDERLYINGS}" \
    --short-strike-band-pct "${US_OPTIONS_SHORT_STRIKE_BAND_PCT}" \
    ${US_OPTIONS_EXTRA_ARGS} > "${TMP_LOG}" 2>&1
  RC=$?
fi

cat "${TMP_LOG}" >> "${LOG_FILE}"

if [ "${RC}" -eq 124 ]; then
  echo "US_OPTIONS_DAILY ERROR: timeout(${US_OPTIONS_TIMEOUT_SECONDS}s)" >> "${LOG_FILE}"
elif [ "${RC}" -ne 0 ]; then
  echo "US_OPTIONS_DAILY ERROR: rc=${RC}" >> "${LOG_FILE}"
else
  echo "US_OPTIONS_DAILY OK: rc=0" >> "${LOG_FILE}"
fi

echo "US_OPTIONS_DAILY END: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"

rm -f "${TMP_LOG}"
exit "${RC}"
