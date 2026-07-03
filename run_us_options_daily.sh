#!/bin/bash

set -u

APP_DIR="${APP_DIR:-/root/finance_app/future-app}"
LOG_FILE="${US_OPTIONS_DAILY_LOG_FILE:-${APP_DIR}/update_us_options_daily.log}"
PYTHON_BIN="${PYTHON_BIN:-${APP_DIR}/venv/bin/python}"
LOCK_FILE="${US_OPTIONS_DAILY_LOCK_FILE:-/tmp/run_us_options_daily.lock}"

US_OPTIONS_TIMEOUT_SECONDS="${US_OPTIONS_TIMEOUT_SECONDS:-3600}"
US_OPTIONS_UNDERLYINGS="${US_OPTIONS_UNDERLYINGS:-SPY,QQQ,IWM,GLD,TLT,SLV,XLF,XLE,DIA,HYG,TSLA,NVDA,AMD,AAPL,AMZN}"
US_OPTIONS_SHORT_STRIKE_BAND_PCT="${US_OPTIONS_SHORT_STRIKE_BAND_PCT:-5}"
US_OPTIONS_EXTRA_ARGS="${US_OPTIONS_EXTRA_ARGS:-}"
MARKET_CLIMATE_DAILY_ENABLED="${MARKET_CLIMATE_DAILY_ENABLED:-1}"
MARKET_CLIMATE_TIMEOUT_SECONDS="${MARKET_CLIMATE_TIMEOUT_SECONDS:-240}"
US_OPTIONS_CONE_CACHE_ENABLED="${US_OPTIONS_CONE_CACHE_ENABLED:-1}"
US_OPTIONS_CONE_CACHE_WINDOW="${US_OPTIONS_CONE_CACHE_WINDOW:-5}"
US_OPTIONS_CONE_CACHE_TIMEOUT_SECONDS="${US_OPTIONS_CONE_CACHE_TIMEOUT_SECONDS:-900}"
US_OPTIONS_OI_DEFENSE_CACHE_ENABLED="${US_OPTIONS_OI_DEFENSE_CACHE_ENABLED:-1}"
US_OPTIONS_OI_DEFENSE_CACHE_WINDOW="${US_OPTIONS_OI_DEFENSE_CACHE_WINDOW:-20}"
US_OPTIONS_OI_DEFENSE_CACHE_TIMEOUT_SECONDS="${US_OPTIONS_OI_DEFENSE_CACHE_TIMEOUT_SECONDS:-900}"

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

if [ "${MARKET_CLIMATE_DAILY_ENABLED}" = "1" ]; then
  TMP_CLIMATE_LOG="$(mktemp /tmp/market_climate_daily.XXXXXX.log)"
  CLIMATE_RC=0
  echo "MARKET_CLIMATE_DAILY START: $(date)" >> "${LOG_FILE}"
  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=TERM --kill-after=15 "${MARKET_CLIMATE_TIMEOUT_SECONDS}" \
      "${PYTHON_BIN}" -u "update_market_climate_daily.py" > "${TMP_CLIMATE_LOG}" 2>&1
    CLIMATE_RC=$?
  else
    "${PYTHON_BIN}" -u "update_market_climate_daily.py" > "${TMP_CLIMATE_LOG}" 2>&1
    CLIMATE_RC=$?
  fi
  cat "${TMP_CLIMATE_LOG}" >> "${LOG_FILE}"
  if [ "${CLIMATE_RC}" -eq 124 ]; then
    echo "MARKET_CLIMATE_DAILY WARN: timeout(${MARKET_CLIMATE_TIMEOUT_SECONDS}s)" >> "${LOG_FILE}"
  elif [ "${CLIMATE_RC}" -ne 0 ]; then
    echo "MARKET_CLIMATE_DAILY WARN: rc=${CLIMATE_RC}" >> "${LOG_FILE}"
  else
    echo "MARKET_CLIMATE_DAILY OK: rc=0" >> "${LOG_FILE}"
  fi
  rm -f "${TMP_CLIMATE_LOG}"
else
  echo "MARKET_CLIMATE_DAILY SKIP: disabled" >> "${LOG_FILE}"
fi

if [ "${US_OPTIONS_CONE_CACHE_ENABLED}" = "1" ]; then
  TMP_CONE_LOG="$(mktemp /tmp/us_options_cone_cache.XXXXXX.log)"
  CONE_RC=0
  echo "US_OPTIONS_CONE_CACHE START: $(date)" >> "${LOG_FILE}"
  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=TERM --kill-after=15 "${US_OPTIONS_CONE_CACHE_TIMEOUT_SECONDS}" \
      "${PYTHON_BIN}" -u "scripts/rebuild_us_option_volatility_cone_cache.py" \
      --underlyings "${US_OPTIONS_UNDERLYINGS}" \
      --window "${US_OPTIONS_CONE_CACHE_WINDOW}" \
      --apply > "${TMP_CONE_LOG}" 2>&1
    CONE_RC=$?
  else
    "${PYTHON_BIN}" -u "scripts/rebuild_us_option_volatility_cone_cache.py" \
      --underlyings "${US_OPTIONS_UNDERLYINGS}" \
      --window "${US_OPTIONS_CONE_CACHE_WINDOW}" \
      --apply > "${TMP_CONE_LOG}" 2>&1
    CONE_RC=$?
  fi
  cat "${TMP_CONE_LOG}" >> "${LOG_FILE}"
  if [ "${CONE_RC}" -eq 124 ]; then
    echo "US_OPTIONS_CONE_CACHE WARN: timeout(${US_OPTIONS_CONE_CACHE_TIMEOUT_SECONDS}s)" >> "${LOG_FILE}"
  elif [ "${CONE_RC}" -ne 0 ]; then
    echo "US_OPTIONS_CONE_CACHE WARN: rc=${CONE_RC}" >> "${LOG_FILE}"
  else
    echo "US_OPTIONS_CONE_CACHE OK: rc=0" >> "${LOG_FILE}"
  fi
  rm -f "${TMP_CONE_LOG}"
else
  echo "US_OPTIONS_CONE_CACHE SKIP: disabled" >> "${LOG_FILE}"
fi

if [ "${US_OPTIONS_OI_DEFENSE_CACHE_ENABLED}" = "1" ]; then
  TMP_OI_DEFENSE_LOG="$(mktemp /tmp/us_options_oi_defense_cache.XXXXXX.log)"
  OI_DEFENSE_RC=0
  echo "US_OPTIONS_OI_DEFENSE_CACHE START: $(date)" >> "${LOG_FILE}"
  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=TERM --kill-after=15 "${US_OPTIONS_OI_DEFENSE_CACHE_TIMEOUT_SECONDS}" \
      "${PYTHON_BIN}" -u "scripts/rebuild_us_option_oi_defense_cache.py" \
      --underlyings "${US_OPTIONS_UNDERLYINGS}" \
      --window "${US_OPTIONS_OI_DEFENSE_CACHE_WINDOW}" \
      --apply > "${TMP_OI_DEFENSE_LOG}" 2>&1
    OI_DEFENSE_RC=$?
  else
    "${PYTHON_BIN}" -u "scripts/rebuild_us_option_oi_defense_cache.py" \
      --underlyings "${US_OPTIONS_UNDERLYINGS}" \
      --window "${US_OPTIONS_OI_DEFENSE_CACHE_WINDOW}" \
      --apply > "${TMP_OI_DEFENSE_LOG}" 2>&1
    OI_DEFENSE_RC=$?
  fi
  cat "${TMP_OI_DEFENSE_LOG}" >> "${LOG_FILE}"
  if [ "${OI_DEFENSE_RC}" -eq 124 ]; then
    echo "US_OPTIONS_OI_DEFENSE_CACHE WARN: timeout(${US_OPTIONS_OI_DEFENSE_CACHE_TIMEOUT_SECONDS}s)" >> "${LOG_FILE}"
  elif [ "${OI_DEFENSE_RC}" -ne 0 ]; then
    echo "US_OPTIONS_OI_DEFENSE_CACHE WARN: rc=${OI_DEFENSE_RC}" >> "${LOG_FILE}"
  else
    echo "US_OPTIONS_OI_DEFENSE_CACHE OK: rc=0" >> "${LOG_FILE}"
  fi
  rm -f "${TMP_OI_DEFENSE_LOG}"
else
  echo "US_OPTIONS_OI_DEFENSE_CACHE SKIP: disabled" >> "${LOG_FILE}"
fi

echo "US_OPTIONS_DAILY END: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"

rm -f "${TMP_LOG}"
exit "${RC}"
