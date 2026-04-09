#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update.log"
PYTHON_BIN="${APP_DIR}/venv/bin/python"
FAILED_STEPS=0
US_CHUNK_STATE_FILE="${APP_DIR}/.us_chunk_state"
LOCK_FILE="/tmp/run_daily2.lock"
DEFAULT_STEP_TIMEOUT_SECONDS="${DEFAULT_STEP_TIMEOUT_SECONDS:-2400}"
STEP1_TIMEOUT_SECONDS="${STEP1_TIMEOUT_SECONDS:-2400}"
STEP2_TIMEOUT_SECONDS="${STEP2_TIMEOUT_SECONDS:-2400}"
STEP3_TIMEOUT_SECONDS="${STEP3_TIMEOUT_SECONDS:-1800}"
STEP4_TIMEOUT_SECONDS="${STEP4_TIMEOUT_SECONDS:-1800}"
STEP5_TIMEOUT_SECONDS="${STEP5_TIMEOUT_SECONDS:-1800}"

cd "${APP_DIR}" || exit 1

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "❌ 任务开始: $(date)" >> "${LOG_FILE}"
  echo "❌ 未找到虚拟环境解释器: ${PYTHON_BIN}" >> "${LOG_FILE}"
  echo "❌ 任务终止，请先创建/修复 venv 环境" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  exit 1
fi

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "" >> "${LOG_FILE}"
  echo "========================================" >> "${LOG_FILE}"
  echo "⚠️ 任务跳过: $(date)" >> "${LOG_FILE}"
  echo "⚠️ 检测到已有 run_daily2.sh 在运行，避免重复执行 (lock=${LOCK_FILE})" >> "${LOG_FILE}"
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

  echo ">>> [${idx}/${total}] 开始${title} (timeout=${timeout_seconds}s)..." >> "${LOG_FILE}"

  if command -v timeout >/dev/null 2>&1; then
    timeout --signal=TERM --kill-after=30 "${timeout_seconds}" "${PYTHON_BIN}" -u "${script}" >> "${LOG_FILE}" 2>&1
    rc=$?
  else
    echo "⚠️ 未找到 timeout 命令，本步骤不设超时保护" >> "${LOG_FILE}"
    "${PYTHON_BIN}" -u "${script}" >> "${LOG_FILE}" 2>&1
    rc=$?
  fi

  ended_at=$(date +%s)
  elapsed=$((ended_at - started_at))
  echo "<<< [${idx}/${total}] 结束${title} (rc=${rc}, elapsed=${elapsed}s)" >> "${LOG_FILE}"

  if [ "${rc}" -eq 124 ]; then
    echo "⚠️ [${idx}/${total}] ${title} 超时(${timeout_seconds}s)，已终止并继续后续步骤" >> "${LOG_FILE}"
  elif [ "${rc}" -eq 137 ]; then
    echo "⚠️ [${idx}/${total}] ${title} 被强制杀死(SIGKILL)，已继续后续步骤" >> "${LOG_FILE}"
  fi
  echo "" >> "${LOG_FILE}"

  if [ ${rc} -ne 0 ]; then
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
    echo "ℹ️ 美股分片: 使用外部指定 index=${chunk_index}, total=${chunk_total}" >> "${LOG_FILE}"
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
    echo "ℹ️ 美股分片: 自动轮转 index=${chunk_index}, total=${chunk_total}" >> "${LOG_FILE}"
  fi

  export US_SYMBOL_CHUNK_TOTAL="${chunk_total}"
  export US_SYMBOL_CHUNK_INDEX="${chunk_index}"
}

check_dxy_freshness() {
  local source_line backfill_line latest_line
  local dxy_source dxy_backfilled latest_date
  local now_epoch latest_epoch age_days

  source_line=$(grep -E "DXY_SOURCE=" "${LOG_FILE}" | tail -n 1 || true)
  backfill_line=$(grep -E "DXY_BACKFILLED_DATES=" "${LOG_FILE}" | tail -n 1 || true)
  latest_line=$(grep -E "DXY_LATEST_DATE=" "${LOG_FILE}" | tail -n 1 || true)

  dxy_source="${source_line#*=}"
  dxy_backfilled="${backfill_line#*=}"
  latest_date="${latest_line#*=}"

  if [ -z "${latest_line}" ] || [ -z "${latest_date}" ] || [ "${latest_date}" = "NONE" ]; then
    echo "⚠️ DXY 新鲜度检查: 未获取到最新日期标记(DXY_LATEST_DATE)" >> "${LOG_FILE}"
    return 0
  fi

  latest_epoch=$(date -d "${latest_date}" +%s 2>/dev/null || echo "")
  now_epoch=$(date +%s)
  if [ -z "${latest_epoch}" ]; then
    echo "⚠️ DXY 新鲜度检查: 无法解析日期 ${latest_date}" >> "${LOG_FILE}"
    return 0
  fi

  age_days=$(((now_epoch - latest_epoch) / 86400))
  echo "ℹ️ DXY 状态: source=${dxy_source:-unknown}, backfilled=${dxy_backfilled:-0}, latest=${latest_date}, age_days=${age_days}" >> "${LOG_FILE}"
  if [ ${age_days} -gt 3 ]; then
    echo "⚠️ DXY 新鲜度告警: 最新日期距今 ${age_days} 天(>3天)，请人工检查宏观数据源" >> "${LOG_FILE}"
  fi
}

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "⏰ 任务开始: $(date)" >> "${LOG_FILE}"
echo "🐍 Python: ${PYTHON_BIN}" >> "${LOG_FILE}"
echo "🔒 Lock: ${LOCK_FILE}" >> "${LOG_FILE}"
echo "⏱️ 默认步骤超时: ${DEFAULT_STEP_TIMEOUT_SECONDS}s" >> "${LOG_FILE}"

run_step 1 5 "更新期货席位数据" "update_open_oneday.py" "${STEP1_TIMEOUT_SECONDS}"
prepare_us_chunk_env
run_step 2 5 "更新美股价格数据" "update_stock_tiingo.py" "${STEP2_TIMEOUT_SECONDS}"
run_step 3 5 "更新债券收益数据" "update_bond_data.py" "${STEP3_TIMEOUT_SECONDS}"
run_step 4 5 "更新热搜数据" "trend_monitor.py" "${STEP4_TIMEOUT_SECONDS}"
run_step 5 5 "更新宏观数据" "update_micro_daily.py" "${STEP5_TIMEOUT_SECONDS}"
check_dxy_freshness

if [ ${FAILED_STEPS} -gt 0 ]; then
  echo "⚠️ 本次任务失败步骤数: ${FAILED_STEPS}" >> "${LOG_FILE}"
else
  echo "✅ 本次任务全部步骤成功" >> "${LOG_FILE}"
fi

echo "✅ 任务结束: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
