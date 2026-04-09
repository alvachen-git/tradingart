#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update.log"
LOCK_FILE="/tmp/run_daily5.lock"
STATE_DIR="/tmp/run_daily5_state"
CURRENT_PID_FILE="${STATE_DIR}/current.pid"
CURRENT_PGID_FILE="${STATE_DIR}/current.pgid"
CURRENT_STEP_FILE="${STATE_DIR}/current.step"
CURRENT_CMD_FILE="${STATE_DIR}/current.cmd"
PYTHON_BIN="${APP_DIR}/venv/bin/python"
FAILED_STEPS=0

DEFAULT_STEP_TIMEOUT_SECONDS="${DEFAULT_STEP_TIMEOUT_SECONDS:-2400}"
STEP1_TIMEOUT_SECONDS="${STEP1_TIMEOUT_SECONDS:-2400}"
STEP2_TIMEOUT_SECONDS="${STEP2_TIMEOUT_SECONDS:-900}"
STEP3_TIMEOUT_SECONDS="${STEP3_TIMEOUT_SECONDS:-1800}"
STEP4_TIMEOUT_SECONDS="${STEP4_TIMEOUT_SECONDS:-1800}"

mkdir -p "${STATE_DIR}"

log_line() {
  printf "%s\n" "$1" >> "${LOG_FILE}"
}

clear_current_state() {
  rm -f "${CURRENT_PID_FILE}" "${CURRENT_PGID_FILE}" "${CURRENT_STEP_FILE}" "${CURRENT_CMD_FILE}"
}

cleanup() {
  clear_current_state
}

trap cleanup EXIT INT TERM

normalize_timeout() {
  local raw="$1"
  if ! [[ "${raw}" =~ ^[0-9]+$ ]] || [ "${raw}" -lt 1 ]; then
    echo "${DEFAULT_STEP_TIMEOUT_SECONDS}"
    return
  fi
  echo "${raw}"
}

get_process_group_id() {
  local pid="$1"
  ps -o pgid= -p "${pid}" 2>/dev/null | tr -d '[:space:]'
}

terminate_running_step() {
  local pid="$1"
  local pgid="$2"
  local grace_seconds=10

  if [ -n "${pgid}" ]; then
    kill -TERM "-${pgid}" 2>/dev/null || true
  fi
  kill -TERM "${pid}" 2>/dev/null || true

  while [ "${grace_seconds}" -gt 0 ]; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      return
    fi
    sleep 1
    grace_seconds=$((grace_seconds - 1))
  done

  if [ -n "${pgid}" ]; then
    kill -KILL "-${pgid}" 2>/dev/null || true
  fi
  kill -KILL "${pid}" 2>/dev/null || true
}

run_step() {
  local idx="$1"
  local total="$2"
  local title="$3"
  local script="$4"
  local timeout_seconds
  local started_at
  local ended_at
  local elapsed
  local pid=""
  local pgid=""
  local rc=0

  timeout_seconds="$(normalize_timeout "${5:-${DEFAULT_STEP_TIMEOUT_SECONDS}}")"
  started_at=$(date +%s)

  log_line ">>> [${idx}/${total}] 开始${title} (timeout=${timeout_seconds}s)..."

  "${PYTHON_BIN}" -u "${script}" >> "${LOG_FILE}" 2>&1 &
  pid=$!
  pgid="$(get_process_group_id "${pid}")"

  printf "%s\n" "${pid}" > "${CURRENT_PID_FILE}"
  printf "%s\n" "${pgid}" > "${CURRENT_PGID_FILE}"
  printf "%s\n" "${title}" > "${CURRENT_STEP_FILE}"
  printf "%s\n" "${script}" > "${CURRENT_CMD_FILE}"

  log_line "ℹ️ [${idx}/${total}] 当前进程: pid=${pid} pgid=${pgid:-unknown} script=${script}"
  log_line "ℹ️ 排障命令: ps -fp ${pid} | kill -TERM ${pid} | kill -KILL ${pid}"

  while kill -0 "${pid}" 2>/dev/null; do
    elapsed=$(( $(date +%s) - started_at ))
    if [ "${elapsed}" -ge "${timeout_seconds}" ]; then
      log_line "⚠️ [${idx}/${total}] ${title} 超时(${timeout_seconds}s)，开始终止 pid=${pid}"
      terminate_running_step "${pid}" "${pgid}"
      rc=124
      break
    fi
    sleep 2
  done

  if [ "${rc}" -eq 0 ]; then
    wait "${pid}"
    rc=$?
  else
    wait "${pid}" 2>/dev/null || true
  fi

  clear_current_state

  ended_at=$(date +%s)
  elapsed=$((ended_at - started_at))
  log_line "<<< [${idx}/${total}] 结束${title} (rc=${rc}, elapsed=${elapsed}s)"

  if [ "${rc}" -eq 124 ]; then
    log_line "⚠️ [${idx}/${total}] ${title} 已超时终止，继续后续步骤"
  elif [ "${rc}" -ne 0 ]; then
    log_line "❌ [${idx}/${total}] ${title} 执行失败，继续后续步骤"
  fi
  log_line ""

  if [ "${rc}" -ne 0 ]; then
    FAILED_STEPS=$((FAILED_STEPS + 1))
  fi
}

cd "${APP_DIR}" || exit 1

if [ ! -x "${PYTHON_BIN}" ]; then
  log_line ""
  log_line "========================================"
  log_line "❌ 任务开始: $(date)"
  log_line "❌ 未找到虚拟环境解释器: ${PYTHON_BIN}"
  log_line "========================================"
  exit 1
fi

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  log_line ""
  log_line "========================================"
  log_line "⚠️ 任务跳过: $(date)"
  log_line "⚠️ 检测到已有 run_daily5.sh 在运行，避免重复执行 (lock=${LOCK_FILE})"
  log_line "========================================"
  exit 0
fi

log_line ""
log_line "========================================"
log_line "⏰ RUN_DAILY5 START: $(date)"
log_line "🐍 Python: ${PYTHON_BIN}"
log_line "📝 Log file: ${LOG_FILE}"
log_line "🔒 Lock: ${LOCK_FILE}"
log_line "🧭 State dir: ${STATE_DIR}"

run_step 1 4 "更新期货席位数据" "update_open_oneday.py" "${STEP1_TIMEOUT_SECONDS}"
run_step 2 4 "更新外资数据" "calc_foreign_capital.py" "${STEP2_TIMEOUT_SECONDS}"
run_step 3 4 "更新股票资金流数据" "update_stock_moneyflow_daily.py" "${STEP3_TIMEOUT_SECONDS}"
run_step 4 4 "更新产业链快照数据" "update_industry_chain_snapshot_daily.py" "${STEP4_TIMEOUT_SECONDS}"

if [ "${FAILED_STEPS}" -gt 0 ]; then
  log_line "⚠️ 本次任务失败步骤数: ${FAILED_STEPS}"
  log_line "❌ RUN_DAILY5 END: $(date)"
  log_line "========================================"
  exit 1
fi

log_line "✅ 本次任务全部步骤成功"
log_line "✅ RUN_DAILY5 END: $(date)"
log_line "========================================"
