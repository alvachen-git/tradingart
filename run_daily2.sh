#!/bin/bash

set -u

APP_DIR="/root/finance_app/future-app"
LOG_FILE="${APP_DIR}/update.log"
PYTHON_BIN="${APP_DIR}/venv/bin/python"
FAILED_STEPS=0

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

run_step() {
  local idx="$1"
  local total="$2"
  local title="$3"
  local script="$4"

  echo ">>> [${idx}/${total}] 开始${title}..." >> "${LOG_FILE}"
  "${PYTHON_BIN}" "${script}" >> "${LOG_FILE}" 2>&1
  local rc=$?
  echo "<<< [${idx}/${total}] 结束${title} (rc=${rc})" >> "${LOG_FILE}"
  echo "" >> "${LOG_FILE}"

  if [ ${rc} -ne 0 ]; then
    FAILED_STEPS=$((FAILED_STEPS + 1))
  fi
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

  age_days=$(( (now_epoch - latest_epoch) / 86400 ))
  echo "ℹ️ DXY 状态: source=${dxy_source:-unknown}, backfilled=${dxy_backfilled:-0}, latest=${latest_date}, age_days=${age_days}" >> "${LOG_FILE}"
  if [ ${age_days} -gt 3 ]; then
    echo "⚠️ DXY 新鲜度告警: 最新日期距今 ${age_days} 天(>3天)，请人工检查宏观数据源" >> "${LOG_FILE}"
  fi
}

echo "" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
echo "⏰ 任务开始: $(date)" >> "${LOG_FILE}"
echo "🐍 Python: ${PYTHON_BIN}" >> "${LOG_FILE}"

run_step 1 7 "更新期货席位数据" "update_open_oneday.py"
run_step 2 7 "更新股票资金流数据" "update_stock_moneyflow_daily.py"
run_step 3 7 "更新产业链快照数据" "update_industry_chain_snapshot_daily.py"
run_step 4 7 "更新美股价格数据" "update_stock_tiingo.py"
run_step 5 7 "更新债券收益数据" "update_bond_data.py"
run_step 6 7 "更新热搜数据" "trend_monitor.py"
run_step 7 7 "更新宏观数据" "update_micro_daily.py"
check_dxy_freshness

if [ ${FAILED_STEPS} -gt 0 ]; then
  echo "⚠️ 本次任务失败步骤数: ${FAILED_STEPS}" >> "${LOG_FILE}"
else
  echo "✅ 本次任务全部步骤成功" >> "${LOG_FILE}"
fi

echo "✅ 任务结束: $(date)" >> "${LOG_FILE}"
echo "========================================" >> "${LOG_FILE}"
