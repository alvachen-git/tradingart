#!/bin/bash

# 1. 进入项目目录 (非常重要！确保找到 .env 文件)
cd /root/finance_app/future-app

LOG_FILE="update.log"
FAILED_STEPS=0

log_system_state() {
  local stage="$1"
  echo "----- ${stage} | $(date '+%F %T') -----" >> "$LOG_FILE"
  free -h >> "$LOG_FILE" 2>&1
  echo "[Top RSS]" >> "$LOG_FILE"
  ps -eo pid,cmd,%mem,rss --sort=-rss | head -n 8 >> "$LOG_FILE" 2>&1
}

run_step() {
  local idx="$1"
  local total="$2"
  local title="$3"
  local script="$4"

  echo ">>> [${idx}/${total}] 开始${title}..." >> "$LOG_FILE"
  log_system_state "STEP ${idx} BEFORE ${script}"

  /usr/bin/time -v /usr/bin/python3 "$script" >> "$LOG_FILE" 2>&1
  local rc=$?

  log_system_state "STEP ${idx} AFTER ${script} (rc=${rc})"
  echo "<<< [${idx}/${total}] 结束${title} (rc=${rc})" >> "$LOG_FILE"
  echo "" >> "$LOG_FILE"

  if [ $rc -ne 0 ]; then
    FAILED_STEPS=$((FAILED_STEPS + 1))
  fi
}

# 2. 打印开始时间到日志
echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "⏰ 任务开始: $(date)" >> "$LOG_FILE"
log_system_state "RUN START"


# 1. 更新【期权数据】
run_step 1 8 "更新ETF期权数据" "update_options_daily.py"

run_step 2 8 "更新指数数据" "update_index.py"

# 2. 更新【A股数据】
run_step 3 8 "更新股票价格数据" "update_astock_daily.py"

run_step 4 8 "更新股票财务数据" "update_stock_valuation.py"

run_step 5 8 "更新指数估值数据" "update_index_valuation.py"

run_step 6 8 "更新板块资金流数据" "update_sector_flow.py"

run_step 7 8 "更新股票成交量排名数据" "update_stock_money_scan.py"

# 3. 更新【波动率数据】
run_step 8 8 "股票期权IV计算" "calc_iv_oneday.py"



# 7. 结束
log_system_state "RUN END"
echo "⚠️ 失败步骤数: ${FAILED_STEPS}" >> "$LOG_FILE"
echo "✅ 任务结束: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
