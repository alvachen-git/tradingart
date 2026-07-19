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
  shift 4

  echo ">>> [${idx}/${total}] 开始${title}: ${script} $*..." >> "$LOG_FILE"
  log_system_state "STEP ${idx} BEFORE ${script} $*"

  /usr/bin/time -v /usr/bin/python3 "$script" "$@" >> "$LOG_FILE" 2>&1
  local rc=$?

  log_system_state "STEP ${idx} AFTER ${script} $* (rc=${rc})"
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
run_step 1 11 "更新ETF期权数据" "update_options_daily.py"

run_step 2 11 "更新指数数据" "update_index.py"

# 2. 更新【A股数据】
run_step 3 11 "更新股票价格数据" "update_astock_daily.py"

run_step 4 11 "更新股票财务数据" "update_stock_valuation.py"

run_step 5 11 "更新指数估值数据" "update_index_valuation.py"

run_step 6 11 "更新板块资金流数据" "update_sector_flow.py"

run_step 7 11 "更新行业板块价格数据" "update_sector_index_price.py" "--lookback-days" "10"

run_step 8 11 "更新股票成交量排名数据" "update_stock_money_scan.py"

# 3. 更新【波动率数据】
run_step 9 11 "股票期权IV计算" "calc_iv_oneday.py"

# 4. 全市场股票/ETF：补齐前复权价格；日结由 run_ai_simulation_weekday_2030.sh 执行
run_step 10 11 "更新全市场股票ETF前复权价格" "update_stock_price_qfq.py" "--date" "latest" "--lookback-days" "420" "--symbols" "510050.SH,510300.SH,510500.SH,159915.SZ,588000.SH" "--portfolio-id" "official_cn_a_etf_v3" "--v3-daily-candidates" "--all-stock-price-symbols" "--asset-scope" "stock_etf" "--only-missing-or-stale" "--sleep-sec" "0.05"

# 5. ETF期权页市场环境：放在A股、指数、估值和前复权数据完成之后
run_step 11 11 "更新ETF期权市场环境指标" "update_cn_market_climate_daily.py"


# 7. 结束
log_system_state "RUN END"
echo "⚠️ 失败步骤数: ${FAILED_STEPS}" >> "$LOG_FILE"
echo "✅ 任务结束: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
