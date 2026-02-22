#!/bin/bash

set -u
set -o pipefail

APP_DIR="/root/finance_app/future-app"
PYTHON_BIN="$APP_DIR/venv/bin/python"
LOG_FILE="$APP_DIR/update.log"

log_line() {
  echo "$1" >> "$LOG_FILE"
}

run_job() {
  local step="$1"
  local script_name="$2"

  log_line ">>> [$step] 开始执行 ${script_name}..."

  if "$PYTHON_BIN" "$script_name" >> "$LOG_FILE" 2>&1; then
    log_line "✅ [$step] ${script_name} 执行成功"
  else
    local code=$?
    log_line "❌ [$step] ${script_name} 执行失败，退出码: ${code}"
    log_line "🛑 任务中断: $(date)"
    log_line "========================================"
    exit "$code"
  fi
}

# 1. 进入项目目录
if ! cd "$APP_DIR"; then
  echo "❌ 无法进入目录: $APP_DIR" >&2
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  log_line "❌ Python解释器不存在或不可执行: $PYTHON_BIN"
  exit 1
fi

# 2. 打印开始时间到日志
log_line ""
log_line "========================================"
log_line "⏰ 任务开始: $(date)"

# 3. 交易日门禁（非交易日或当日数据未就绪时跳过发布）
TODAY=$(date +%Y%m%d)
LATEST_DB_DATE=$("$PYTHON_BIN" -c "from data_engine import get_latest_data_date; d=get_latest_data_date(); s=''.join(ch for ch in str(d) if ch.isdigit())[:8]; print(s)" 2>>"$LOG_FILE" || true)

if [ -z "$LATEST_DB_DATE" ]; then
  log_line "⏭️ 跳过：未能获取数据库最新交易日（门禁保护）"
  log_line "✅ 任务结束: $(date)"
  log_line "========================================"
  exit 0
fi

if [ "$LATEST_DB_DATE" != "$TODAY" ]; then
  log_line "⏭️ 跳过：非交易日或数据未更新（today=$TODAY latest_db=$LATEST_DB_DATE）"
  log_line "✅ 任务结束: $(date)"
  log_line "========================================"
  exit 0
fi

# 4. 顺序执行（任一步失败即中断）
run_job "1/2" "fund_flow_report_generator.py"
run_job "2/2" "broker_position_generator.py"

# 5. 结束
log_line "✅ 任务结束: $(date)"
log_line "========================================"
