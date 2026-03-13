#!/usr/bin/env bash
set -euo pipefail

# Cron (Asia/Shanghai, Mon-Fri 20:30):
# 30 20 * * 1-5 cd /Users/alvachen/aiproject/tradingart && /bin/bash /Users/alvachen/aiproject/tradingart/run_ai_simulation_weekday_2030.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export TZ=Asia/Shanghai

LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/ai_simulation_daily.log"

{
  echo ""
  echo "========================================"
  echo "⏰ AI模拟盘任务开始: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  ./.venv311/bin/python run_ai_simulation_daily.py
  echo "✅ AI模拟盘任务结束: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "========================================"
} >> "${LOG_FILE}" 2>&1
