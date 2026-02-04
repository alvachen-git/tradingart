#!/bin/bash

# 1. 进入项目目录
cd /root/finance_app/future-app

# 2. 打印开始时间到日志
echo "" >> update.log
echo "========================================" >> update.log
echo "⏰ 任务开始: $(date)" >> update.log

# ---------------------------------------------------------
# [核心修改点]：使用 venv 里的 python，而不是 /usr/bin/python3
# ---------------------------------------------------------
echo ">>> [1/1] 开始更新资金流晚报..." >> update.log

# 方法 A：直接调用虚拟环境的解释器 (推荐，最稳妥)
/root/finance_app/future-app/venv/bin/python fund_flow_report_generator.py >> update.log 2>&1

# 或者 方法 B：先激活环境再运行 (备选)
# source venv/bin/activate
# python daily_report_generator.py >> update.log 2>&1
# ---------------------------------------------------------

# 结束
echo "✅ 任务结束: $(date)" >> update.log
echo "========================================" >> update.log