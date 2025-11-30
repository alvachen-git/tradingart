#!/bin/bash

# 1. 进入项目目录 (非常重要！确保找到 .env 文件)
cd /root/finance_app/future-app

# 2. 打印开始时间到日志
echo "" >> update.log
echo "========================================" >> update.log
echo "⏰ 任务开始: $(date)" >> update.log


# 使用 python3 运行，将输出追加到 update.log，错误也追加到 update.log
echo ">>> [1/2] 开始更新期货席位数据..." >> update.log
/usr/bin/python3 update_open_oneday.py >> update.log 2>&1

echo ">>> [2/2] 开始更新美股价格数据..." >> update.log
/usr/bin/python3 update_stock_tiingo.py >> update.log 2>&1

# 7. 结束
echo "✅ 任务结束: $(date)" >> update.log
echo "========================================" >> update.log