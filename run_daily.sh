#!/bin/bash

# 1. 进入项目目录 (非常重要！确保找到 .env 文件)
cd /root/finance_app/future-app

# 2. 打印开始时间到日志
echo "" >> update.log
echo "========================================" >> update.log
echo "⏰ 任务开始: $(date)" >> update.log

# 4. 更新【期货数据】
echo ">>> [1/2] 开始更新期货价格数据..." >> update.log
/usr/bin/python3 update_future_price_daily.py >> update.log 2>&1


# 1. 更新【期权数据】
echo ">>> [2/2] 开始更新商品期权价格数据..." >> update.log
/usr/bin/python3 update_commodity_opt_daily_old.py >> update.log 2>&1


# 7. 结束
echo "✅ 任务结束: $(date)" >> update.log
echo "========================================" >> update.log