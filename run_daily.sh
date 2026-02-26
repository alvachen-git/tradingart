#!/bin/bash

# 1. 进入项目目录 (非常重要！确保找到 .env 文件)
cd /root/finance_app/future-app

# 2. 打印开始时间到日志
echo "" >> update.log
echo "========================================" >> update.log
echo "⏰ 任务开始: $(date)" >> update.log

# 3. 先更新【商品期权基础合约表】(带门禁+原子替换)
echo ">>> [1/3] 开始安全更新商品期权基础表..." >> update.log
/usr/bin/python3 update_commodity_option_basic_safe.py >> update.log 2>&1
if [ $? -ne 0 ]; then
  echo "❌ 安全更新 commodity_option_basic 失败，任务中止: $(date)" >> update.log
  echo "========================================" >> update.log
  exit 1
fi

# 4. 更新【期货数据】
echo ">>> [2/3] 开始更新期货价格数据..." >> update.log
/usr/bin/python3 update_future_price_daily.py >> update.log 2>&1
if [ $? -ne 0 ]; then
  echo "❌ 更新期货价格数据失败，任务中止: $(date)" >> update.log
  echo "========================================" >> update.log
  exit 1
fi

# 1. 更新【期权数据】
echo ">>> [3/3] 开始更新商品期权价格数据..." >> update.log
/usr/bin/python3 update_commodity_opt_daily.py >> update.log 2>&1
if [ $? -ne 0 ]; then
  echo "❌ 更新商品期权价格数据失败，任务中止: $(date)" >> update.log
  echo "========================================" >> update.log
  exit 1
fi


# 7. 结束
echo "✅ 任务结束: $(date)" >> update.log
echo "========================================" >> update.log
