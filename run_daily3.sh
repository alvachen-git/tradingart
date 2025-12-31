#!/bin/bash

# 1. 进入项目目录 (非常重要！确保找到 .env 文件)
cd /root/finance_app/future-app

# 2. 打印开始时间到日志
echo "" >> update.log
echo "========================================" >> update.log
echo "⏰ 任务开始: $(date)" >> update.log


# 1. 更新【期权数据】
echo ">>> [1/5] 开始更新ETF期权数据..." >> update.log
/usr/bin/python3 update_options_daily.py >> update.log 2>&1

echo ">>> [2/5] 开始更新指数数据..." >> update.log
/usr/bin/python3 update_index.py >> update.log 2>&1

# 2. 更新【A股数据】
echo ">>> [3/5] 开始更新股票价格数据..." >> update.log
/usr/bin/python3 update_astock_daily.py >> update.log 2>&1

# 2. 更新【A股数据】
echo ">>> [4/5] 开始更新板块资金流数据..." >> update.log
/usr/bin/python3 update_sector_flow.py >> update.log 2>&1

# 3. 更新【波动率数据】
echo ">>> [5/5] 开始股票期权IV计算..." >> update.log
/usr/bin/python3 calc_iv_oneday.py >> update.log 2>&1



# 7. 结束
echo "✅ 任务结束: $(date)" >> update.log
echo "========================================" >> update.log