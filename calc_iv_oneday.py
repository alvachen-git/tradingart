import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from py_vollib_vectorized import vectorized_implied_volatility
from datetime import datetime
import time
import gc
from tqdm import tqdm

# 初始化
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER") or 'root'
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT") or '3306'
DB_NAME = os.getenv("DB_NAME") or 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_recycle=3600)


class IVCalculator:
    """
    可调参数的IV计算器

    所有参数都可以自定义，方便测试和调优
    """

    def __init__(
            self,
            risk_free_rate=0.020,  # 🔧 无风险利率（2.0%）
            atm_range_pct=0.02,  # 🔧 ATM范围（±2%）
            atm_range_fallback=0.05,  # 🔧 ATM范围备用（±5%）
            min_volume=200,  # 🔧 最小成交量（200手）
            min_time_to_expiry=0.04,  # 🔧 最小到期时间（14天）
            iqr_multiplier=1.0,  # 🔧 异常值过滤强度（1.0倍IQR）
            iv_min=5.0,  # 🔧 IV最小值（5%）
            iv_max=100.0,  # 🔧 IV最大值（100%）
            use_weighted_avg=False,  # 🔧 是否使用成交量加权
            atm_method='percentage'  # 🔧 ATM选择方法: 'percentage' 或 'nearest'
    ):
        self.r = risk_free_rate
        self.atm_range_pct = atm_range_pct
        self.atm_range_fallback = atm_range_fallback
        self.min_volume = min_volume
        self.min_T = min_time_to_expiry
        self.iqr_mult = iqr_multiplier
        self.iv_min = iv_min
        self.iv_max = iv_max
        self.use_weighted = use_weighted_avg
        self.atm_method = atm_method

        print(f"\n{'=' * 60}")
        print("IV计算器参数设置")
        print(f"{'=' * 60}")
        print(f"无风险利率:       {self.r * 100:.2f}%")
        print(f"ATM范围:          ±{self.atm_range_pct * 100:.1f}%")
        print(f"最小成交量:       {self.min_volume} 手")
        print(f"最小到期时间:     {self.min_T * 365:.0f} 天")
        print(f"异常值过滤:       {self.iqr_mult} 倍IQR")
        print(f"IV合理范围:       {self.iv_min:.0f}% - {self.iv_max:.0f}%")
        print(f"成交量加权:       {'是' if self.use_weighted else '否'}")
        print(f"ATM选择方法:      {self.atm_method}")
        print(f"{'=' * 60}\n")

    def select_atm_options(self, df_opt, S):
        """
        选择ATM期权

        参数:
            df_opt: 期权数据
            S: 标的价格
        """
        df_opt['diff'] = abs(df_opt['strike'] - S)
        df_opt['diff_pct'] = df_opt['diff'] / S

        if self.atm_method == 'nearest':
            # 方法1: 只选最接近的行权价
            min_diff = df_opt['diff'].min()
            atm_opts = df_opt[df_opt['diff'] <= min_diff + 0.01].copy()

        else:  # 'percentage'
            # 方法2: 按百分比范围选择
            atm_opts = df_opt[df_opt['diff_pct'] <= self.atm_range_pct].copy()

            if atm_opts.empty and self.atm_range_fallback:
                # 备用范围
                atm_opts = df_opt[df_opt['diff_pct'] <= self.atm_range_fallback].copy()

        return atm_opts

    def filter_by_expiry(self, atm_opts, date):
        """过滤到期时间太短的期权"""
        atm_opts['T'] = (pd.to_datetime(atm_opts['delist_date']) -
                         pd.to_datetime(date)).dt.days / 365.0
        return atm_opts[atm_opts['T'] > self.min_T].copy()

    def calculate_iv_for_options(self, options, S):
        """
        计算一组期权的IV

        参数:
            options: 期权数据（Call或Put）
            S: 标的价格
        """
        if options.empty:
            return np.array([]), np.array([])

        try:
            option_type = options['call_put'].iloc[0].lower()

            ivs = vectorized_implied_volatility(
                options['price'].values,
                S,
                options['strike'].values,
                options['T'].values,
                self.r,
                np.array([option_type] * len(options)),
                return_as='numpy'
            )

            # 基本过滤
            valid_mask = ~np.isnan(ivs) & (ivs > 0) & (ivs < 2)
            valid_ivs = ivs[valid_mask]
            valid_vols = options['vol'].values[valid_mask] if self.use_weighted else None

            return valid_ivs, valid_vols

        except Exception as e:
            return np.array([]), np.array([])

    def remove_outliers(self, ivs, volumes=None):
        """
        去除异常值

        参数:
            ivs: IV数组
            volumes: 成交量数组（用于加权）
        """
        if len(ivs) < 3:
            return ivs, volumes

        # 使用四分位数方法
        q1 = np.percentile(ivs, 25)
        q3 = np.percentile(ivs, 75)
        iqr = q3 - q1

        lower_bound = max(0.05, q1 - self.iqr_mult * iqr)
        upper_bound = min(2.0, q3 + self.iqr_mult * iqr)

        mask = (ivs >= lower_bound) & (ivs <= upper_bound)

        if volumes is not None:
            return ivs[mask], volumes[mask]
        else:
            return ivs[mask], None

    def calculate_average_iv(self, ivs, volumes=None):
        """
        计算平均IV

        参数:
            ivs: IV数组
            volumes: 成交量数组（用于加权）
        """
        if len(ivs) == 0:
            return None

        if self.use_weighted and volumes is not None and len(volumes) > 0:
            # 成交量加权平均
            avg_iv = np.average(ivs, weights=volumes) * 100
        else:
            # 简单平均
            avg_iv = np.mean(ivs) * 100

        # 合理性检查
        if self.iv_min < avg_iv < self.iv_max:
            return avg_iv
        else:
            return None

    def calculate_one_day(self, etf_code, date, S, hv_val):
        """
        计算某一天的IV

        返回: (iv, 失败原因) 或 (None, 失败原因)
        """
        # 1. 获取期权数据
        sql_opt = f"""
            SELECT d.close as price, d.vol,
                   b.exercise_price as strike, b.call_put, b.delist_date
            FROM option_daily d
            JOIN option_basic b ON d.ts_code = b.ts_code
            WHERE b.underlying = '{etf_code}' 
              AND d.trade_date = '{date}'
              AND d.vol > {self.min_volume}
        """

        try:
            df_opt = pd.read_sql(sql_opt, engine)
        except Exception as e:
            return None, f"SQL错误: {str(e)[:50]}"

        if df_opt.empty:
            # 尝试降低成交量要求
            sql_opt = sql_opt.replace(f"d.vol > {self.min_volume}",
                                      f"d.vol > {self.min_volume // 2}")
            df_opt = pd.read_sql(sql_opt, engine)

            if df_opt.empty:
                return None, "无期权数据"

        # 2. 选择ATM期权
        atm_opts = self.select_atm_options(df_opt, S)

        if atm_opts.empty:
            return None, "无ATM合约"

        # 3. 过滤到期时间
        atm_opts = self.filter_by_expiry(atm_opts, date)

        if atm_opts.empty:
            return None, "到期时间太短"

        # 4. 分别计算Call和Put的IV
        call_opts = atm_opts[atm_opts['call_put'].str.lower() == 'c'].copy()
        put_opts = atm_opts[atm_opts['call_put'].str.lower() == 'p'].copy()

        all_ivs = []
        all_volumes = []

        # 计算Call IV
        if not call_opts.empty:
            call_ivs, call_vols = self.calculate_iv_for_options(call_opts, S)
            if len(call_ivs) > 0:
                all_ivs.extend(call_ivs)
                if call_vols is not None:
                    all_volumes.extend(call_vols)

        # 计算Put IV
        if not put_opts.empty:
            put_ivs, put_vols = self.calculate_iv_for_options(put_opts, S)
            if len(put_ivs) > 0:
                all_ivs.extend(put_ivs)
                if put_vols is not None:
                    all_volumes.extend(put_vols)

        if len(all_ivs) == 0:
            return None, "IV计算失败"

        # 5. 去除异常值
        all_ivs = np.array(all_ivs)
        all_volumes = np.array(all_volumes) if self.use_weighted and len(all_volumes) > 0 else None

        all_ivs, all_volumes = self.remove_outliers(all_ivs, all_volumes)

        if len(all_ivs) == 0:
            return None, "异常值过滤后无数据"

        # 6. 计算平均IV
        avg_iv = self.calculate_average_iv(all_ivs, all_volumes)

        if avg_iv is None:
            return None, f"IV超出合理范围"

        return avg_iv, None

    def calculate_history(self, etf_code="510050.SH", days=100, skip_existing=True):
        """
        计算历史IV
        """
        print(f"\n{'=' * 60}")
        print(f"开始计算 {etf_code} 过去 {days} 天的IV数据")
        print(f"{'=' * 60}\n")

        # A. 获取ETF价格数据
        sql_stock = f"""
            SELECT trade_date, close_price 
            FROM stock_price 
            WHERE ts_code='{etf_code}' 
            ORDER BY trade_date DESC 
            LIMIT {days + 20}
        """

        try:
            df_stock = pd.read_sql(sql_stock, engine).sort_values('trade_date')
        except Exception as e:
            print(f"❌ 数据库读取失败: {e}")
            return

        if df_stock.empty:
            print(f"❌ 无价格数据")
            return

        print(f"✅ 获取到 {len(df_stock)} 天的价格数据")

        # B. 计算HV
        df_stock['log_ret'] = np.log(df_stock['close_price'] / df_stock['close_price'].shift(1))
        df_stock['hv'] = df_stock['log_ret'].rolling(window=20).std() * np.sqrt(252) * 100

        # C. 确定计算日期
        df_stock = df_stock.tail(days).copy()
        df_stock = df_stock.dropna(subset=['hv'])

        print(f"✅ 实际可计算 {len(df_stock)} 天的数据")

        # D. 断点续传
        if skip_existing:
            print("\n🔍 检查数据库已有数据...")
            sql_existing = f"""
                SELECT DISTINCT trade_date 
                FROM etf_iv_history 
                WHERE etf_code = '{etf_code}'
            """
            try:
                df_existing = pd.read_sql(sql_existing, engine)
                existing_dates = set(df_existing['trade_date'].astype(str))
                df_stock = df_stock[~df_stock['trade_date'].isin(existing_dates)]
                print(f"   数据库已有 {len(existing_dates)} 天的数据")
                print(f"   需要新计算 {len(df_stock)} 天")
            except Exception as e:
                print(f"   ⚠️  检查失败: {e}")

        if df_stock.empty:
            print("\n✅ 所有日期的IV数据都已存在！")
            return

        # E. 循环计算IV
        print(f"\n📊 开始计算IV...")
        iv_results = []
        failed_dates = []

        for idx, row in tqdm(df_stock.iterrows(), total=len(df_stock), desc=f"{etf_code}"):
            date = row['trade_date']
            S = row['close_price']
            hv_val = row['hv']

            iv, error = self.calculate_one_day(etf_code, date, S, hv_val)

            if iv is not None:
                iv_results.append({
                    'trade_date': date,
                    'etf_code': etf_code,
                    'iv': iv,
                    'hv': hv_val
                })
            else:
                failed_dates.append((date, error))

            gc.collect()

        # F. 入库
        print(f"\n💾 数据入库...")

        if iv_results:
            df_res = pd.DataFrame(iv_results)

            try:
                with engine.begin() as conn:
                    # 删除旧数据
                    dates_to_delete = df_res['trade_date'].tolist()

                    if dates_to_delete:
                        date_list = ','.join([f"'{d}'" for d in dates_to_delete])
                        del_sql = text(f"""
                            DELETE FROM etf_iv_history
                            WHERE etf_code = :etf_code
                              AND trade_date IN ({date_list})
                        """)
                        result = conn.execute(del_sql, {"etf_code": etf_code})
                        if result.rowcount > 0:
                            print(f"   🗑️  删除 {result.rowcount} 条旧数据")

                    df_res.to_sql('etf_iv_history', conn, if_exists='append', index=False)

                print(f"   ✅ 成功入库 {len(df_res)} 条数据")
                print(f"   IV 范围: {df_res['iv'].min():.2f}% ~ {df_res['iv'].max():.2f}%")
                print(f"   IV 均值: {df_res['iv'].mean():.2f}%")
                print(f"   IV 中位数: {df_res['iv'].median():.2f}%")

            except Exception as e:
                print(f"   ❌ 数据库写入失败: {e}")
        else:
            print(f"   ⚠️  无有效数据可入库")

        # G. 失败日期汇总
        if failed_dates:
            print(f"\n⚠️  {len(failed_dates)} 个日期计算失败:")
            for date, reason in failed_dates[:10]:
                print(f"   - {date}: {reason}")
            if len(failed_dates) > 10:
                print(f"   ... 还有 {len(failed_dates) - 10} 个")

        print(f"\n{'=' * 60}")
        print(f"{etf_code} 计算完成！")
        print(f"{'=' * 60}\n")


if __name__ == "__main__":
    # ============================================================
    # 参数调整区域 - 根据需要修改这里的参数
    # ============================================================

    # 方案1: 推荐配置（针对你的情况优化）⭐
    calculator = IVCalculator(
        risk_free_rate=0.03,  # 2.0% (从1.5%提高)
        atm_range_pct=0.05,  # ±2%
        atm_range_fallback=0.1,  # 备用±5%
        min_volume=0,  # 200手 (从100提高)
        min_time_to_expiry=0.02,  # 14天 (从7天提高) ← 关键改动
        iqr_multiplier=0.8,  # 1.0倍IQR (从1.5降低)
        iv_min=5.0,  # 5%
        iv_max=100.0,  # 100%
        use_weighted_avg=False,  # 不使用加权
        atm_method='percentage'  # 按百分比选择
    )

    # 方案2: 保守配置（最严格，IV最低最平滑）
    # calculator = IVCalculator(
    #     risk_free_rate=0.025,       # 2.5%
    #     atm_range_pct=0.01,          # ±1%
    #     atm_range_fallback=0.02,     # 备用±2%
    #     min_volume=300,              # 300手
    #     min_time_to_expiry=0.08,     # 30天
    #     iqr_multiplier=0.5,          # 0.5倍IQR
    #     iv_min=8.0,                  # 8%
    #     iv_max=50.0,                 # 50%
    #     use_weighted_avg=True,       # 使用加权
    #     atm_method='nearest'         # 只选最近的
    # )

    # 方案3: 宽松配置（样本多，但可能有噪音）
    # calculator = IVCalculator(
    #     risk_free_rate=0.015,       # 1.5%
    #     atm_range_pct=0.05,          # ±5%
    #     atm_range_fallback=None,     # 无备用
    #     min_volume=50,               # 50手
    #     min_time_to_expiry=0.02,     # 7天
    #     iqr_multiplier=1.5,          # 1.5倍IQR
    #     iv_min=3.0,                  # 3%
    #     iv_max=150.0,                # 150%
    #     use_weighted_avg=False,
    #     atm_method='percentage'
    # )

    # 执行计算
    calculator.calculate_history("510050.SH", days=1, skip_existing=False)
    calculator.calculate_history("510300.SH", days=1, skip_existing=False)
    calculator.calculate_history("510500.SH", days=1, skip_existing=False)
    calculator.calculate_history("588000.SH", days=1, skip_existing=False)
    calculator.calculate_history("159915.SZ", days=1, skip_existing=False)