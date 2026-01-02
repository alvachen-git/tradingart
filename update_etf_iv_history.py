import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from py_vollib_vectorized import vectorized_implied_volatility
from datetime import datetime, timedelta
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
    """

    def __init__(
            self,
            risk_free_rate=0.020,  # 🔧 无风险利率
            atm_range_pct=0.02,  # 🔧 ATM范围
            atm_range_fallback=0.05,  # 🔧 ATM范围备用
            min_volume=200,  # 🔧 最小成交量
            min_time_to_expiry=0.04,  # 🔧 最小到期时间
            iqr_multiplier=1.0,  # 🔧 异常值过滤强度
            iv_min=5.0,  # 🔧 IV最小值
            iv_max=100.0,  # 🔧 IV最大值
            use_weighted_avg=False,  # 🔧 是否使用成交量加权
            atm_method='percentage'  # 🔧 ATM选择方法
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
        """选择ATM期权"""
        df_opt['diff'] = abs(df_opt['strike'] - S)
        df_opt['diff_pct'] = df_opt['diff'] / S

        if self.atm_method == 'nearest':
            min_diff = df_opt['diff'].min()
            atm_opts = df_opt[df_opt['diff'] <= min_diff + 0.01].copy()
        else:
            atm_opts = df_opt[df_opt['diff_pct'] <= self.atm_range_pct].copy()
            if atm_opts.empty and self.atm_range_fallback:
                atm_opts = df_opt[df_opt['diff_pct'] <= self.atm_range_fallback].copy()
        return atm_opts

    def filter_by_expiry(self, atm_opts, date):
        """过滤到期时间"""
        atm_opts['T'] = (pd.to_datetime(atm_opts['delist_date']) -
                         pd.to_datetime(date)).dt.days / 365.0
        return atm_opts[atm_opts['T'] > self.min_T].copy()

    def calculate_iv_for_options(self, options, S):
        """计算IV核心函数"""
        if options.empty: return np.array([]), np.array([])
        try:
            option_type = options['call_put'].iloc[0].lower()
            ivs = vectorized_implied_volatility(
                options['price'].values, S, options['strike'].values, options['T'].values,
                self.r, np.array([option_type] * len(options)), return_as='numpy'
            )
            valid_mask = ~np.isnan(ivs) & (ivs > 0) & (ivs < 2)
            valid_ivs = ivs[valid_mask]
            valid_vols = options['vol'].values[valid_mask] if self.use_weighted else None
            return valid_ivs, valid_vols
        except:
            return np.array([]), np.array([])

    def remove_outliers(self, ivs, volumes=None):
        """去除异常值"""
        if len(ivs) < 3: return ivs, volumes
        q1 = np.percentile(ivs, 25)
        q3 = np.percentile(ivs, 75)
        iqr = q3 - q1
        lower_bound = max(0.05, q1 - self.iqr_mult * iqr)
        upper_bound = min(2.0, q3 + self.iqr_mult * iqr)
        mask = (ivs >= lower_bound) & (ivs <= upper_bound)
        if volumes is not None:
            return ivs[mask], volumes[mask]
        return ivs[mask], None

    def calculate_average_iv(self, ivs, volumes=None):
        """计算平均IV"""
        if len(ivs) == 0: return None
        if self.use_weighted and volumes is not None and len(volumes) > 0:
            avg_iv = np.average(ivs, weights=volumes) * 100
        else:
            avg_iv = np.mean(ivs) * 100
        if self.iv_min < avg_iv < self.iv_max: return avg_iv
        return None

    def calculate_one_day(self, etf_code, date, S, hv_val):
        """计算单日IV"""
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
            return None, f"SQL错误"

        if df_opt.empty:
            sql_opt = sql_opt.replace(f"d.vol > {self.min_volume}", f"d.vol > {self.min_volume // 2}")
            df_opt = pd.read_sql(sql_opt, engine)
            if df_opt.empty: return None, "无期权数据"

        atm_opts = self.select_atm_options(df_opt, S)
        if atm_opts.empty: return None, "无ATM合约"

        atm_opts = self.filter_by_expiry(atm_opts, date)
        if atm_opts.empty: return None, "到期时间太短"

        call_opts = atm_opts[atm_opts['call_put'].str.lower() == 'c'].copy()
        put_opts = atm_opts[atm_opts['call_put'].str.lower() == 'p'].copy()

        all_ivs = []
        all_volumes = []

        if not call_opts.empty:
            ivs, vols = self.calculate_iv_for_options(call_opts, S)
            all_ivs.extend(ivs)
            if vols is not None: all_volumes.extend(vols)

        if not put_opts.empty:
            ivs, vols = self.calculate_iv_for_options(put_opts, S)
            all_ivs.extend(ivs)
            if vols is not None: all_volumes.extend(vols)

        if len(all_ivs) == 0: return None, "IV计算失败"

        all_ivs = np.array(all_ivs)
        all_volumes = np.array(all_volumes) if self.use_weighted and len(all_volumes) > 0 else None
        all_ivs, all_volumes = self.remove_outliers(all_ivs, all_volumes)

        if len(all_ivs) == 0: return None, "异常值过滤后无数据"

        avg_iv = self.calculate_average_iv(all_ivs, all_volumes)
        if avg_iv is None: return None, f"IV超出合理范围"

        return avg_iv, None

    def calculate_period(self, etf_code, start_date, end_date, skip_existing=True):
        """
        【修改版】计算指定区间的IV
        """
        print(f"\n{'=' * 60}")
        print(f"开始计算 {etf_code} 区间IV数据: {start_date} 至 {end_date}")
        print(f"{'=' * 60}\n")

        # 1. 计算缓冲日期 (为了计算HV，需要往前多取 60 天)
        try:
            s_date_obj = datetime.strptime(str(start_date), "%Y%m%d")
            buffer_date_str = (s_date_obj - timedelta(days=60)).strftime("%Y%m%d")
        except ValueError:
            print("❌ 日期格式错误，请使用 YYYYMMDD")
            return

        # A. 获取ETF价格数据 (带缓冲)
        sql_stock = f"""
            SELECT trade_date, close_price 
            FROM stock_price 
            WHERE ts_code='{etf_code}' 
              AND trade_date >= '{buffer_date_str}'
              AND trade_date <= '{end_date}'
            ORDER BY trade_date ASC 
        """

        try:
            df_stock = pd.read_sql(sql_stock, engine)
        except Exception as e:
            print(f"❌ 数据库读取失败: {e}")
            return

        if df_stock.empty:
            print(f"❌ 查无价格数据")
            return

        # B. 计算HV (Rolling Window)
        df_stock['log_ret'] = np.log(df_stock['close_price'] / df_stock['close_price'].shift(1))
        df_stock['hv'] = df_stock['log_ret'].rolling(window=20).std() * np.sqrt(252) * 100

        # C. 截取目标区间 (去掉缓冲数据)
        # 将 trade_date 统一转为字符串进行比较
        df_stock['trade_date'] = df_stock['trade_date'].astype(str)
        df_target = df_stock[df_stock['trade_date'] >= str(start_date)].copy()
        df_target = df_target.dropna(subset=['hv'])

        print(f"✅ 实际需处理 {len(df_target)} 天的数据 (HV已就绪)")

        # D. 断点续传 (跳过已有)
        if skip_existing:
            print("🔍 检查数据库已有数据...")
            sql_existing = f"""
                SELECT DISTINCT trade_date 
                FROM etf_iv_history 
                WHERE etf_code = '{etf_code}'
                  AND trade_date >= '{start_date}'
                  AND trade_date <= '{end_date}'
            """
            try:
                df_existing = pd.read_sql(sql_existing, engine)
                existing_dates = set(df_existing['trade_date'].astype(str))
                df_target = df_target[~df_target['trade_date'].isin(existing_dates)]
                print(f"   已存在 {len(existing_dates)} 天，跳过。")
                print(f"   剩余 {len(df_target)} 天待计算。")
            except Exception as e:
                print(f"   ⚠️  检查失败: {e}")

        if df_target.empty:
            print("✅ 目标区间数据已全部存在！")
            return

        # E. 循环计算IV
        print(f"\n📊 开始逐日计算IV...")
        iv_results = []
        failed_dates = []

        for idx, row in tqdm(df_target.iterrows(), total=len(df_target), desc=f"{etf_code}"):
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

            # 释放内存
            gc.collect()

        # F. 入库
        if iv_results:
            print(f"\n💾 正在入库 {len(iv_results)} 条数据...")
            df_res = pd.DataFrame(iv_results)
            try:
                with engine.begin() as conn:
                    # 幂等性：删除区间内旧数据 (只删除我们要插入的日期)
                    dates_to_delete = df_res['trade_date'].tolist()
                    if dates_to_delete:
                        date_list = ','.join([f"'{d}'" for d in dates_to_delete])
                        del_sql = text(f"""
                            DELETE FROM etf_iv_history
                            WHERE etf_code = :etf_code
                              AND trade_date IN ({date_list})
                        """)
                        conn.execute(del_sql, {"etf_code": etf_code})

                    df_res.to_sql('etf_iv_history', conn, if_exists='append', index=False)

                print(f"   ✅ 入库成功！IV均值: {df_res['iv'].mean():.2f}%")
            except Exception as e:
                print(f"   ❌ 数据库写入失败: {e}")
        else:
            print(f"   ⚠️  本次无有效数据产生")

        # G. 失败汇总
        if failed_dates:
            print(f"\n⚠️  {len(failed_dates)} 个日期计算失败 (前5个):")
            for date, reason in failed_dates[:5]:
                print(f"   - {date}: {reason}")

        print(f"\n{'=' * 60}")
        print(f"{etf_code} 任务完成")
        print(f"{'=' * 60}\n")


if __name__ == "__main__":
    # ============================================================
    # 🔴 配置区域：指定时间区间
    # ============================================================

    START_DATE = "20240101"
    END_DATE = "20250127"
    SKIP_EXISTING = False  # True=跳过已算过的日期, False=强制重算覆盖

    print(f"🚀 启动区间IV计算任务: {START_DATE} -> {END_DATE}\n")

    # 参数方案 (推荐)
    calculator = IVCalculator(
        risk_free_rate=0.03,
        atm_range_pct=0.04,
        atm_range_fallback=0.1,
        min_volume=10,  # 允许低成交量
        min_time_to_expiry=0.04,  # 14天
        iqr_multiplier=0.06,
        iv_min=5.0,
        iv_max=100.0,
        use_weighted_avg=False,
        atm_method='percentage'
    )

    # 目标 ETF 列表
    TARGET_ETFS = [
        "510050.SH"
    ]

    for etf in TARGET_ETFS:
        calculator.calculate_period(
            etf_code=etf,
            start_date=START_DATE,
            end_date=END_DATE,
            skip_existing=SKIP_EXISTING
        )