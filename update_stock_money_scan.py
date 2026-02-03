import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
import traceback
from dotenv import load_dotenv

# 加载配置
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")

DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, pool_recycle=3600, pool_pre_ping=True)

# =====================================================================
# 评分参数 (集中配置，方便调优)
# =====================================================================
SCORE_CONFIG = {
    # --- 环比量比 (今日/昨日) ---
    'ratio_1d_weight': 40,       # 满分 40
    'ratio_1d_cap': 10,         # 量比 ≥8 得满分

    # --- 均量比 (今日/10日均) ---
    'ratio_10d_weight': 30,      # 满分 30
    'ratio_10d_cap': 6.0,        # 量比 ≥6 得满分

    # --- 成交额 (log10 映射) ---
    'amount_weight': 30,         # 满分 30
    'amount_floor': 500_000,     # 50万元 → 0 分 (统一为"元"后的值)
    'amount_cap': 500_000_000,   # 5亿元 → 满分 30

    # --- 过滤门槛 ---
    'min_amount_yuan': 10_000_000,  # 日成交额 > 50万元
    'min_score': 50,             # 总分 < 30 不入库
}

# =====================================================================
# 评分参考表 (方便理解分数含义)
# =====================================================================
# 环比量比 (40分):  2x→13  3x→21  4x→27  5x→31  8x→40
# 均量比 (30分):    1.5x→5  2x→12  3x→18  4x→23  6x→30
# 成交额 (30分):    500万→10  5000万→20  5亿→30
#
# 举例:
#   环比3x + 均量2x + 成交额5000万  = 21 + 12 + 20 = 53 分
#   环比5x + 均量3x + 成交额3亿     = 31 + 18 + 28 = 77 分
#   环比8x + 均量5x + 成交额5亿+    = 40 + 27 + 30 = 97 分
# =====================================================================


def score_ratio_vec(ratio_series, weight, cap):
    """
    向量化量比评分: log2 映射
    ratio <= 1.0 → 0 分 (无放量)
    ratio >= cap → 满分 weight
    """
    ratio = ratio_series.copy().fillna(1.0)
    # ratio <= 1 的部分 clip 到 1，log2(1)=0 自然得 0 分
    ratio = ratio.clip(lower=1.0)
    scores = weight * (np.log2(ratio) / np.log2(cap))
    return scores.clip(lower=0, upper=weight)


def score_amount_vec(amount_series, weight, floor, cap):
    """
    向量化成交额评分: log10 映射
    amount <= floor → 0 分
    amount >= cap   → 满分 weight
    """
    amount = amount_series.copy().fillna(0)
    log_floor = np.log10(floor)
    log_cap = np.log10(cap)
    log_range = log_cap - log_floor

    # 对 <= 0 的值特殊处理 (避免 log10 报错)
    safe_amount = amount.clip(lower=1)
    scores = weight * (np.log10(safe_amount) - log_floor) / log_range
    return scores.clip(lower=0, upper=weight)


def run_daily_fund_scan():
    cfg = SCORE_CONFIG
    print(f"🚀 开始执行资金流放量评分扫描 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"   评分权重: 环比={cfg['ratio_1d_weight']} 均量比={cfg['ratio_10d_weight']} 成交额={cfg['amount_weight']}")
    print(f"   入库门槛: 成交额>{cfg['min_amount_yuan']/1e4:.0f}万元, 总分>={cfg['min_score']}")

    try:
        # 1. 确定扫描日期范围
        scan_start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        # 2. 拉取全市场数据 (60 天，保证均线计算有足够数据)
        print("\n  📥 正在从数据库拉取股价数据...")
        sql = """
              SELECT trade_date, ts_code, name, close_price, vol, amount, pct_chg
              FROM stock_price
              WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
              """

        with engine.connect() as conn:
            try:
                df = pd.read_sql(sql, conn)
            except Exception as e:
                if "Unknown column 'vol'" in str(e):
                    print("  ⚠️ 字段 vol 不存在，尝试使用 volume...")
                    sql = sql.replace("vol,", "volume,")
                    df = pd.read_sql(sql, conn)
                else:
                    raise e

        if df.empty:
            print("  ❌ 错误：未获取到股价数据，请检查 stock_price 表。")
            return

        print(f"  ✅ 获取 {len(df)} 条记录")

        # 3. 数据清洗
        df = df.rename(columns={
            'ts_code': 'stock_code',
            'name': 'stock_name',
            'close_price': 'close',
            'vol': 'volume'
        })

        df = df.sort_values(['stock_code', 'trade_date'], ascending=[True, True]).reset_index(drop=True)

        # --- 统一 amount 单位为「元」---
        # A 股 (Tushare): amount 单位是「千元」，需要 ×1000
        a_share_mask = df['stock_code'].str.endswith(('.SH', '.SZ'))
        if a_share_mask.any():
            a_median = df.loc[a_share_mask, 'amount'].median()
            print(f"  📊 A股 amount 中位数 (原始): {a_median:,.0f}")
            if a_median < 1_000_000:
                print(f"  🔄 A股 amount 单位为「千元」，×1000 → 「元」")
                df.loc[a_share_mask, 'amount'] = df.loc[a_share_mask, 'amount'] * 1000
            else:
                print(f"  ✅ A股 amount 已经是「元」")

        hk_mask = df['stock_code'].str.endswith('.HK')
        if hk_mask.any():
            hk_median = df.loc[hk_mask, 'amount'].median()
            print(f"  📊 港股 amount 中位数: {hk_median:,.0f}")
            if hk_median < 1_000_000:
                print(f"  🔄 港股 amount 单位为「千元」，×1000 → 「元」")
                df.loc[hk_mask, 'amount'] = df.loc[hk_mask, 'amount'] * 1000

        print(f"  📊 统一后全市场 amount 中位数: {df['amount'].median():,.0f} 元")

        # 4. 核心指标计算
        print("\n  ⚙️ 计算量比指标...")
        grouped = df.groupby('stock_code')

        df['prev_amount'] = grouped['amount'].shift(1)

        # transform 确保 rolling 在分组内计算，不跨股票串数据
        df['ma10_amount'] = grouped['amount'].transform(
            lambda x: x.shift(1).rolling(window=10, min_periods=5).mean()
        )

        # 量比
        df['vol_ratio_1d'] = np.where(
            df['prev_amount'] > 0,
            df['amount'] / df['prev_amount'],
            np.nan
        )
        df['vol_ratio_10d'] = np.where(
            df['ma10_amount'] > 0,
            df['amount'] / df['ma10_amount'],
            np.nan
        )

        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # 5. 筛选最近 N 天
        recent_df = df[df['trade_date'].astype(str) >= scan_start_date].copy()

        valid_ratio = recent_df['vol_ratio_10d'].dropna()
        print(f"  🔍 数据分布 (vol_ratio_10d): "
              f"有效={len(valid_ratio)}, "
              f"中位={valid_ratio.median():.2f}, "
              f"均值={valid_ratio.mean():.2f}")

        # 6. 基础过滤: 上涨 + 放量 + 最低成交额
        # 这一步先大幅缩小计算范围，再对通过的股票评分
        has_ratio_1d = recent_df['vol_ratio_1d'].notna()
        has_ratio_10d = recent_df['vol_ratio_10d'].notna()

        base_filter = (
            (recent_df['pct_chg'] > 0) &                        # 只看上涨
            (recent_df['amount'] > cfg['min_amount_yuan']) &     # 成交额 > 50万元
            (                                                     # 至少一个量比维度有数据且放量
                (has_ratio_1d & (recent_df['vol_ratio_1d'] > 1.0)) |
                (has_ratio_10d & (recent_df['vol_ratio_10d'] > 1.0))
            )
        )
        candidates = recent_df[base_filter].copy()

        if candidates.empty:
            print("  📭 无符合基础条件的股票 (上涨+放量+成交额>50万)。")
            return

        print(f"\n  📊 基础过滤后: {len(candidates)} 条候选")

        # 7. 评分 (向量化，高性能)
        candidates['score_1d'] = score_ratio_vec(
            candidates['vol_ratio_1d'],
            cfg['ratio_1d_weight'],
            cfg['ratio_1d_cap']
        ).round(1)

        candidates['score_10d'] = score_ratio_vec(
            candidates['vol_ratio_10d'],
            cfg['ratio_10d_weight'],
            cfg['ratio_10d_cap']
        ).round(1)

        candidates['score_amount'] = score_amount_vec(
            candidates['amount'],
            cfg['amount_weight'],
            cfg['amount_floor'],
            cfg['amount_cap']
        ).round(1)

        candidates['total_score'] = (
            candidates['score_1d'] + candidates['score_10d'] + candidates['score_amount']
        ).round(1)

        # 8. 按分数过滤
        scored_df = candidates[candidates['total_score'] >= cfg['min_score']].copy()

        if scored_df.empty:
            print(f"  📭 无股票达到最低分 {cfg['min_score']}。")
            # 打印最高分帮助排查
            top_score = candidates['total_score'].max()
            print(f"      (候选中最高分: {top_score:.1f})")
            return

        # 按分数降序
        scored_df = scored_df.sort_values('total_score', ascending=False)

        # 9. 打标签
        def get_label(row):
            labels = []
            r1d = row['vol_ratio_1d'] if pd.notna(row['vol_ratio_1d']) else 1.0
            r10d = row['vol_ratio_10d'] if pd.notna(row['vol_ratio_10d']) else 1.0

            if r1d > 5.0:
                labels.append("极端爆量")
            elif r1d > 3.0:
                labels.append("突发放量")
            elif r1d > 2.0:
                labels.append("显著放量")
            elif r1d > 1.5:
                labels.append("温和放量")

            if r10d > 3.0:
                labels.append("持续抢筹")
            elif r10d > 2.0:
                labels.append("资金关注")
            elif r10d > 1.5:
                labels.append("量能回升")

            return "+".join(labels) if labels else "小幅放量"

        scored_df['abnormal_type'] = scored_df.apply(get_label, axis=1)

        # 10. 入库准备
        save_df = scored_df[[
            'trade_date', 'stock_code', 'stock_name',
            'close', 'pct_chg', 'amount',
            'vol_ratio_1d', 'vol_ratio_10d',
            'score_1d', 'score_10d', 'score_amount', 'total_score',
            'abnormal_type'
        ]].rename(columns={'close': 'close_price'})

        save_df = save_df.fillna(0)
        save_df['trade_date'] = pd.to_datetime(save_df['trade_date']).dt.strftime('%Y-%m-%d')

        # 汇总统计
        print(f"\n  🔍 筛选出 {len(save_df)} 条 (score ≥ {cfg['min_score']}):")
        print(f"      分数分布: "
              f"90+={len(save_df[save_df['total_score'] >= 90])}, "
              f"70~89={len(save_df[(save_df['total_score'] >= 70) & (save_df['total_score'] < 90)])}, "
              f"50~69={len(save_df[(save_df['total_score'] >= 50) & (save_df['total_score'] < 70)])}, "
              f"30~49={len(save_df[(save_df['total_score'] >= 30) & (save_df['total_score'] < 50)])}")

        for suffix, label in [('.SH', '沪市'), ('.SZ', '深市'), ('.HK', '港股')]:
            cnt = save_df['stock_code'].str.endswith(suffix).sum()
            if cnt > 0:
                print(f"      {label}: {cnt} 条")

        # Top 5 预览
        print("\n  🏆 Top 5 预览:")
        for _, row in save_df.head(5).iterrows():
            print(f"      {row['stock_code']} {row['stock_name']} | "
                  f"总分={row['total_score']:.0f} "
                  f"(1d={row['score_1d']:.0f} 10d={row['score_10d']:.0f} amt={row['score_amount']:.0f}) | "
                  f"环比={row['vol_ratio_1d']:.1f}x 均量比={row['vol_ratio_10d']:.1f}x | "
                  f"涨幅={row['pct_chg']:.1f}% | 成交额={row['amount'] / 1e8:.2f}亿")

        # 11. 安全写入
        print(f"\n  💾 开始写入数据库...")
        target_dates = save_df['trade_date'].unique().tolist()

        with engine.connect() as conn:
            trans = conn.begin()
            try:
                for t_date in target_dates:
                    delete_sql = text("DELETE FROM stock_fund_flow_abnormal WHERE trade_date = :d")
                    conn.execute(delete_sql, {"d": t_date})

                    daily_data = save_df[save_df['trade_date'] == t_date]
                    if not daily_data.empty:
                        daily_data.to_sql('stock_fund_flow_abnormal', conn, if_exists='append', index=False)
                        print(f"    ✓ {t_date}: 写入 {len(daily_data)} 条")

                trans.commit()
                print("  ✅ 全部写入完成!")

            except Exception as sql_err:
                trans.rollback()
                print(f"  ❌ 写入失败，已回滚: {sql_err}")
                raise sql_err

    except Exception as e:
        print(f"  ❌ 脚本致命错误: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    run_daily_fund_scan()