import pandas as pd
import numpy as np
import os
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
from pydantic import BaseModel, Field
import symbol_map

# =====================================================================
# 数据库连接
# =====================================================================
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=7200,
        pool_size=5,
        max_overflow=10
    )


engine = get_db_engine()


# =====================================================================
# 工具 1: query_stock_volume
# =====================================================================

class StockVolumeInput(BaseModel):
    stock_name: str = Field(
        description="股票名称或代码，如'茅台'、'600519'"
    )
    days: int = Field(
        default=5,
        description="查看天数，默认5天"
    )


@tool(args_schema=StockVolumeInput)
def query_stock_volume(stock_name: str, days: int = 5):
    """
    单只股票成交量分析
    功能：查询指定股票的成交量趋势（含环比量比、10日均量比、放量/缩量信号）
    示例："茅台成交量怎么样"、"600519最近放量了吗"、"比亚迪量能分析"
    """
    if engine is None:
        return "❌ 数据库未连接"

    # 1. 解析股票代码
    symbol_code, asset_type = symbol_map.resolve_symbol(stock_name)
    if not symbol_code:
        return f"⚠️ 未找到股票: {stock_name}"

    if asset_type != 'stock':
        return f"⚠️ {stock_name} ({symbol_code}) 不是股票，本工具仅支持 A 股/港股"

    target_code = symbol_code.upper()

    try:
        # 2. 查询数据
        buffer_days = days + 20

        if target_code.endswith(('.HK',)):
            code_filter = "ts_code = :code"
            params = {"code": target_code, "buffer": buffer_days}
        elif '.' in target_code:
            code_filter = "ts_code = :code"
            params = {"code": target_code, "buffer": buffer_days}
        else:
            code_filter = "ts_code IN (:code_sz, :code_sh)"
            params = {
                "code_sz": f"{target_code}.SZ",
                "code_sh": f"{target_code}.SH",
                "buffer": buffer_days
            }

        sql = text(f"""
            SELECT trade_date, ts_code, name, close_price, amount, pct_chg
            FROM stock_price
            WHERE {code_filter}
              AND trade_date >= DATE_SUB(CURDATE(), INTERVAL :buffer DAY)
            ORDER BY trade_date ASC
        """)

        df = pd.read_sql(sql, engine, params=params)

        if df.empty:
            return f"⚠️ 未查到 {stock_name} ({target_code}) 的交易数据"

        actual_name = df.iloc[-1]['name'] if 'name' in df.columns else stock_name
        actual_code = df.iloc[-1]['ts_code']

        # 3. 单位统一
        is_a_share = actual_code.endswith(('.SH', '.SZ'))
        if is_a_share and df['amount'].median() < 1_000_000:
            df['amount'] = df['amount'] * 1000

        # 4. 计算量比指标
        df['prev_amount'] = df['amount'].shift(1)
        df['ma10_amount'] = df['amount'].shift(1).rolling(window=10, min_periods=5).mean()
        df['ma5_amount'] = df['amount'].shift(1).rolling(window=5, min_periods=3).mean()

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

        # 5. 截取最近 N 天
        recent = df.tail(days).copy()

        if recent.empty:
            return f"⚠️ {actual_name} 近期无交易数据"

        # 6. 构建输出
        result_lines = [
            f"📊 **{actual_name} ({actual_code}) 近 {len(recent)} 日成交量变化**\n",
            "| 日期 | 收盘价 | 涨跌幅 | 成交额 | 环比量比 | 10日均量比 | 信号 |",
            "|------|--------|--------|--------|----------|-----------|------|",
        ]

        for _, row in recent.iterrows():
            date_str = str(row['trade_date'])[:10]
            close = row['close_price']
            pct = row['pct_chg']
            amt = row['amount']
            r1d = row['vol_ratio_1d']
            r10d = row['vol_ratio_10d']

            if amt >= 1e8:
                amt_str = f"{amt / 1e8:.2f}亿"
            elif amt >= 1e4:
                amt_str = f"{amt / 1e4:.0f}万"
            else:
                amt_str = f"{amt:.0f}"

            r1d_str = f"{r1d:.2f}x" if pd.notna(r1d) else "-"
            r10d_str = f"{r10d:.2f}x" if pd.notna(r10d) else "-"

            signal = ""
            if pd.notna(r1d) and r1d > 3.0:
                signal = "🔴爆量"
            elif pd.notna(r1d) and r1d > 2.0:
                signal = "🟠放量"
            elif pd.notna(r10d) and r10d > 2.0:
                signal = "🟡持续放量"
            elif pd.notna(r10d) and r10d < 0.5:
                signal = "🔵缩量"
            elif pd.notna(r1d) and r1d < 0.5:
                signal = "🔵骤缩"

            pct_str = f"{pct:+.2f}%" if pd.notna(pct) else "-"

            result_lines.append(
                f"| {date_str} | {close} | {pct_str} | {amt_str} | {r1d_str} | {r10d_str} | {signal} |"
            )

        # 7. 汇总判断
        latest = recent.iloc[-1]
        r1d_latest = latest['vol_ratio_1d']
        r10d_latest = latest['vol_ratio_10d']

        summary = "\n**📋 量能综合判断：**"
        if pd.notna(r10d_latest):
            if r10d_latest > 2.5:
                summary += f"\n- 当前量能是 10 日均量的 {r10d_latest:.1f} 倍，属于**显著放量**，主力资金活跃"
            elif r10d_latest > 1.5:
                summary += f"\n- 当前量能是 10 日均量的 {r10d_latest:.1f} 倍，量能**温和放大**"
            elif r10d_latest > 0.8:
                summary += f"\n- 当前量能接近 10 日均量 ({r10d_latest:.1f}x)，量能**正常**"
            elif r10d_latest > 0.4:
                summary += f"\n- 当前量能仅为 10 日均量的 {r10d_latest:.1f} 倍，明显**缩量**"
            else:
                summary += f"\n- 当前量能仅为 10 日均量的 {r10d_latest:.1f} 倍，**极度缩量**，市场观望情绪浓厚"

        if len(recent) >= 3:
            last3_r10d = recent.tail(3)['vol_ratio_10d'].dropna()
            if len(last3_r10d) >= 2:
                if last3_r10d.iloc[-1] > last3_r10d.iloc[0] * 1.2:
                    summary += "\n- 近 3 日量能**逐步放大** 📈"
                elif last3_r10d.iloc[-1] < last3_r10d.iloc[0] * 0.8:
                    summary += "\n- 近 3 日量能**逐步萎缩** 📉"
                else:
                    summary += "\n- 近 3 日量能**基本持平** ➡️"

        result_lines.append(summary)
        return "\n".join(result_lines)

    except Exception as e:
        print(f"query_stock_volume 错误: {traceback.format_exc()}")
        return f"❌ 查询 {stock_name} 成交量时出错: {e}"


# =====================================================================
# 工具 2: search_volume_anomalies (平衡版 - Token优化)
# =====================================================================

class VolumeAnomalyInput(BaseModel):
    days: int = Field(
        default=1,
        description="查看天数：1=今天，3=近3天，5=近5天"
    )
    min_score: int = Field(
        default=30,
        description="最低分（满分100）：30=常规，50=强异动，70=极强"
    )
    limit: int = Field(
        default=15,
        description="返回数量：默认15只，最多50只"
    )
    abnormal_type: str = Field(
        default="",
        description="按类型筛选：'突发放量'、'资金抢筹'、'持续抢筹'，空=不限"
    )


@tool(args_schema=VolumeAnomalyInput)
def search_volume_anomalies(days: int = 1, min_score: int = 30, limit: int = 15, abnormal_type: str = ""):
    """
    【全市场成交量异常筛选】★核心工具★

    ✅ 必须使用的场景：
    • "成交量+【异常|异动|突增|爆发|突发】的股票"
    • "成交量+【TOP|排名|最大|最高】的股票"
    • "【放量|资金抢筹|主力买入】的股票"
    • 用户问"哪些/什么股票+成交量相关"时

    ⚠️ 工具区分：
    • 本工具=全市场筛选（用户问"哪些股票"）
    • query_stock_volume=单只分析（用户指定具体股票）
    • search_top_stocks=K线形态排名（不支持成交量筛选）

    参数：days=1今天, min_score=30常规/50强异动, limit=10-20, abnormal_type可选

    重要：用户问成交量异常/TOP/放量时优先使用，不要说"系统不支持"
    """
    if engine is None:
        return "❌ 数据库未连接"

    try:
        # 1. 构建查询
        conditions = ["trade_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)"]
        params = {"days": days, "limit": limit}

        if min_score > 0:
            conditions.append("total_score >= :min_score")
            params["min_score"] = min_score

        if abnormal_type:
            conditions.append("abnormal_type LIKE :atype")
            params["atype"] = f"%{abnormal_type}%"

        where_clause = " AND ".join(conditions)

        sql = text(f"""
            SELECT trade_date, stock_code, stock_name, close_price, pct_chg,
                   amount, vol_ratio_1d, vol_ratio_10d,
                   score_1d, score_10d, score_amount, total_score,
                   abnormal_type
            FROM stock_fund_flow_abnormal
            WHERE {where_clause}
            ORDER BY total_score DESC
            LIMIT :limit
        """)

        df = pd.read_sql(sql, engine, params=params)

        if df.empty:
            check_sql = text("""
                             SELECT COUNT(*) as cnt, MAX(trade_date) as latest
                             FROM stock_fund_flow_abnormal
                             WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                             """)
            check_df = pd.read_sql(check_sql, engine)
            if check_df.iloc[0]['cnt'] > 0:
                latest = check_df.iloc[0]['latest']
                return (f"📭 最近 {days} 天无符合条件的放量股 (最低分={min_score})。\n"
                        f"数据最新日期: {latest}，建议：增大 days 或降低 min_score。")
            else:
                return "📭 评分表暂无数据，请确认 update_stock_money_scan.py 已执行。"

        # 2. 格式化输出
        result_lines = []

        for trade_date in sorted(df['trade_date'].unique(), reverse=True):
            day_df = df[df['trade_date'] == trade_date].sort_values('total_score', ascending=False)
            date_str = str(trade_date)[:10]

            result_lines.append(f"\n📅 **{date_str} 成交量异常股票** ({len(day_df)} 只)\n")
            result_lines.append(
                "| 排名 | 股票 | 总分 | 涨幅 | 成交额 | 环比 | 均量比 | 标签 |"
            )
            result_lines.append(
                "|------|------|------|------|--------|------|--------|------|"
            )

            for rank, (_, row) in enumerate(day_df.iterrows(), 1):
                name = row['stock_name']
                code = row['stock_code']
                score = row['total_score']
                pct = row['pct_chg']
                amt = row['amount']
                r1d = row['vol_ratio_1d']
                r10d = row['vol_ratio_10d']
                label = row['abnormal_type']

                if amt >= 1e8:
                    amt_str = f"{amt / 1e8:.1f}亿"
                elif amt >= 1e4:
                    amt_str = f"{amt / 1e4:.0f}万"
                else:
                    amt_str = f"{amt:.0f}"

                if score >= 70:
                    score_str = f"🔴 {score:.0f}"
                elif score >= 50:
                    score_str = f"🟠 {score:.0f}"
                else:
                    score_str = f"🟡 {score:.0f}"

                result_lines.append(
                    f"| {rank} | {name}({code}) | {score_str} | "
                    f"{pct:+.1f}% | {amt_str} | "
                    f"{r1d:.1f}x | {r10d:.1f}x | {label} |"
                )

        # 3. 汇总
        avg_score = df['total_score'].mean()
        max_score = df['total_score'].max()
        top_stock = df.iloc[0]
        result_lines.append(
            f"\n**📋 汇总**: 共 {len(df)} 只, "
            f"平均分 {avg_score:.0f}, 最高分 {max_score:.0f} "
            f"({top_stock['stock_name']})"
        )

        return "\n".join(result_lines)

    except Exception as e:
        if "doesn't exist" in str(e) or "no such table" in str(e).lower():
            return ("❌ 评分表 stock_fund_flow_abnormal 不存在。\n"
                    "请先运行 update_stock_money_scan.py 生成评分数据。")
        print(f"search_volume_anomalies 错误: {traceback.format_exc()}")
        return f"❌ 查询放量异动股时出错: {e}"