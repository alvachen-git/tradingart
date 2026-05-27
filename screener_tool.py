"""
智能选股工具
===================
功能：
1. 根据K线形态全市场选股（红三兵、金针探底、吞噬形态等）
2. 按行业板块筛选
3. 按综合评分排名
"""

import pandas as pd
import os
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool

# 1. 初始化数据库连接
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


@st.cache_resource
def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


engine = get_db_engine()

# ==========================================
#  K线形态名称映射表
#  用户可能说的各种名称 -> 数据库中实际存储的关键词
# ==========================================
PATTERN_ALIAS = {
    # === 看涨形态 ===
    "红三兵": ["红三兵"],
    "三红兵": ["红三兵"],
    "连续阳线": ["连续", "阳"],
    "连阳": ["连续", "阳"],
    "三连阳": ["连续3阳", "红三兵"],

    "多头吞噬": ["多头吞噬"],
    "看涨吞噬": ["多头吞噬"],
    "阳包阴": ["多头吞噬"],

    "锤子": ["锤子"],
    "锤子线": ["锤子"],
    "金针探底": ["锤子", "金针"],
    "下影线": ["锤子"],

    "早晨之星": ["早晨之星", "晨星"],
    "启明星": ["早晨之星", "启明星"],

    "V型反转": ["V型反转"],
    "V形反转": ["V型反转"],
    "反转": ["反转", "吞噬"],
    "假突破": ["假突破"],
    "假跌破": ["假跌破"],
    "诱空": ["诱空"],
    "诱多": ["诱多"],

    "多头反击": ["多头反击"],
    "放量突破": ["放量突破"],
    "突破": ["突破"],

    "大阳线": ["大阳线", "大阳"],
    "大阳": ["大阳线", "大阳"],
    "涨停": ["涨停"],

    # === 看跌形态 ===
    "三只乌鸦": ["三只乌鸦"],
    "黑三兵": ["三只乌鸦"],
    "连续阴线": ["连续", "阴"],
    "连阴": ["连续", "阴"],
    "三连阴": ["连续3阴", "三只乌鸦"],

    "空头吞噬": ["空头吞噬"],
    "看跌吞噬": ["空头吞噬"],
    "阴包阳": ["空头吞噬"],

    "吊人线": ["吊人线", "吊人"],
    "上吊线": ["吊人线"],

    "倒锤子": ["倒锤子", "倒状锤子"],
    "倒锤头": ["倒锤子", "倒状锤子"],
    "射击之星": ["倒锤子", "射击"],
    "流星": ["倒锤子", "流星"],

    "黄昏之星": ["黄昏之星"],
    "倒V": ["倒V"],
    "见顶": ["见顶", "倒V"],

    "空头反击": ["空头反击"],
# === 🔥 [新增] 危险/风险相关别名 ===
    "破位": ["破位", "跌破", "下破"],
    "跌破": ["跌破", "破位"],
    "跌破支撑": ["跌破", "破位"],
    "破位下跌": ["破位", "跌破"],
    "头部形态": ["见顶", "头肩顶", "M头"],
    "头肩顶": ["头肩顶", "见顶"],
    "M头": ["M头", "双顶"],
    "双顶": ["双顶", "M头"],
    "下跌趋势": ["空头排列", "下跌"],
    "弱势": ["空头排列", "弱势"],
    "危险": ["空头吞噬", "三只乌鸦", "破位", "跌破","夜星"],
    "风险": ["空头吞噬", "三只乌鸦", "破位", "跌破"],
    "要卖": ["空头吞噬", "见顶", "破位"],
    "卖出信号": ["空头吞噬", "见顶", "破位", "三只乌鸦"],
    "放量下跌": ["放量下跌"],

    "大阴线": ["大阴线", "大阴"],
    "大阴": ["大阴线", "大阴"],
    "跌停": ["跌停"],

    # === 中性形态 ===
    "十字星": ["十字星"],
    "十字线": ["十字星"],
    "doji": ["十字星"],

    "波动收窄": ["波动收窄", "收窄"],
    "整理": ["整理", "收窄", "横盘"],
    "蓄势": ["蓄势", "收窄"],

    # === 趋势类 ===
    "均线多头": ["多头排列"],
    "多头排列": ["多头排列"],
    "均线空头": ["空头排列"],
    "空头排列": ["空头排列"],

    "站上5日线": ["站上5日线", "短强"],
    "站稳20日线": ["站稳20日线", "中多"],
    "跌破20日线": ["跌破20日线", "中空"],

    "上升通道": ["多头排列", "上涨"],
    "下降通道": ["空头排列", "下跌"],
}


def resolve_pattern_keywords(user_input: str) -> list:
    """
    将用户输入的形态名称解析为数据库查询关键词列表

    示例:
    - "红三兵" -> ["红三兵"]
    - "金针探底" -> ["锤子", "金针"]
    - "吞噬" -> ["吞噬"]  (直接模糊匹配)
    """
    user_input = user_input.strip()

    # 1. 先尝试精确匹配别名表
    if user_input in PATTERN_ALIAS:
        return PATTERN_ALIAS[user_input]

    # 2. 尝试部分匹配别名表
    for alias, keywords in PATTERN_ALIAS.items():
        if user_input in alias or alias in user_input:
            return keywords

    # 3. 如果没有匹配到，直接返回用户输入作为模糊搜索词
    return [user_input]


def _format_us_pct(value: float) -> str:
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "N/A"


def _build_us_stock_technical_candidates(
    df: pd.DataFrame,
    latest_date,
    *,
    limit: int = 10,
    min_bars: int = 80,
) -> tuple[pd.DataFrame, str]:
    """Build lightweight US stock bottom-breakout candidates from OHLCV rows."""
    if df is None or df.empty:
        return pd.DataFrame(), "数据不足：stock_prices 没有可用美股日线。"

    required = {"date", "symbol", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        return pd.DataFrame(), f"数据不足：stock_prices 缺少字段 {', '.join(sorted(missing))}。"

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    latest_ts = pd.to_datetime(latest_date, errors="coerce")
    if pd.isna(latest_ts):
        return pd.DataFrame(), "数据不足：无法识别最新美股交易日。"

    for col in ["high", "low", "close", "volume"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work["symbol"] = work["symbol"].astype(str).str.upper().str.strip()
    work = work.dropna(subset=["date", "symbol", "high", "low", "close"]).sort_values(["symbol", "date"])
    work = work[work["date"] <= latest_ts]
    if work.empty:
        return pd.DataFrame(), "数据不足：最新交易日前没有可用日线。"

    rows = []
    for symbol, group in work.groupby("symbol"):
        group = group.sort_values("date").tail(max(120, min_bars + 40)).copy()
        if len(group) < min_bars:
            continue
        latest = group.iloc[-1]
        if latest["date"].normalize() != latest_ts.normalize():
            continue

        prior = group.iloc[:-1]
        if len(prior) < 60:
            continue

        prev_close = float(prior.iloc[-1]["close"])
        close = float(latest["close"])
        low60 = float(group.tail(60)["low"].min())
        prior20_high = float(prior.tail(20)["high"].max())
        ma60 = float(group.tail(60)["close"].mean())
        vol20 = float(prior.tail(20)["volume"].mean() or 0)
        volume = float(latest.get("volume") or 0)

        if prev_close <= 0 or close <= 0 or low60 <= 0:
            continue

        pct_chg = (close / prev_close - 1) * 100
        rebound_pct = (close / low60 - 1) * 100
        breakout_20d = prior20_high > 0 and close >= prior20_high * 1.002
        reclaim_ma60 = prev_close < ma60 <= close
        above_ma60 = close >= ma60
        volume_ratio = volume / vol20 if vol20 > 0 else None

        if rebound_pct < 8 or not (breakout_20d or reclaim_ma60):
            continue

        status_parts = []
        if breakout_20d:
            status_parts.append("突破前20日高点")
        if reclaim_ma60:
            status_parts.append("站回60日线")
        elif above_ma60:
            status_parts.append("位于60日线上方")
        if volume_ratio is not None:
            if volume_ratio >= 1.5:
                status_parts.append("量能放大")
            elif volume_ratio >= 1.0:
                status_parts.append("量能温和")
            else:
                status_parts.append("量能未放大")

        if rebound_pct >= 60:
            bucket = "强势延续但不算底部刚启动"
            bucket_order = 2
        elif volume_ratio is not None and volume_ratio < 1.0:
            bucket = "突破但量能不足"
            bucket_order = 3
        else:
            bucket = "底部刚突破优先观察"
            bucket_order = 1

        # “底部刚突破”更看重回升幅度适中，而不是离低点越远越好。
        ideal_rebound = 32.0
        score = 100 - abs(rebound_pct - ideal_rebound)
        if breakout_20d:
            score += 15
        if reclaim_ma60:
            score += 10
        if volume_ratio is not None:
            score += min(volume_ratio, 2.5) * 6
        if rebound_pct > 60:
            score -= (rebound_pct - 60) * 0.8
        if bucket_order == 2:
            score -= 20
        elif bucket_order == 3:
            score -= 12

        rows.append(
            {
                "分层": bucket,
                "代码": f"{symbol}.US",
                "最新价": close,
                "涨跌幅": pct_chg,
                "距60日低点涨幅": rebound_pct,
                "突破位": prior20_high if breakout_20d else ma60,
                "量能比": volume_ratio,
                "当前状态": "；".join(status_parts) if status_parts else "观察",
                "_bucket_order": bucket_order,
                "_score": score,
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return result, ""
    result = result.sort_values(["_bucket_order", "_score"], ascending=[True, False]).head(max(1, min(30, int(limit or 10))))
    return result.drop(columns=["_bucket_order", "_score"]), ""


def _format_us_stock_technical_candidates(df: pd.DataFrame, latest_date, warning: str = "") -> str:
    date_text = pd.to_datetime(latest_date).strftime("%Y-%m-%d") if latest_date else "未知"
    if warning:
        return (
            "结论：数据不足\n"
            f"- 数据日期：{date_text}\n"
            f"- 原因：{warning}\n"
            "- 提醒：美股筛选使用日线 EOD 数据，不代表盘中实时行情。"
        )
    if df is None or df.empty:
        return (
            "结论：暂无符合条件候选\n"
            f"- 数据日期：{date_text}\n"
            "- 筛选条件：从60日低点回升，且最新收盘突破前20日高点或站回60日线。\n"
            "- 可放宽条件：改看综合强势、放宽到站上20日线，或扩大美股池。\n"
            "- 提醒：美股筛选使用日线 EOD 数据，不代表盘中实时行情。"
        )

    display = df.copy()
    display["最新价"] = display["最新价"].map(lambda x: f"{float(x):.2f}")
    display["涨跌幅"] = display["涨跌幅"].map(_format_us_pct)
    display["距60日低点涨幅"] = display["距60日低点涨幅"].map(lambda x: f"{float(x):.1f}%")
    display["突破位"] = display["突破位"].map(lambda x: f"{float(x):.2f}")
    display["量能比"] = display["量能比"].map(lambda x: "N/A" if pd.isna(x) else f"{float(x):.2f}x")
    bucket_counts = display["分层"].value_counts().to_dict() if "分层" in display.columns else {}
    bucket_summary = "；".join(f"{name}{count}只" for name, count in bucket_counts.items()) or "未分层"

    return (
        "结论：美股技术候选如下，需按分层解读\n"
        f"- 数据日期：{date_text}\n"
        "- 筛选条件：从60日低点回升，且最新收盘突破前20日高点或站回60日线。\n"
        f"- 候选分层：{bucket_summary}\n"
        "- 定位：候选观察名单，不是直接买入指令。\n\n"
        f"{display.to_markdown(index=False)}\n\n"
        "- 提醒：美股筛选使用日线 EOD 数据，不代表盘中实时行情；突破后仍要看回踩和成交量能否延续。"
    )


@tool
def search_us_stocks_by_technical_setup(setup: str = "bottom_breakout", limit: int = 10, min_bars: int = 80):
    """
    【美股轻量技术筛选】
    基于 stock_prices 美股日线筛选底部起来、刚突破、横盘突破等候选股。
    """
    if engine is None:
        return "结论：数据不足\n- 原因：数据库连接失败，无法获取美股日线。\n- 提醒：美股筛选使用日线 EOD 数据。"

    try:
        with engine.connect() as conn:
            latest_date = conn.execute(text("SELECT MAX(date) FROM stock_prices")).scalar()
        if not latest_date:
            return "结论：数据不足\n- 原因：stock_prices 没有美股日线。\n- 提醒：美股筛选使用日线 EOD 数据。"

        start_date = pd.to_datetime(latest_date) - pd.Timedelta(days=260)
        sql = """
            SELECT date, UPPER(symbol) AS symbol, high, low, close, volume
            FROM stock_prices
            WHERE date >= :start_date AND date <= :latest_date
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params={"start_date": start_date.date(), "latest_date": latest_date})

        candidates, warning = _build_us_stock_technical_candidates(
            df,
            latest_date,
            limit=limit,
            min_bars=min_bars,
        )
        return _format_us_stock_technical_candidates(candidates, latest_date, warning)
    except Exception as exc:
        return (
            "结论：数据不足\n"
            f"- 原因：美股筛选工具运行出错：{exc}\n"
            "- 提醒：美股筛选使用日线 EOD 数据。"
        )


# ==========================================
# 🛠️ 智能选股工具
# ==========================================

@tool
def search_top_stocks(condition: str = "综合评分", industry: str = None, limit: int = 10,sort_order: str = "desc"):
    """
    【智能选股器】
    根据K线形态、技术形态、评分和行业板块筛选股票。

    参数:
    - condition: 形态条件，支持以下类型：
        看涨形态: "红三兵"、"金针探底"、"多头吞噬"、"大阳线"、"V型反转"、"突破"、"多头反击"、"晨星"、"假跌破"
        看跌形态: "三只乌鸦"、"空头吞噬"、"吊人线"、"大阴线"、"空头反击"、"夜星"、"假突破"
        中性形态: "十字星"、"波动收窄"、"锤子"
        趋势类: "均线多头"、"均线空头"、"上升通道"
        默认: "综合评分" (按分数排名)
    - industry: (可选) 行业或板块名称，如 "银行"、"半导体"、"酿酒"
    - limit: 返回结果数量，默认10条
    """
    if engine is None:
        return "数据库连接失败，无法获取选股数据。"

    try:
        # 1. 获取最新交易日期
        with engine.connect() as conn:
            max_date = conn.execute(text("SELECT MAX(trade_date) FROM daily_stock_screener")).scalar()

        if not max_date:
            return "选股数据库为空，请先运行盘后更新程序。"

        # 2. 解析用户输入的形态条件
        pattern_keywords = []
        is_pattern_search = False

        if condition and condition not in ["综合评分", "推荐", "股票", "好的", "全部", ""]:
            pattern_keywords = resolve_pattern_keywords(condition)
            is_pattern_search = True

        # 3. 构建 SQL 查询
        base_sql = f"""
            SELECT ts_code, name, industry, close, pct_chg, pattern, ma_trend, score, ai_summary
            FROM daily_stock_screener
            WHERE trade_date = '{max_date}'
        """

        params = {}

        # 3.1 处理行业过滤
        if industry and industry not in ["全市场", "所有", "全部", ""]:
            base_sql += " AND industry LIKE :industry_param"
            params['industry_param'] = f"%{industry}%"

        # 3.2 处理形态过滤 (核心逻辑)
        if is_pattern_search and pattern_keywords:
            # 构建多关键词 OR 查询
            pattern_conditions = []
            for i, keyword in enumerate(pattern_keywords):
                param_name = f"pattern_{i}"
                # 同时搜索 pattern 和 ma_trend 字段
                pattern_conditions.append(f"(pattern LIKE :{param_name} OR ma_trend LIKE :{param_name})")
                params[param_name] = f"%{keyword}%"

            base_sql += " AND (" + " OR ".join(pattern_conditions) + ")"

            # 🔥 [修复] 统一在这里处理排序，避免重复 ORDER BY
        order_direction = "ASC" if sort_order.lower() == "asc" else "DESC"
        base_sql += f" ORDER BY score {order_direction}"

        base_sql += f" LIMIT {limit}"

        # 4. 执行查询
        stmt = text(base_sql)
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn, params=params)

        # 5. 处理空结果
        if df.empty:
            msg = f"📅 {max_date} "
            if industry:
                msg += f"【{industry}】板块中 "
            if is_pattern_search:
                msg += f"未找到符合【{condition}】形态的股票。\n\n"
                msg += "💡 建议：\n"
                msg += "- 尝试其他形态，如：红三兵、金针探底、多头吞噬\n"
                msg += "- 放宽行业限制，搜索全市场\n"
                msg += "- 该形态可能在今日市场中较少出现"
            else:
                msg += "暂无数据。"
            return msg

        # 6. 格式化输出
        is_risk_mode = sort_order.lower() == "asc"
        title_suffix = f" - {industry}板块" if industry else ""
        search_desc = f"【{condition}】形态" if is_pattern_search else "综合评分"

        if is_risk_mode:
            result_text = f"⚠️ **风险股票警示 ({max_date}){title_suffix}**\n"
            result_text += f"🔍 筛选条件: {search_desc} (按风险排序)\n"
            result_text += f"🚨 共找到 {len(df)} 只需要警惕的股票\n\n"
        else:
            result_text = f"📅 **选股结果 ({max_date}){title_suffix}**\n"
            result_text += f"🔍 筛选条件: {search_desc}\n"
            result_text += f"📊 共找到 {len(df)} 只符合条件的股票\n\n"

        for idx, row in df.iterrows():
            score_icon = "🌟" if row['score'] >= 80 else "📈" if row['score'] >= 60 else "📊"
            pct_icon = "🔴" if row['pct_chg'] > 0 else "🟢" if row['pct_chg'] < 0 else "⚪"

            result_text += f"**{idx + 1}. {row['name']} ({row['ts_code']})** [{row['industry']}]\n"
            result_text += f"   {score_icon} 评分: **{row['score']}分** | {pct_icon} 现价: {row['close']} ({row['pct_chg']:+.2f}%)\n"

            # 显示匹配到的形态
            features = []
            if row['pattern']:
                features.append(f"形态: {row['pattern'][:60]}")
            if row['ma_trend']:
                features.append(f"趋势: {row['ma_trend'][:40]}")

            if features:
                result_text += f"   ⚡ {' | '.join(features)}\n"

            if row['ai_summary']:
                summary = row['ai_summary'][:80] + "..." if len(str(row['ai_summary'])) > 80 else row['ai_summary']
                result_text += f"   💡 {summary}\n"

            result_text += "\n"

        # 7. 添加提示信息
        if is_pattern_search:
            result_text += "---\n"
            result_text += "💡 **其他可搜索形态**: 红三兵、三只乌鸦、金针探底、多头吞噬、空头吞噬、十字星、大阳线、放量突破、均线多头"

        return result_text

    except Exception as e:
        return f"选股工具运行出错: {e}"


@tool
def get_available_patterns():
    """
    【形态查询助手】
    查询数据库中今日出现的所有K线形态及其数量统计。
    用于帮助用户了解当前市场有哪些形态可供筛选。
    """
    if engine is None:
        return "数据库连接失败"

    try:
        with engine.connect() as conn:
            max_date = conn.execute(text("SELECT MAX(trade_date) FROM daily_stock_screener")).scalar()

        if not max_date:
            return "数据库为空"

        # 统计各形态出现次数
        sql = f"""
            SELECT pattern, COUNT(*) as cnt
            FROM daily_stock_screener
            WHERE trade_date = '{max_date}'
              AND pattern IS NOT NULL 
              AND pattern != ''
            GROUP BY pattern
            ORDER BY cnt DESC
            LIMIT 10
        """

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

        if df.empty:
            return f"📅 {max_date} 暂无形态数据"

        result = f"📅 **{max_date} 市场形态统计**\n\n"
        result += "| 形态 | 出现次数 |\n"
        result += "|------|----------|\n"

        for _, row in df.iterrows():
            pattern = row['pattern'][:30] + "..." if len(str(row['pattern'])) > 30 else row['pattern']
            result += f"| {pattern} | {row['cnt']} |\n"

        result += "\n💡 使用示例: `帮我找红三兵的股票` 或 `半导体板块有哪些金针探底`"

        return result

    except Exception as e:
        return f"查询形态统计出错: {e}"


# ==========================================
#  辅助函数：获取选股数据供页面使用
# ==========================================
def get_screener_data(condition: str = None, industry: str = None, limit: int = 50) -> pd.DataFrame:
    """
    获取选股数据，供 Streamlit 页面可视化使用
    """
    if engine is None:
        return pd.DataFrame()

    try:
        with engine.connect() as conn:
            max_date = conn.execute(text("SELECT MAX(trade_date) FROM daily_stock_screener")).scalar()

        if not max_date:
            return pd.DataFrame()

        sql = f"""
            SELECT ts_code, name, industry, close, pct_chg, pattern, ma_trend, score, ai_summary
            FROM daily_stock_screener
            WHERE trade_date = '{max_date}'
        """

        params = {}

        if industry and industry not in ["全市场", "所有", ""]:
            sql += " AND industry LIKE :industry"
            params['industry'] = f"%{industry}%"

        if condition and condition not in ["综合评分", ""]:
            keywords = resolve_pattern_keywords(condition)
            conditions = []
            for i, kw in enumerate(keywords):
                param = f"p{i}"
                conditions.append(f"(pattern LIKE :{param} OR ma_trend LIKE :{param})")
                params[param] = f"%{kw}%"
            sql += " AND (" + " OR ".join(conditions) + ")"

        sql += f" ORDER BY score DESC LIMIT {limit}"

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)

        return df

    except Exception as e:
        print(f"Error in get_screener_data: {e}")
        return pd.DataFrame()


def get_industries_list() -> list:
    """获取所有行业列表"""
    if engine is None:
        return []

    try:
        sql = """
              SELECT DISTINCT industry
              FROM daily_stock_screener
              WHERE industry IS NOT NULL \
                AND industry != ''
              ORDER BY industry \
              """
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        return df['industry'].tolist()
    except:
        return []


# ==========================================
#  测试入口
# ==========================================
if __name__ == "__main__":
    print("=" * 60)
    print("测试1: 搜索破位形态")
    print("=" * 60)
    result = search_top_stocks.invoke({"condition": "破位"})
    print(result)

    print("\n" + "=" * 60)
    print("测试2: 银行板块综合评分")
    print("=" * 60)
    result = search_top_stocks.invoke({"condition": "综合评分", "industry": "银行"})
    print(result)

    print("\n" + "=" * 60)
    print("测试3: 查看今日市场形态统计")
    print("=" * 60)
    result = get_available_patterns.invoke({})
    print(result)
