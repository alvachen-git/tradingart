"""
每日复盘晚报生成器 v2.1
基于原版升级：
- 🔥 新增：发布到订阅中心数据库
- 🔥 新增：自动创建站内消息通知
- 🔥 更新：邮件群发使用新的订阅表
"""

import pandas as pd
import os
import time
import re
import json
from html import escape, unescape
from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from llm_compat import build_report_tongyi_llm, invoke_report_llm_with_fallback
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent
from symbol_match import sql_prefix_condition

# ==========================================
# 1. 引入全套工具 (Toolbox)
# ==========================================
from news_tools import get_financial_news
from fund_flow_tools import (
    SectorMoneyFlowNotReadyError,
    build_sector_money_flow_snapshot,
    canonical_sector_name,
    tool_get_retail_money_flow,
)
from futures_fund_flow_tools import get_futures_fund_flow
from volume_oi_tools import get_option_volume_abnormal, analyze_etf_option_sentiment
from screener_tool import search_top_stocks
from kline_tools import analyze_kline_pattern
from data_engine import get_commodity_iv_info, search_broker_holdings_on_date, tool_analyze_broker_positions
from email_utils2 import send_email
from search_tools import search_web
from polymarket_tool import tool_get_polymarket_sentiment
from market_tools import get_today_hotlist, get_finance_related_trends

# 🔥 新增：订阅服务
import subscription_service as sub_svc

# 1. 初始化环境
load_dotenv(override=True)

# 数据库连接
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

# 初始化 LLM
REPORT_LLM_ENV_PREFIX = "DAILY_REPORT"
REPORT_LLM_TEMPERATURE = 0.1
REPORT_REWRITE_TEMPERATURE = 0.0
llm = build_report_tongyi_llm(env_prefix=REPORT_LLM_ENV_PREFIX, temperature=REPORT_LLM_TEMPERATURE)
rewrite_llm = build_report_tongyi_llm(
    env_prefix=REPORT_LLM_ENV_PREFIX,
    temperature=REPORT_REWRITE_TEMPERATURE,
)

# 商品卡片发布前校验配置（仅校验“商品期货全景”中的隐含波动率口径）
COMMODITY_CARD_LIST = [
    "黄金", "白银", "原油", "铜", "碳酸锂",
    "铁矿石", "豆粕", "橡胶", "棉花", "PTA",
]
COMMODITY_IV_PREFIX_MAP = {
    "黄金": ["AU"],
    "白银": ["AG"],
    "原油": ["SC"],
    "铜": ["CU"],
    "碳酸锂": ["LC"],
    "铁矿石": ["I"],
    "豆粕": ["M"],
    "橡胶": ["RU", "NR", "BR"],
    "棉花": ["CF"],
    "PTA": ["TA"],
}
COMMODITY_IV_LEVEL_TOKENS = ["极低", "低", "偏低", "中", "中等", "偏高", "高", "极高"]
COMMODITY_IV_INVALID_TOKENS = ["无数据", "N/A", "未知", "None", "--"]


def _bounded_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


MAX_REWRITE_ROUNDS = _bounded_int_env("DAILY_REPORT_MAX_REWRITE_ROUNDS", 4, 1, 6)
REPORT_SLOT_ORDER = (
    "market-headline", "stock-sector", "futures-holding", "commodity-panorama",
    "option-volatility", "daily-bull", "risk-warning", "tomorrow-strategy", "footer-quote",
)
REPORT_SLOT_LABELS = {
    "market-headline": "市场头条", "stock-sector": "股票板块", "futures-holding": "期货商持仓",
    "commodity-panorama": "商品期货全景", "option-volatility": "期权波动率", "daily-bull": "每日牛股",
    "risk-warning": "风险警示", "tomorrow-strategy": "明日策略", "footer-quote": "底部点评",
}
REPORT_REPAIR_FORBIDDEN_TAGS = {"html", "head", "body", "style", "script", "link", "meta"}

# A股晚报硬事实配置。股票/指数方向、板块资金与ETF IV都必须先通过程序查库，
# AI只能解释这些事实，不能自行补数或沿用上一交易日素材。
A_SHARE_INDEX_MAP = {
    "上证指数": "000001.SH",
    "深证成指": "399001.SZ",
    "创业板指": "399006.SZ",
}
ETF_OPTION_SNAPSHOT_MAP = {
    "沪深300": "510300.SH",
    "中证500": "510500.SH",
    "创业板": "159915.SZ",
    "科创50": "588000.SH",
    "上证50": "510050.SH",
}
SECTOR_FLOW_AMOUNT_TOLERANCE_YI = 0.25
ETF_IV_RANK_TOLERANCE = 0.75


class ReportDataNotReadyError(RuntimeError):
    """Essential report data has not reached the requested trading date."""


def _current_trade_date_key() -> str:
    """报告运行日对应的 YYYYMMDD，传给K线工具避免重跑历史日报时漂移到最新K线。"""
    return datetime.now().strftime("%Y%m%d")


def _extract_first_percent(value_text: str):
    """从文本中提取第一个百分比数值（如 42.5% -> 42.5）。"""
    if not value_text:
        return None
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", value_text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _calc_iv_level(iv_rank: float) -> str:
    """将 IV Rank 映射为等级文本。"""
    if iv_rank < 20:
        return "低"
    if iv_rank < 40:
        return "偏低"
    if iv_rank < 60:
        return "中等"
    if iv_rank < 80:
        return "偏高"
    return "高"


def _normalize_report_trade_date(value) -> str:
    """Normalize DB/tool dates to YYYYMMDD."""
    cleaned = re.sub(r"\D", "", str(value or ""))[:8]
    return cleaned if len(cleaned) == 8 else ""


def _report_date_context(report_trade_date: str = None):
    """Return normalized date key, localized title date and weekday for a report."""
    date_key = _normalize_report_trade_date(report_trade_date or _current_trade_date_key())
    if not date_key:
        raise ValueError(f"无效报告交易日: {report_trade_date}")
    report_dt = datetime.strptime(date_key, "%Y%m%d")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][report_dt.weekday()]
    return date_key, report_dt.strftime("%Y年%m月%d日"), weekday


def _require_report_date(dataset_name: str, actual_date, expected_date: str) -> str:
    """Fail closed when a required dataset has not reached the report date."""
    actual = _normalize_report_trade_date(actual_date)
    expected = _normalize_report_trade_date(expected_date)
    if not actual:
        raise ReportDataNotReadyError(f"{dataset_name} 无可用交易日，报告已阻断")
    if actual != expected:
        raise ReportDataNotReadyError(
            f"{dataset_name} 数据未就绪：报告日 {expected}，数据库最新仅到 {actual}；"
            "禁止用旧交易日数据生成当日晚报"
        )
    return actual


def _fetch_price_move(table_name: str, ts_code: str, report_trade_date: str) -> dict:
    """Fetch two closes and derive a deterministic daily/candlestick direction."""
    if table_name not in {"index_price", "stock_price"}:
        raise ValueError(f"不允许的行情表: {table_name}")

    sql = text(f"""
        SELECT REPLACE(trade_date, '-', '') AS trade_date,
               open_price, high_price, low_price, close_price
        FROM {table_name}
        WHERE ts_code = :ts_code
          AND REPLACE(trade_date, '-', '') <= :report_trade_date
        ORDER BY trade_date DESC
        LIMIT 2
    """)
    df = pd.read_sql(
        sql,
        engine,
        params={"ts_code": ts_code, "report_trade_date": report_trade_date},
    )
    if len(df) < 2:
        raise ReportDataNotReadyError(f"{table_name} 缺少 {ts_code} 最近两个交易日行情")

    latest = df.iloc[0]
    previous = df.iloc[1]
    latest_date = _require_report_date(f"{table_name}:{ts_code}", latest["trade_date"], report_trade_date)
    close_price = float(latest["close_price"])
    previous_close = float(previous["close_price"])
    open_price = float(latest["open_price"])
    pct_change = ((close_price / previous_close) - 1.0) * 100.0 if previous_close else 0.0
    candle_change = ((close_price / open_price) - 1.0) * 100.0 if open_price else 0.0

    if candle_change >= 1.0:
        candle = "大阳线"
    elif candle_change > 0.05:
        candle = "阳线"
    elif candle_change <= -1.0:
        candle = "大阴线"
    elif candle_change < -0.05:
        candle = "阴线"
    else:
        candle = "十字/平盘K线"

    return {
        "trade_date": latest_date,
        "previous_trade_date": _normalize_report_trade_date(previous["trade_date"]),
        "open": open_price,
        "high": float(latest["high_price"]),
        "low": float(latest["low_price"]),
        "close": close_price,
        "previous_close": previous_close,
        "pct_change": round(pct_change, 4),
        "candle_change_pct": round(candle_change, 4),
        "direction": "上涨" if pct_change > 0 else "下跌" if pct_change < 0 else "平盘",
        "candle": candle,
        "ts_code": ts_code,
    }


def _canonical_sector_name(value: str) -> str:
    """Collapse duplicate DC hierarchy suffixes such as 航天装备Ⅱ/Ⅲ."""
    return canonical_sector_name(value)


def _fetch_etf_iv_snapshot(etf_code: str, report_trade_date: str) -> dict:
    sql = text("""
        SELECT REPLACE(trade_date, '-', '') AS trade_date, iv
        FROM etf_iv_history
        WHERE etf_code = :etf_code
          AND REPLACE(trade_date, '-', '') <= :report_trade_date
        ORDER BY trade_date DESC
        LIMIT 252
    """)
    df = pd.read_sql(
        sql,
        engine,
        params={"etf_code": etf_code, "report_trade_date": report_trade_date},
    )
    if df.empty:
        raise ReportDataNotReadyError(f"etf_iv_history 缺少 {etf_code} IV数据")

    latest_date = _require_report_date(
        f"etf_iv_history:{etf_code}",
        df.iloc[0]["trade_date"],
        report_trade_date,
    )
    iv_values = pd.to_numeric(df["iv"], errors="coerce").dropna()
    if iv_values.empty:
        raise ReportDataNotReadyError(f"etf_iv_history:{etf_code} IV均为无效值")

    current_iv = float(iv_values.iloc[0])
    min_iv = float(iv_values.min())
    max_iv = float(iv_values.max())
    iv_rank = ((current_iv - min_iv) / (max_iv - min_iv) * 100.0) if max_iv != min_iv else 50.0
    return {
        "trade_date": latest_date,
        "iv": round(current_iv, 2),
        "iv_rank": round(iv_rank, 1),
        "level": _calc_iv_level(iv_rank),
        "sample_days": int(len(iv_values)),
        "etf_code": etf_code,
    }


def _fetch_programmatic_a_share_snapshot(report_trade_date: str = None):
    """
    Build the publication truth set for A-share direction, sector flow and ETF IV.

    Every required dataset must be fresh for report_trade_date. A stale/missing table
    raises ReportDataNotReadyError so the report is not generated or published.
    """
    report_date = _normalize_report_trade_date(report_trade_date or _current_trade_date_key())
    snapshot = {
        "report_date": report_date,
        "indices": {},
        "etfs": {},
        "etf_iv": {},
        "sectors": {},
        "sector_top_in": [],
        "sector_top_out": [],
    }

    lines = [f"【A股发布真值｜交易日 {report_date}】"]
    lines.append("指数/ETF当日方向（收盘对前收盘）：")

    for display_name, ts_code in A_SHARE_INDEX_MAP.items():
        move = _fetch_price_move("index_price", ts_code, report_date)
        move["display_name"] = display_name
        snapshot["indices"][display_name] = move
        lines.append(
            f"- {display_name}({ts_code}): {move['pct_change']:+.2f}%，"
            f"收盘 {move['close']:.2f}，{move['candle']}，当日{move['direction']}"
        )

    for display_name, ts_code in ETF_OPTION_SNAPSHOT_MAP.items():
        move = _fetch_price_move("stock_price", ts_code, report_date)
        move["display_name"] = display_name
        snapshot["etfs"][display_name] = move
        iv_info = _fetch_etf_iv_snapshot(ts_code, report_date)
        iv_info["display_name"] = display_name
        snapshot["etf_iv"][display_name] = iv_info
        lines.append(
            f"- {display_name}ETF({ts_code}): {move['pct_change']:+.2f}%，"
            f"{move['candle']}；IV {iv_info['iv']:.2f}%，"
            f"252日Rank {iv_info['iv_rank']:.1f}%（{iv_info['level']}）"
        )

    try:
        sector_snapshot = build_sector_money_flow_snapshot(
            days=1,
            as_of_date=report_date,
            db_engine=engine,
        )
    except SectorMoneyFlowNotReadyError as exc:
        raise ReportDataNotReadyError(str(exc)) from exc

    snapshot["sectors"] = sector_snapshot["sectors"]
    snapshot["sector_top_in"] = sector_snapshot["sector_top_in"]
    snapshot["sector_top_out"] = sector_snapshot["sector_top_out"]
    print(
        "🏦 [板块资金快照] source=programmatic "
        f"date={sector_snapshot['report_date']} "
        f"raw={sector_snapshot['raw_row_count']} "
        f"unique={len(sector_snapshot['sectors'])} "
        f"collapsed={sector_snapshot['collapsed_duplicate_count']}"
    )

    lines.append("板块主力净额（main_net_inflow，单位亿元；必须按正负号写流入/流出）：")
    lines.append("- 主力净流入Top3：" + "；".join(
        f"{x['display_name']} {x['main_flow_yi']:+.1f}亿（板块{x['pct_change']:+.2f}%）"
        for x in snapshot["sector_top_in"]
    ))
    lines.append("- 主力净流出Top3：" + "；".join(
        f"{x['display_name']} {x['main_flow_yi']:+.1f}亿（板块{x['pct_change']:+.2f}%）"
        for x in snapshot["sector_top_out"]
    ))
    lines.append("硬规则：记者素材与本真值冲突时，以本真值为准；不得把下跌写成上涨、流出写成流入。")
    return snapshot, "\n".join(lines)


def _fetch_programmatic_commodity_iv_snapshot(report_trade_date: str = None):
    """
    程序端确定性抓取 10 个商品当前 IV 与等级，作为晚报卡片真值。
    返回:
      - snapshot_map: {商品: {iv, iv_rank, level, ts_code, trade_date}}
      - snapshot_text: 可直接注入到 prompt 的说明文本
    """
    snapshot_map = {}
    lines = []
    report_date = _normalize_report_trade_date(report_trade_date)
    date_filter = (
        f"AND REPLACE(trade_date, '-', '') <= '{report_date}'"
        if report_date else ""
    )

    for commodity in COMMODITY_CARD_LIST:
        prefixes = COMMODITY_IV_PREFIX_MAP.get(commodity, [])
        best = None

        for prefix in prefixes:
            try:
                sql_main = f"""
                    SELECT ts_code, REPLACE(trade_date, '-', '') AS trade_date, oi
                    FROM futures_price
                    WHERE {sql_prefix_condition(prefix)}
                      AND ts_code NOT LIKE '%%TAS%%'
                      {date_filter}
                    ORDER BY trade_date DESC, oi DESC
                    LIMIT 1
                """
                df_main = pd.read_sql(sql_main, engine)
                if df_main.empty:
                    continue

                ts_code = str(df_main.iloc[0]["ts_code"])
                sql_iv_latest = f"""
                    SELECT REPLACE(trade_date, '-', '') AS trade_date, iv
                    FROM commodity_iv_history
                    WHERE ts_code = '{ts_code}'
                      {date_filter}
                    ORDER BY trade_date DESC
                    LIMIT 1
                """
                df_latest = pd.read_sql(sql_iv_latest, engine)

                if df_latest.empty:
                    sql_iv_fallback = f"""
                        SELECT ts_code, REPLACE(trade_date, '-', '') AS trade_date, iv
                        FROM commodity_iv_history
                        WHERE {sql_prefix_condition(prefix)}
                          {date_filter}
                        ORDER BY trade_date DESC
                        LIMIT 1
                    """
                    df_fb = pd.read_sql(sql_iv_fallback, engine)
                    if df_fb.empty:
                        continue
                    ts_code = str(df_fb.iloc[0]["ts_code"])
                    latest_trade_date = str(df_fb.iloc[0]["trade_date"])
                    curr_iv = float(df_fb.iloc[0]["iv"])
                else:
                    latest_trade_date = str(df_latest.iloc[0]["trade_date"])
                    curr_iv = float(df_latest.iloc[0]["iv"])

                latest_trade_date = _normalize_report_trade_date(latest_trade_date)

                sql_hist = f"""
                    SELECT iv
                    FROM commodity_iv_history
                    WHERE ts_code = '{ts_code}'
                      AND REPLACE(trade_date, '-', '') <= '{latest_trade_date}'
                    ORDER BY trade_date DESC
                    LIMIT 252
                """
                df_hist = pd.read_sql(sql_hist, engine)
                if df_hist.empty:
                    iv_rank = 50.0
                else:
                    max_iv = float(df_hist["iv"].max())
                    min_iv = float(df_hist["iv"].min())
                    iv_rank = ((curr_iv - min_iv) / (max_iv - min_iv) * 100.0) if max_iv != min_iv else 50.0

                level = _calc_iv_level(iv_rank)
                best = {
                    "iv": round(curr_iv, 2),
                    "iv_rank": round(iv_rank, 1),
                    "level": level,
                    "ts_code": ts_code,
                    "trade_date": latest_trade_date,
                }
                break
            except Exception as e:
                print(f"⚠️ [IV快照] {commodity} via {prefix} 查询异常: {e}")
                continue

        if best:
            snapshot_map[commodity] = best
            lines.append(
                f"- {commodity}: {best['iv']:.2f}%（{best['level']}，Rank {best['iv_rank']:.1f}%）"
                f" [合约 {best['ts_code']}, 日期 {best['trade_date']}]"
            )
        else:
            lines.append(f"- {commodity}: 暂无可用IV数据（本次不做数值对齐）")

    snapshot_text = "\n".join(lines)
    return snapshot_map, snapshot_text


def _clean_inline_html_text(text_value: str) -> str:
    """清理卡片字段中的HTML标签与多余空白，便于确定性校验。"""
    if not text_value:
        return ""
    cleaned = re.sub(r"<[^>]+>", "", str(text_value))
    cleaned = cleaned.replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", cleaned).strip()


def _normalize_kline_shape_label(shape: str) -> str:
    """统一形态标签写法，去掉装饰括号和“形态”等尾缀。"""
    label = _clean_inline_html_text(shape)
    label = label.strip("【】[]（）() ")
    label = re.sub(r"[，,。；;：:].*$", "", label).strip()
    label = re.sub(r"(形态|信号)$", "", label).strip()
    return label


def _extract_section_after_heading(report_text: str, heading_keyword: str) -> str:
    """从 analyze_kline_pattern 的多段文本里提取指定标题后的第一段正文。"""
    if not report_text:
        return ""
    pattern = re.compile(
        rf"{re.escape(heading_keyword)}(?:\*\*)?\s*\n(?P<body>.*?)(?:\n\s*\*\*|$)",
        re.S,
    )
    m = pattern.search(report_text)
    if not m:
        return ""
    return m.group("body").strip()


def _extract_kline_shape_from_report(report_text: str) -> str:
    """
    将 K 线工具输出压缩成商品卡片可展示的确定性形态标签。
    优先使用“今日形态信号”，普通K线则保留为“普通震荡K线”，避免LLM自行改写。
    """
    section = _extract_section_after_heading(report_text, "今日形态信号")
    first_line = ""
    for raw_line in section.splitlines():
        line = _clean_inline_html_text(raw_line).lstrip("-• ").strip()
        if line:
            first_line = line
            break

    if not first_line:
        return "暂无明确形态"
    if "普通震荡K线" in first_line:
        return "普通震荡K线"
    if "无明显形态" in first_line:
        return "无明显形态"

    m = re.search(r"【([^】]+)】", first_line)
    if m:
        return _normalize_kline_shape_label(m.group(1))
    return _normalize_kline_shape_label(first_line) or "暂无明确形态"


def _normalize_commodity_direction_label(direction: str) -> str:
    """将商品趋势标签统一为 看多/看空/震荡。"""
    text_value = _clean_inline_html_text(direction)
    if not text_value:
        return ""
    if "看多" in text_value or "偏多" in text_value:
        return "看多"
    if "看空" in text_value or "偏空" in text_value:
        return "看空"
    if "震荡" in text_value or "中性" in text_value or "观望" in text_value:
        return "震荡"
    return text_value.strip()


def _extract_commodity_direction_label(card_segment: str) -> str:
    """从单个商品卡片片段中提取展示给用户的趋势标签。"""
    text_value = _clean_inline_html_text(card_segment)
    m = re.search(r"(看多|偏多|看空|偏空|震荡|中性|观望)", text_value)
    if not m:
        return ""
    return _normalize_commodity_direction_label(m.group(1))


def _derive_kline_direction_from_report(report_text: str, shape: str = "") -> str:
    """
    从 K 线工具报告确定性派生商品趋势标签，避免 LLM 把偏空技术面标成看多。
    输出只允许：看多 / 看空 / 震荡。
    """
    report = _clean_inline_html_text(report_text)
    shape_label = _normalize_kline_shape_label(shape)
    source_text = f"{shape_label} {report}"

    bullish_score = 0
    bearish_score = 0

    bullish_shape_tokens = [
        "假跌破", "多头吞噬", "上升三法", "大阳线", "多头反击",
        "晨星", "锤子线", "突破", "放量突破", "红三兵", "V型反转",
    ]
    bearish_shape_tokens = [
        "假突破", "空头吞噬", "下降三法", "大阴线", "空头反击",
        "夜星", "吊人线", "射击之星", "破位", "放量下跌", "三只乌鸦", "倒V",
    ]

    for token in bullish_shape_tokens:
        if token in shape_label:
            bullish_score += 3
            break
    for token in bearish_shape_tokens:
        if token in shape_label:
            bearish_score += 3
            break

    bullish_trend_weights = {
        "近5日强势上涨": 3,
        "多头主导": 3,
        "均线多头排列": 2,
        "站上5日线": 1,
        "站稳20日线": 1,
        "中多": 1,
        "震荡偏多": 1,
        "小幅上涨": 1,
    }
    bearish_trend_weights = {
        "近5日持续下跌": 3,
        "空头主导": 3,
        "均线空头排列": 2,
        "跌破5日线": 1,
        "跌破20日线": 1,
        "中空": 1,
        "震荡偏空": 1,
        "小幅下跌": 1,
    }

    for token, weight in bullish_trend_weights.items():
        if token in source_text:
            bullish_score += weight
    for token, weight in bearish_trend_weights.items():
        if token in source_text:
            bearish_score += weight

    if "站上5日线" in source_text and "站稳20日线" in source_text:
        bullish_score += 1
    if "跌破5日线" in source_text and "跌破20日线" in source_text:
        bearish_score += 1

    if bullish_score - bearish_score >= 2:
        return "看多"
    if bearish_score - bullish_score >= 2:
        return "看空"
    return "震荡"


def _fetch_programmatic_commodity_kline_snapshot(trade_date: str = None):
    """
    程序端调用K线工具抓取商品形态真值，供晚报商品卡片强制使用。
    返回:
      - snapshot_map: {商品: {shape, direction, trade_date}}
      - snapshot_text: 可直接注入到 prompt 的说明文本
    """
    snapshot_map = {}
    lines = []
    report_trade_date = trade_date or _current_trade_date_key()

    for commodity in COMMODITY_CARD_LIST:
        try:
            report = analyze_kline_pattern.invoke({
                "query": commodity,
                "trade_date": report_trade_date,
            })
            report_text = str(report)
            shape = _extract_kline_shape_from_report(report_text)
            direction = _derive_kline_direction_from_report(report_text, shape)
            snapshot_map[commodity] = {
                "shape": shape,
                "direction": direction,
                "trade_date": report_trade_date,
            }
            lines.append(f"- {commodity}: 形态={shape}; 趋势={direction} [K线日期 <= {report_trade_date}]")
        except Exception as e:
            print(f"⚠️ [K线形态快照] {commodity} 查询异常: {e}")
            lines.append(f"- {commodity}: 暂无可用K线形态与趋势（本次不做形态/趋势对齐）")

    snapshot_text = "\n".join(lines)
    return snapshot_map, snapshot_text


def validate_commodity_cards(html: str, expected_iv_map: dict = None, expected_kline_map: dict = None):
    """
    校验商品卡片字段是否合理：
    1) 必须包含“隐含波动率/隐波/IV”字段；
    2) 必须给出高/中/低等级描述；
    3) 若给出百分比，必须在 0%~300% 区间。
    4) 若提供 expected_iv_map，百分比需与程序注入真值一致（容忍±0.3%）。
    5) 若提供 expected_kline_map，形态字段需与程序注入真值一致。
    6) 若 expected_kline_map 提供 direction，趋势标签需与程序注入真值一致。
    """
    anomalies = []
    if not html:
        return False, ["HTML为空"]

    for commodity in COMMODITY_CARD_LIST:
        pattern = re.compile(
            rf"{re.escape(commodity)}(.{{0,500}}?)(?:隐含波动率|隐波|IV)[：:]\s*(?P<iv_text>[^<\n]+)",
            re.S | re.I
        )
        m = pattern.search(html)
        if not m:
            anomalies.append(f"{commodity} 缺少“隐含波动率”字段")
            continue

        iv_text = m.group("iv_text").strip()
        lower_text = iv_text.lower()

        if any(token.lower() in lower_text for token in COMMODITY_IV_INVALID_TOKENS):
            anomalies.append(f"{commodity} IV字段无效: {iv_text}")

        if not any(token in iv_text for token in COMMODITY_IV_LEVEL_TOKENS):
            anomalies.append(f"{commodity} IV缺少等级描述(高/中/低): {iv_text}")

        iv_percent = _extract_first_percent(iv_text)
        if iv_percent is not None and not (0 <= iv_percent <= 300):
            anomalies.append(f"{commodity} IV百分比超范围: {iv_percent:.2f}%")
        if iv_percent is None:
            anomalies.append(f"{commodity} IV缺少百分比数值: {iv_text}")

        expected = (expected_iv_map or {}).get(commodity)
        if expected and iv_percent is not None:
            expected_iv = float(expected["iv"])
            if abs(iv_percent - expected_iv) > 0.3:
                anomalies.append(
                    f"{commodity} IV与真值不一致: 页面={iv_percent:.2f}%, 真值={expected_iv:.2f}%"
                )

        expected_kline = (expected_kline_map or {}).get(commodity)
        if expected_kline:
            expected_direction = _normalize_commodity_direction_label(expected_kline.get("direction", ""))
            if expected_direction:
                direction_pattern = re.compile(
                    rf"{re.escape(commodity)}(?P<direction_segment>.{{0,400}}?)形态[：:]",
                    re.S | re.I
                )
                direction_match = direction_pattern.search(html)
                if not direction_match:
                    anomalies.append(f"{commodity} 缺少趋势标签或“形态”字段")
                    continue
                actual_direction = _extract_commodity_direction_label(direction_match.group("direction_segment"))
                if not actual_direction:
                    anomalies.append(f"{commodity} 缺少趋势标签")
                elif actual_direction != expected_direction:
                    anomalies.append(
                        f"{commodity} 趋势标签与真值不一致: 页面={actual_direction}, 真值={expected_direction}"
                    )

            shape_pattern = re.compile(
                rf"{re.escape(commodity)}(.{{0,500}}?)形态[：:]\s*(?P<shape_text>[^<\n]+)",
                re.S | re.I
            )
            shape_match = shape_pattern.search(html)
            if not shape_match:
                anomalies.append(f"{commodity} 缺少“形态”字段")
                continue

            actual_shape = _normalize_kline_shape_label(shape_match.group("shape_text"))
            expected_shape = _normalize_kline_shape_label(expected_kline.get("shape", ""))
            if expected_shape and actual_shape != expected_shape:
                anomalies.append(
                    f"{commodity} 形态与真值不一致: 页面={actual_shape}, 真值={expected_shape}"
                )

    return len(anomalies) == 0, anomalies


def _report_html_to_plain_text(html_content: str) -> str:
    text_value = re.sub(r"<style[\s\S]*?</style>", " ", str(html_content or ""), flags=re.I)
    text_value = re.sub(r"<script[\s\S]*?</script>", " ", text_value, flags=re.I)
    text_value = re.sub(r"<!--[\s\S]*?-->", " ", text_value)
    text_value = re.sub(r"<(?:br|/p|/div|/td|/tr|/h[1-6])\b[^>]*>", "\n", text_value, flags=re.I)
    text_value = re.sub(r"<[^>]+>", " ", text_value)
    text_value = unescape(text_value).replace("−", "-").replace("＋", "+")
    text_value = re.sub(r"[ \t\r\f\v]+", " ", text_value)
    text_value = re.sub(r"\n\s*\n+", "\n", text_value)
    return text_value.strip()


def _extract_plain_section(plain_text: str, start_token: str, end_tokens) -> str:
    start = plain_text.find(start_token)
    if start < 0:
        return ""
    end_positions = [plain_text.find(token, start + len(start_token)) for token in end_tokens]
    end_positions = [pos for pos in end_positions if pos >= 0]
    end = min(end_positions) if end_positions else len(plain_text)
    return plain_text[start:end].strip()


def _entity_pattern(entity: str) -> str:
    return rf"{re.escape(entity)}(?![\u4e00-\u9fffA-Za-z0-9])"


def _sector_entity_pattern(entity: str) -> str:
    """Match a sector name with the common optional 行业/板块 suffix."""
    return (
        rf"(?<![\u4e00-\u9fffA-Za-z0-9]){re.escape(entity)}"
        rf"(?:行业|板块)?(?![\u4e00-\u9fffA-Za-z0-9])"
    )


def _etf_entity_pattern(entity: str) -> str:
    """Match both 沪深300 and 沪深300ETF before applying the boundary."""
    return rf"{re.escape(entity)}(?:ETF)?(?![\u4e00-\u9fffA-Za-z0-9])"


def _is_conditional_context(context: str) -> bool:
    return any(token in context for token in ["若", "如果", "一旦", "可能", "预期", "关注", "假设"])


def _direction_entity_pattern(alias: str) -> str:
    """Match an index/ETF alias without letting short names consume known suffixes."""
    pattern = re.escape(alias)
    if not alias.endswith("ETF") and not alias.endswith("指"):
        pattern += r"(?!ETF|指)"
    return pattern


def _nearest_entity_fact_claims(
    text_value: str,
    direction_items: list[tuple[str, list[str], dict]],
    fact_tokens: list[str],
    max_distance: int = 40,
):
    """Bind each fact token to the nearest entity in the same sentence."""
    mentions = []
    seen_mentions = set()
    for label, aliases, move in direction_items:
        for alias in sorted(set(aliases), key=len, reverse=True):
            for mention in re.finditer(_direction_entity_pattern(alias), text_value):
                key = (label, mention.start(), mention.end())
                if key in seen_mentions:
                    continue
                seen_mentions.add(key)
                mentions.append((mention.start(), mention.end(), label, move))

    if not mentions or not fact_tokens:
        return

    mentions.sort(key=lambda item: (item[0], item[1]))
    token_pattern = re.compile(
        "|".join(re.escape(token) for token in sorted(fact_tokens, key=len, reverse=True))
    )
    for token_match in token_pattern.finditer(text_value):
        sentence_start = max(
            text_value.rfind("\n", 0, token_match.start()),
            text_value.rfind("。", 0, token_match.start()),
            text_value.rfind("；", 0, token_match.start()),
        ) + 1
        sentence_end_candidates = [
            position
            for position in (
                text_value.find("\n", token_match.end()),
                text_value.find("。", token_match.end()),
                text_value.find("；", token_match.end()),
            )
            if position >= 0
        ]
        sentence_end = min(sentence_end_candidates) if sentence_end_candidates else len(text_value)

        clause_start = max(
            text_value.rfind("，", sentence_start, token_match.start()),
            text_value.rfind(",", sentence_start, token_match.start()),
            text_value.rfind("、", sentence_start, token_match.start()),
        ) + 1
        clause_end_candidates = [
            position
            for position in (
                text_value.find("，", token_match.end(), sentence_end),
                text_value.find(",", token_match.end(), sentence_end),
                text_value.find("、", token_match.end(), sentence_end),
            )
            if position >= 0
        ]
        clause_end = min(clause_end_candidates) if clause_end_candidates else sentence_end

        candidates = []
        for start, end, label, move in mentions:
            if start < sentence_start or end > sentence_end:
                continue
            if end <= token_match.start():
                distance = token_match.start() - end
                follows_entity = 0
            elif start >= token_match.end():
                distance = start - token_match.end()
                follows_entity = 1
            else:
                distance = 0
                follows_entity = 0
            if distance <= max_distance:
                candidates.append((distance, follows_entity, start, end, label, move))

        if not candidates:
            continue
        clause_candidates = [
            item for item in candidates if item[2] >= clause_start and item[3] <= clause_end
        ]
        if clause_candidates:
            selected = min(clause_candidates, key=lambda item: (item[0], item[1]))
        else:
            preceding = [item for item in candidates if item[3] <= token_match.start()]
            following = [item for item in candidates if item[2] >= token_match.end()]
            selected = min(preceding or following, key=lambda item: (item[0], item[1]))

        _, _, start, end, label, move = selected
        relation = text_value[min(start, token_match.start()):max(end, token_match.end())]
        yield label, move, token_match.group(0), relation


def _daily_return_polarity(move: dict) -> int:
    pct_change = float((move or {}).get("pct_change") or 0.0)
    if pct_change > 0.05:
        return 1
    if pct_change < -0.05:
        return -1
    return 0


def _candle_polarity(move: dict) -> int:
    candle = str((move or {}).get("candle") or "")
    if "阳线" in candle:
        return 1
    if "阴线" in candle:
        return -1
    candle_change = (move or {}).get("candle_change_pct")
    if candle_change is None:
        return 0
    candle_change = float(candle_change or 0.0)
    if candle_change > 0.05:
        return 1
    if candle_change < -0.05:
        return -1
    return 0


def _validate_programmatic_stock_sector(html_content: str, snapshot: dict) -> tuple[list[str], bool]:
    """Validate exact program-rendered sector rows; return (violations, markers_found)."""
    soup = BeautifulSoup(str(html_content or ""), "html.parser")
    stock_node = soup.find(attrs={"data-report-slot": "stock-sector"})
    markers = stock_node.find_all(attrs={"data-sector-name": True}) if stock_node else []
    if not markers:
        return [], False

    expected = []
    for direction, key in (("in", "sector_top_in"), ("out", "sector_top_out")):
        for rank, row in enumerate(list((snapshot or {}).get(key) or []), start=1):
            expected.append((direction, rank, row))

    violations = []
    if len(markers) != len(expected):
        violations.append(
            f"股票板块程序资金条目数量异常：页面={len(markers)}，真值={len(expected)}"
        )

    expected_names = {str(row.get("display_name") or "").strip() for _, _, row in expected}
    actual_names = [str(marker.get("data-sector-name") or "").strip() for marker in markers]
    for name in sorted(set(actual_names) - expected_names):
        violations.append(f"股票板块出现非Top3程序行业：{name}")

    for direction, rank, row in expected:
        name = str(row.get("display_name") or "").strip()
        matches = [marker for marker in markers if marker.get("data-sector-name") == name]
        if len(matches) != 1:
            violations.append(f"股票板块程序行业数量异常：{name}={len(matches)}")
            continue
        marker = matches[0]
        if marker.get("data-sector-direction") != direction:
            violations.append(f"{name}程序资金流方向分组错误")
        if str(marker.get("data-sector-rank") or "") != str(rank):
            violations.append(f"{name}程序资金排名错误：页面={marker.get('data-sector-rank')}，真值={rank}")

        expected_amount = float(row.get("main_flow_yi") or 0.0)
        try:
            attribute_amount = float(marker.get("data-main-flow-yi"))
        except (TypeError, ValueError):
            violations.append(f"{name}程序资金属性金额无效")
            continue
        amount_match = re.search(r"(?P<amount>[+\-]?\d+(?:\.\d+)?)\s*亿", marker.get_text(" ", strip=True))
        if not amount_match:
            violations.append(f"{name}程序资金正文缺少亿元金额")
            continue
        displayed_amount = float(amount_match.group("amount"))
        tolerance = max(SECTOR_FLOW_AMOUNT_TOLERANCE_YI, abs(expected_amount) * 0.015)
        if abs(attribute_amount - expected_amount) > 0.0001:
            violations.append(
                f"{name}程序资金属性与真值不一致：页面={attribute_amount:+.4f}亿，"
                f"真值={expected_amount:+.4f}亿"
            )
        if abs(displayed_amount - expected_amount) > tolerance:
            violations.append(
                f"{name}主力净额与真值不一致：页面={displayed_amount:+.1f}亿，"
                f"真值={expected_amount:+.1f}亿"
            )
    return violations, True


def validate_a_share_report_facts(html_content: str, snapshot: dict) -> list[str]:
    """Reject stale/invented A-share directions, sector amounts and ETF IV ranks."""
    violations = []
    if not html_content:
        return ["HTML为空，无法校验A股事实"]
    if not snapshot:
        return ["缺少A股程序真值，禁止发布"]

    structured_sector_violations, has_structured_sector_rows = _validate_programmatic_stock_sector(
        html_content,
        snapshot,
    )
    violations.extend(structured_sector_violations)

    plain = _report_html_to_plain_text(html_content)
    stock_section = _extract_plain_section(plain, "股票板块", ["期货商持仓", "商品期货全景"])
    if not stock_section:
        violations.append("缺少“股票板块”区块，无法核验板块资金")
    else:
        if not has_structured_sector_rows:
            required_sector_rows = list(snapshot.get("sector_top_in") or []) + list(snapshot.get("sector_top_out") or [])
            for row in required_sector_rows:
                name = str(row.get("display_name") or "").strip()
                if name and not re.search(_sector_entity_pattern(name), stock_section):
                    violations.append(f"股票板块未列出主力资金Top3必选行业：{name}")

            # Legacy/manual HTML fallback: exact bounded names must match DB main_net_inflow.
            for name, row in sorted(
                (snapshot.get("sectors") or {}).items(),
                key=lambda item: len(item[0]),
                reverse=True,
            ):
                amount_pattern = re.compile(
                    rf"{_sector_entity_pattern(name)}\s*[（(]?\s*"
                    rf"(?P<amount>[+\-]?\d+(?:\.\d+)?)\s*亿",
                    re.I,
                )
                for match in amount_pattern.finditer(stock_section):
                    actual_amount = float(match.group("amount"))
                    expected_amount = float(row.get("main_flow_yi") or 0.0)
                    tolerance = max(
                        SECTOR_FLOW_AMOUNT_TOLERANCE_YI,
                        abs(expected_amount) * 0.015,
                    )
                    if abs(actual_amount - expected_amount) > tolerance:
                        violations.append(
                            f"{name}主力净额与真值不一致：页面={actual_amount:+.1f}亿，"
                            f"真值={expected_amount:+.1f}亿"
                        )

        # Program-rendered rows already carry exact direction/rank/amount markers.
        # Fuzzy prose matching is kept only for legacy/manual HTML, where it cannot
        # accidentally bridge the end of the inflow list and the outflow heading.
        if not has_structured_sector_rows:
            positive_flow_tokens = "净流入|资金流入|资金涌入|主力流入|抢筹|吸金"
            negative_flow_tokens = "净流出|资金流出|资金撤离|主力流出|抛售|出逃"
            for name, row in (snapshot.get("sectors") or {}).items():
                expected_amount = float(row.get("main_flow_yi") or 0.0)
                if abs(expected_amount) < 0.5:
                    continue
                name_pat = _sector_entity_pattern(name)
                positive_claim = re.search(
                    rf"(?:{name_pat}.{{0,16}}(?:{positive_flow_tokens})|"
                    rf"(?:{positive_flow_tokens}).{{0,16}}{name_pat})",
                    stock_section,
                )
                negative_claim = re.search(
                    rf"(?:{name_pat}.{{0,16}}(?:{negative_flow_tokens})|"
                    rf"(?:{negative_flow_tokens}).{{0,16}}{name_pat})",
                    stock_section,
                )
                if expected_amount < 0 and positive_claim:
                    violations.append(f"{name}真实主力净流出 {expected_amount:+.1f}亿，但页面写成资金流入")
                if expected_amount > 0 and negative_claim:
                    violations.append(f"{name}真实主力净流入 {expected_amount:+.1f}亿，但页面写成资金流出")

    # Close-vs-previous-close return and close-vs-open candle direction are
    # independent facts. Gap moves can legitimately rise with a bearish candle
    # or fall with a bullish candle, so validate the two vocabularies separately.
    return_token_polarity = {
        "涨停": 1, "大涨": 1, "收涨": 1, "上涨": 1,
        "跌停": -1, "大跌": -1, "收跌": -1, "下跌": -1,
    }
    candle_token_polarity = {
        "大阳线": 1, "阳线": 1,
        "大阴线": -1, "阴线": -1,
    }
    direction_items = []
    for name, move in (snapshot.get("indices") or {}).items():
        direction_items.append((name, [name], move))
    for name, move in (snapshot.get("etfs") or {}).items():
        direction_items.append((f"{name}ETF", [f"{name}ETF", name], move))

    seen_direction_errors = set()
    for label, move, token, relation in _nearest_entity_fact_claims(
        plain,
        direction_items,
        list(return_token_polarity),
    ):
        if _is_conditional_context(relation):
            continue
        expected_polarity = _daily_return_polarity(move)
        if expected_polarity and return_token_polarity[token] != expected_polarity:
            key = (label, "return", token)
            if key not in seen_direction_errors:
                seen_direction_errors.add(key)
                pct_change = float(move.get("pct_change") or 0.0)
                violations.append(
                    f"{label}当日涨跌幅 {pct_change:+.2f}%，但页面出现相反方向表述“{token}”"
                )

    for label, move, token, relation in _nearest_entity_fact_claims(
        plain,
        direction_items,
        list(candle_token_polarity),
    ):
        if _is_conditional_context(relation):
            continue
        expected_polarity = _candle_polarity(move)
        if expected_polarity and candle_token_polarity[token] != expected_polarity:
            key = (label, "candle", token)
            if key not in seen_direction_errors:
                seen_direction_errors.add(key)
                candle = str(move.get("candle") or "未知")
                violations.append(
                    f"{label}当日K线形态为“{candle}”，但页面出现相反K线表述“{token}”"
                )

    iv_section = _extract_plain_section(plain, "期权波动率", ["每日牛股", "风险警示", "明日策略"])
    if not iv_section:
        violations.append("缺少“期权波动率”区块，无法核验ETF IV")
    else:
        for name, iv_info in (snapshot.get("etf_iv") or {}).items():
            iv_pattern = re.compile(
                rf"{_etf_entity_pattern(name)}(?P<body>.{{0,120}}?)"
                rf"(?P<rank>\d+(?:\.\d+)?)\s*%\s*[（(]?\s*"
                rf"(?P<level>极低|偏低|低|中等|中|偏高|高|极高)",
                re.S,
            )
            match = iv_pattern.search(iv_section)
            if not match:
                violations.append(f"{name}缺少可核验的IV Rank百分比与等级")
                continue
            actual_rank = float(match.group("rank"))
            expected_rank = float(iv_info.get("iv_rank") or 0.0)
            if abs(actual_rank - expected_rank) > ETF_IV_RANK_TOLERANCE:
                violations.append(
                    f"{name} IV Rank与真值不一致：页面={actual_rank:.1f}%，"
                    f"真值={expected_rank:.1f}%"
                )
            actual_level = match.group("level")
            expected_level = str(iv_info.get("level") or "")
            level_alias = {"中": "中等", "低": "低", "高": "高"}
            if level_alias.get(actual_level, actual_level) != level_alias.get(expected_level, expected_level):
                violations.append(
                    f"{name} IV等级与真值不一致：页面={actual_level}，真值={expected_level}"
                )

    return violations


def _inject_report_data_provenance(html_content: str, snapshot: dict) -> str:
    """Add a visible data-date marker so readers can audit report freshness."""
    report_date = str((snapshot or {}).get("report_date") or "")
    if not html_content or not report_date or 'data-market-trade-date=' in html_content:
        return html_content
    marker = (
        f'<p data-market-trade-date="{report_date}" '
        'style="color:#64748b; font-size:11px; margin:6px 0 0;">'
        f'A股行情 / 板块资金 / ETF IV 数据日：{report_date}</p>'
    )
    return html_content.replace("</p>", f"</p>{marker}", 1)


def _append_inline_style(node, declarations: str) -> None:
    existing = str(node.get("style") or "").strip().rstrip(";")
    node["style"] = f"{existing}; {declarations}" if existing else declarations


def _normalize_report_slot_fragment(slot_name: str, fragment: str) -> str:
    """Normalize rich list content without allowing it to influence the locked shell."""
    fragment_soup = BeautifulSoup(str(fragment or ""), "html.parser")
    if slot_name not in {"stock-sector", "futures-holding"}:
        return _slot_inner_html(fragment_soup)

    for list_node in fragment_soup.find_all(["ul", "ol"]):
        _append_inline_style(
            list_node,
            "list-style:none; margin:0; padding:0; color:#e2e8f0; text-align:left",
        )
        items = list_node.find_all("li", recursive=False)
        for item in items:
            _append_inline_style(
                item,
                "color:#e2e8f0; margin:0 0 10px 0; padding:0; line-height:1.8; text-align:left",
            )
        if items:
            _append_inline_style(items[-1], "margin-bottom:0")
    for paragraph in fragment_soup.find_all("p"):
        _append_inline_style(paragraph, "color:#e2e8f0; margin:0 0 8px 0; line-height:1.8; text-align:left")
    for strong in fragment_soup.find_all("strong"):
        _append_inline_style(strong, "color:#f8fafc; font-weight:700")
    return _slot_inner_html(fragment_soup)


def _render_programmatic_stock_sector(snapshot: dict) -> str:
    """Render the exact, unique Top3 sector facts without any LLM-authored numbers."""
    groups = (
        ("in", "主力净流入Top3：", list((snapshot or {}).get("sector_top_in") or [])),
        ("out", "主力净流出Top3：", list((snapshot or {}).get("sector_top_out") or [])),
    )
    list_items = []
    for direction, label, rows in groups:
        entries = []
        for rank, row in enumerate(rows, start=1):
            name = str(row.get("display_name") or "").strip()
            amount = float(row.get("main_flow_yi") or 0.0)
            if not name:
                continue
            entries.append(
                f'<span data-sector-name="{escape(name, quote=True)}" '
                f'data-main-flow-yi="{amount:.4f}" '
                f'data-sector-direction="{direction}" data-sector-rank="{rank}">'
                f'{escape(name)}({amount:+.1f}亿)</span>'
            )
        list_items.append(
            f'<li data-sector-flow-group="{direction}"><strong>{label}</strong>'
            f'{"；".join(entries)}</li>'
        )
    return '<ul data-sector-flow-source="programmatic">' + "".join(list_items) + "</ul>"


def _lock_programmatic_stock_sector(slots: dict, snapshot: dict) -> dict:
    """Return slot content with the stock-sector facts forcibly replaced by DB truth."""
    locked = dict(slots or {})
    if snapshot is None:
        return locked
    locked["stock-sector"] = _render_programmatic_stock_sector(snapshot)
    return locked


def _render_locked_report_layout(slots: dict, today: str, weekday: str) -> str:
    """Render the existing visual shell while allowing content only in named slots."""
    values = {
        name: _normalize_report_slot_fragment(name, str((slots or {}).get(name) or ""))
        for name in REPORT_SLOT_ORDER
    }
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    @media screen and (max-width: 640px) {{
      .two-col-table {{ width: 100% !important; }}
      .two-col-table td {{ display: block !important; width: 100% !important; padding: 6px 0 !important; }}
      .main-container {{ padding: 20px 16px !important; }}
      .section-title {{ font-size: 18px !important; }}
      .card-content {{ padding: 16px !important; }}
    }}
    @media screen {{
      .glass-card {{
        backdrop-filter: blur(12px) !important; -webkit-backdrop-filter: blur(12px) !important;
        background: rgba(30, 41, 59, 0.8) !important; border: 1px solid rgba(255,255,255,0.08) !important;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3) !important;
      }}
      .glass-header {{
        backdrop-filter: blur(16px) !important; -webkit-backdrop-filter: blur(16px) !important;
        background: rgba(15, 23, 42, 0.9) !important;
      }}
    }}
  </style>
</head>
<body style="margin:0; padding:0; background:#0f172a; font-family:'PingFang SC','Microsoft YaHei',sans-serif;">
<div class="main-container" style="max-width:700px; margin:0 auto; padding:30px 24px; background:linear-gradient(180deg,#0f172a 0%,#1e293b 100%);">
  <div class="glass-header" style="text-align:center; padding:32px 24px; border-radius:20px; background:rgba(15,23,42,0.9); border:1px solid rgba(255,255,255,0.08); margin-bottom:28px;">
    <div style="font-size:13px; color:#64748b; letter-spacing:2px; margin-bottom:8px;">AIPROTA DAILY REPORT</div>
    <h1 style="color:#fbbf24; font-size:26px; margin:0; font-weight:700; letter-spacing:2px;">📊 爱波塔复盘晚报</h1>
    <p style="color:#64748b; font-size:14px; margin-top:12px;">{today} {weekday} | 深度复盘</p>
  </div>
  <div style="margin-bottom:24px;">
    <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;"><span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>🚀 市场头条</h2>
    <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
      <p data-report-slot="market-headline" style="color:#e2e8f0; font-size:14px; margin:0; line-height:1.9;">{values['market-headline']}</p>
    </div>
  </div>
  <div style="margin-bottom:24px;">
    <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;"><span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>💰 资金暗流</h2>
    <table class="two-col-table" data-report-layout="fund-flow" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
      <td width="50%" style="padding:0 6px 12px 0;" valign="top"><div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06); height:100%;">
        <h4 style="color:#94a3b8; margin:0 0 10px 0; font-size:14px; font-weight:600;">📈 股票板块</h4>
        <div data-report-slot="stock-sector" style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.8; text-align:left;">{values['stock-sector']}</div>
      </div></td>
      <td width="50%" style="padding:0 0 12px 6px;" valign="top"><div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06); height:100%;">
        <h4 style="color:#94a3b8; margin:0 0 10px 0; font-size:14px; font-weight:600;">📊 期货商持仓</h4>
        <div data-report-slot="futures-holding" style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.8; text-align:left;">{values['futures-holding']}</div>
      </div></td>
    </tr></table>
  </div>
  <div style="margin-bottom:24px;">
    <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;"><span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>🏆 商品期货全景</h2>
    <table class="two-col-table" data-report-slot="commodity-panorama" width="100%" cellpadding="0" cellspacing="0" border="0">{values['commodity-panorama']}</table>
  </div>
  <div style="margin-bottom:24px;">
    <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;"><span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>⚖️ 期权波动率</h2>
    <div data-report-slot="option-volatility" class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">{values['option-volatility']}</div>
  </div>
  <div style="margin-bottom:24px;">
    <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;"><span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>🐂 每日牛股</h2>
    <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);"><p data-report-slot="daily-bull" style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.9;">{values['daily-bull']}</p></div>
  </div>
  <div style="margin-bottom:24px;">
    <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;"><span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>🐻 风险警示</h2>
    <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);"><p data-report-slot="risk-warning" style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.9;">{values['risk-warning']}</p></div>
  </div>
  <div style="margin-bottom:24px;"><div class="glass-card" style="background:rgba(251,191,36,0.08); padding:20px; border-radius:14px; border:1px solid rgba(251,191,36,0.25);">
    <h2 style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600;">💡 明日策略</h2>
    <p data-report-slot="tomorrow-strategy" style="color:#e2e8f0; font-size:14px; line-height:1.9; margin:0;">{values['tomorrow-strategy']}</p>
  </div></div>
  <div style="text-align:center; padding:20px 0; border-top:1px solid rgba(255,255,255,0.06);">
    <p data-report-slot="footer-quote" style="color:#64748b; font-size:13px; font-style:italic; margin:0;">{values['footer-quote']}</p>
    <p style="color:#475569; font-size:12px; margin-top:14px;">爱波塔 · 最懂期权的AI | www.aiprota.com</p>
  </div>
</div>
</body>
</html>"""


def _slot_inner_html(node) -> str:
    return "".join(str(child) for child in node.contents).strip() if node else ""


def _find_report_slot_node(soup: BeautifulSoup, slot_name: str):
    node = soup.find(attrs={"data-report-slot": slot_name})
    if node is not None:
        return node

    label = REPORT_SLOT_LABELS.get(slot_name, "")
    heading = soup.find(
        lambda tag: tag.name in {"h2", "h4"} and label in tag.get_text(" ", strip=True)
    )
    if heading is None:
        if slot_name == "footer-quote":
            brand = soup.find(string=lambda value: value and "www.aiprota.com" in value)
            brand_node = brand.parent if brand else None
            return brand_node.find_previous_sibling("p") if brand_node else None
        return None
    if slot_name == "commodity-panorama":
        return heading.find_next("table", class_="two-col-table")
    if slot_name in {"market-headline", "stock-sector", "futures-holding", "daily-bull", "risk-warning", "tomorrow-strategy"}:
        return heading.find_next("p")
    if slot_name == "option-volatility":
        return heading.find_next("div", class_="glass-card")
    return None


def _fragment_has_forbidden_markup(fragment: str, slot_name: str) -> bool:
    fragment_soup = BeautifulSoup(str(fragment or ""), "html.parser")
    if any(fragment_soup.find(tag_name) is not None for tag_name in REPORT_REPAIR_FORBIDDEN_TAGS):
        return True
    if fragment_soup.find(attrs={"data-report-slot": True}) is not None:
        return True
    if fragment_soup.find(attrs={"data-report-layout": True}) is not None:
        return True
    paragraph_slots = {
        "market-headline", "daily-bull", "risk-warning", "tomorrow-strategy", "footer-quote",
    }
    if slot_name in paragraph_slots and fragment_soup.find(["p", "table", "section"]):
        return True
    if slot_name in {"stock-sector", "futures-holding"} and fragment_soup.find(["table", "section"]):
        return True
    return slot_name == "commodity-panorama" and fragment_soup.find("table") is not None


def _extract_report_slots(html_content: str) -> dict:
    soup = BeautifulSoup(str(html_content or ""), "html.parser")
    slots = {}
    for slot_name in REPORT_SLOT_ORDER:
        fragment = _slot_inner_html(_find_report_slot_node(soup, slot_name))
        slots[slot_name] = "" if _fragment_has_forbidden_markup(fragment, slot_name) else fragment
    return slots


def validate_report_layout(html_content: str) -> list[str]:
    """Validate the locked shell independently from market-fact validation."""
    anomalies = []
    soup = BeautifulSoup(str(html_content or ""), "html.parser")
    if soup.find("div", class_="main-container") is None:
        anomalies.append("排版缺少 main-container 外层容器")
    if soup.find("div", class_="glass-header") is None:
        anomalies.append("排版缺少 glass-header 头部")
    if soup.find("script") is not None:
        anomalies.append("排版包含禁止的 script 标签")
    for style_node in soup.find_all("style"):
        if style_node.find_parent("head") is None:
            anomalies.append("正文插槽包含禁止的 style 标签")

    html_text = str(html_content or "")
    positions = []
    for slot_name in REPORT_SLOT_ORDER:
        nodes = soup.find_all(attrs={"data-report-slot": slot_name})
        if len(nodes) != 1:
            anomalies.append(f"排版插槽 {slot_name} 数量异常：{len(nodes)}")
        else:
            positions.append(html_text.find(f'data-report-slot="{slot_name}"'))
            if slot_name != "commodity-panorama" and not nodes[0].get_text(" ", strip=True):
                anomalies.append(f"排版插槽 {slot_name} 内容为空")
    if positions != sorted(positions):
        anomalies.append("排版插槽顺序异常")

    fund_table = soup.find("table", attrs={"data-report-layout": "fund-flow"})
    if fund_table is None or len(fund_table.find_all("td")) < 2:
        anomalies.append("资金暗流双栏结构不完整")
    commodity_table = soup.find(attrs={"data-report-slot": "commodity-panorama"})
    commodity_cards = commodity_table.find_all("td") if commodity_table else []
    if len(commodity_cards) != len(COMMODITY_CARD_LIST):
        anomalies.append(
            f"商品期货全景卡片数量异常：页面={len(commodity_cards)}，要求={len(COMMODITY_CARD_LIST)}"
        )
    return anomalies


def _plain_fragment(fragment: str) -> str:
    return BeautifulSoup(str(fragment or ""), "html.parser").get_text(" ", strip=True)


def _classify_repair_slots(anomalies: list, html_content: str) -> list[str]:
    slots = _extract_report_slots(html_content)
    targets = set()
    commodity_tokens = tuple(COMMODITY_CARD_LIST) + ("商品期货全景", "商品卡片", "趋势标签", "形态字段")
    sector_tokens = ("股票板块", "主力资金Top3", "主力净额", "真实主力净流", "资金流入", "资金流出")
    iv_tokens = ("IV Rank", "IV等级", "可核验的IV", "期权波动率")
    narrative_slots = (
        "market-headline", "stock-sector", "futures-holding", "option-volatility",
        "daily-bull", "risk-warning", "tomorrow-strategy",
    )
    direction_entities = list(A_SHARE_INDEX_MAP) + list(ETF_OPTION_SNAPSHOT_MAP)

    for anomaly in anomalies:
        message = str(anomaly)
        if any(token in message for token in commodity_tokens):
            targets.add("commodity-panorama")
        if any(token in message for token in sector_tokens):
            targets.add("stock-sector")
        if any(token in message for token in iv_tokens):
            targets.add("option-volatility")
        if (
            "当日涨跌幅" in message
            or "相反方向" in message
            or "当日K线形态" in message
            or "相反K线" in message
        ):
            entities = [entity for entity in direction_entities if entity in message]
            located = {
                slot_name
                for slot_name in narrative_slots
                if any(entity in _plain_fragment(slots.get(slot_name, "")) for entity in entities)
            }
            targets.update(located or {"market-headline", "option-volatility", "tomorrow-strategy"})
        if message.startswith("排版插槽 "):
            targets.update(slot_name for slot_name in REPORT_SLOT_ORDER if slot_name in message)

    return [slot_name for slot_name in REPORT_SLOT_ORDER if slot_name in targets] or list(REPORT_SLOT_ORDER)


def _programmatic_stock_sector_failures(anomalies: list) -> list[str]:
    """Identify stock-sector failures that must never be delegated back to the LLM."""
    tokens = (
        "股票板块",
        "主力资金Top3",
        "主力净额",
        "真实主力净流",
        "程序资金",
    )
    return [str(item) for item in anomalies if any(token in str(item) for token in tokens)]


def _parse_slot_repair_response(response_text: str, allowed_slots: list[str]) -> tuple[dict, list[str]]:
    cleaned = str(response_text or "").replace("```html", "").replace("```", "").strip()
    soup = BeautifulSoup(cleaned, "html.parser")
    if any(soup.find(tag_name) is not None for tag_name in REPORT_REPAIR_FORBIDDEN_TAGS):
        return {}, ["<page-markup>"]
    repair_root = soup.find("report-repair")
    if repair_root is None:
        return {}, ["<invalid-wrapper>"]
    repairs = {}
    rejected = []
    allowed = set(allowed_slots)
    for node in repair_root.find_all("section", attrs={"data-slot": True}, recursive=False):
        slot_name = str(node.get("data-slot") or "").strip()
        if slot_name not in allowed or slot_name not in REPORT_SLOT_ORDER:
            rejected.append(slot_name or "<empty>")
            continue
        fragment = _slot_inner_html(node)
        if _fragment_has_forbidden_markup(fragment, slot_name):
            rejected.append(slot_name)
            continue
        repairs[slot_name] = fragment
    return repairs, rejected


def _write_failed_daily_report(html_content: str, snapshot: dict) -> str:
    report_date = str((snapshot or {}).get("report_date") or _current_trade_date_key())
    os.makedirs("outputs", exist_ok=True)
    timestamp = datetime.now().strftime("%H%M%S")
    output_path = os.path.join("outputs", f"failed_daily_report_{report_date}_{timestamp}.html")
    with open(output_path, "w", encoding="utf-8") as output_file:
        output_file.write(str(html_content or ""))
    return output_path


def _write_daily_report_audit(snapshot: dict, raw_material: str) -> str:
    """Persist the reporter material and deterministic truth set for later incident review."""
    report_date = str((snapshot or {}).get("report_date") or _current_trade_date_key())
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)
    audit_path = os.path.join(output_dir, f"daily_report_audit_{report_date}.json")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "report_date": report_date,
        "a_share_snapshot": snapshot,
        "reporter_material": str(raw_material or ""),
    }
    with open(audit_path, "w", encoding="utf-8") as audit_file:
        json.dump(payload, audit_file, ensure_ascii=False, indent=2, default=str)
    return audit_path


def _rewrite_report_slots_after_validation(
    raw_material: str,
    html: str,
    anomalies: list,
    cumulative_anomalies: list,
    target_slots: list[str],
    round_idx: int,
    today: str,
    weekday: str,
    iv_snapshot_text: str = "",
    kline_snapshot_text: str = "",
    a_share_snapshot_text: str = "",
    a_share_snapshot: dict = None,
) -> str:
    """Repair allowlisted slot content, then put it back into the locked report shell."""
    current_slots = _extract_report_slots(html)
    anomaly_text = "\n".join(f"- {item}" for item in anomalies)
    history_text = "\n".join(f"- {item}" for item in cumulative_anomalies)
    target_text = "\n\n".join(
        f'<section data-slot="{slot_name}">\n{current_slots.get(slot_name, "")}\n</section>'
        for slot_name in target_slots
    )
    rewrite_prompt = f"""
你正在修复《每日深度复盘》的局部内容。页面排版由程序锁定，你只能返回指定内容插槽的内部HTML。

【第{round_idx}轮校验发现的问题】
{anomaly_text}

【此前出现过的问题，禁止重新引入】
{history_text}

【强制要求】
1. 只返回下方列出的 data-slot，不得增加未知插槽。
2. 输出格式必须是：<report-repair><section data-slot="插槽名">内部HTML</section></report-repair>。
3. section 内不要返回外层 p、table、html、head、body、style、script 或 Markdown代码块。
4. 未列出的插槽禁止改写；已正确内容尽量原样保留。
5. 商品期货全景必须保留10个商品卡片，形态、趋势和隐含波动率必须逐字匹配程序真值。
6. 股票板块只能使用A股真值中的主力净流入Top3和净流出Top3，金额必须保留正负号。
7. 指数/ETF涨跌、K线阴阳、ETF IV Rank和等级必须逐字匹配程序真值。
8. 记者素材与程序真值冲突时忽略记者素材；不得把下跌写成上涨、流出写成流入。

【商品IV真值】
{iv_snapshot_text}

【商品K线形态/趋势真值】
{kline_snapshot_text}

【A股/ETF/板块资金真值】
{a_share_snapshot_text}

【记者素材】
{raw_material}

【本轮允许修复的插槽】
{target_text}
"""
    response = invoke_report_llm_with_fallback(
        rewrite_llm,
        [HumanMessage(content=rewrite_prompt)],
        env_prefix=REPORT_LLM_ENV_PREFIX,
        temperature=REPORT_REWRITE_TEMPERATURE,
    )
    repairs, rejected = _parse_slot_repair_response(response.content, target_slots)
    if rejected:
        print(f"⚠️ [局部修复] 拒绝非法或未知插槽: {', '.join(rejected)}")
    if not repairs:
        print("⚠️ [局部修复] 模型未返回可接受的插槽，保留上一版内容")
        return html
    for slot_name, fragment in repairs.items():
        current_slots[slot_name] = fragment
    print(f"🧩 [局部修复] 已替换插槽: {', '.join(repairs)}")
    current_slots = _lock_programmatic_stock_sector(current_slots, a_share_snapshot)
    return _render_locked_report_layout(current_slots, today, weekday)


# ==========================================
# 2. 定义【AI 首席记者】(The Reporter)
# ==========================================
def _get_recent_holding_dates():
    """Return latest two futures_holding trade dates as (start_date, end_date) in YYYYMMDD."""
    try:
        sql = """
            SELECT REPLACE(trade_date, '-', '') AS trade_date
            FROM futures_holding
            GROUP BY REPLACE(trade_date, '-', '')
            ORDER BY trade_date DESC
            LIMIT 2
        """
        df_dates = pd.read_sql(sql, engine)
        if len(df_dates) >= 2:
            end_date = str(df_dates.iloc[0]["trade_date"])
            start_date = str(df_dates.iloc[1]["trade_date"])
            return start_date, end_date
        if len(df_dates) == 1:
            only_date = str(df_dates.iloc[0]["trade_date"])
            return only_date, only_date
    except Exception as e:
        print(f"[holding-dates] fetch failed: {e}")

    fallback = datetime.now().strftime("%Y%m%d")
    return fallback, fallback

def collect_data_via_agent(report_trade_date: str = None):
    """
    🔥 派出 AI 记者去采集素材
    """
    print("🕵️‍♂️ [AI记者] 正在出发采集全市场情报 (ReAct 模式)...")

    tools = [
        # 舆情类
        get_finance_related_trends,
        get_today_hotlist,
        tool_get_polymarket_sentiment,
        get_financial_news,
        search_web,

        # 资金类
        tool_get_retail_money_flow,
        get_futures_fund_flow,
        search_broker_holdings_on_date,
        tool_analyze_broker_positions,

        # 期权/技术类
        get_commodity_iv_info,
        analyze_etf_option_sentiment,
        get_option_volume_abnormal,
        analyze_kline_pattern,

        # 选股类
        search_top_stocks
    ]

    trade_date_key, today_str, _ = _report_date_context(report_trade_date)
    holdings_start_date, holdings_end_date = _get_recent_holding_dates()
    system_prompt = f"""
    你是一位**顶级财经记者**，正在为今天的《晚间深度复盘日报》采集素材。
    当前日期：{today_str}。

    【你的任务目标】：
    利用手中的工具，主动发现今日市场的**核心噱头**和**异常数据**。

    【采集策略 (思维链)】：

    ## 第一步：先找热点
    - 调用 `get_financial_news` 看当天财经新闻
    - 用 `get_finance_related_trends` 或 `get_today_hotlist` 看今天大家在讨论什么
    - 发现热点后，可以针对热点去调用 search_web 挖掘细节

    ## 第二步：宏观预测
    - 针对今天的热点事件（如美联储、地缘），调用 `tool_get_polymarket_sentiment` 看市场押注概率

    ## 第三步：资金流向
    - 必须调用 `tool_get_retail_money_flow(days=1, as_of_date="{trade_date_key}")`，
      看当日股票主力资金前3大流出和流入板块
    - 如果工具返回“数据未就绪”或数据日期不是 {trade_date_key}，必须明确标记数据缺失，
      严禁拿上一交易日数据冒充今天，也不得自行补写板块金额

    ## 第四步：期货商持仓分析 
    - 调用 `search_broker_holdings_on_date` 记录以下期货商的前3大多头净持仓和前3大空头净持仓
    - 海通期货
    - 东证期货
    - 国泰君安


    ## 第五步：⚠️【必做】商品期货深度分析
    **这是强制任务，必须完成！**

    请对以下 10 个核心商品逐一调用 `analyze_kline_pattern` 做技术分析：
    1. **黄金** 
    2. **白银**  
    3. **原油** 
    4. **铜** 
    5. **碳酸锂** 
    6. **铁矿石** 
    7. **豆粕** 
    8. **橡胶** 
    9. **棉花**
    10.**PTA**

    ⚠️ 调用 `analyze_kline_pattern` 时必须传入 `trade_date="{trade_date_key}"`，
    不允许省略日期，避免历史晚报重跑时漂移到最新K线。

    对每个品种，记录：
    - 当前趋势（多/空/震荡）
    - K线形态（如大阳线、十字星、吞噬等）
    - 通过 get_commodity_iv_info 获取并记录“当前隐含波动率水平（高/中/低）”
    - 你的短期判断

    ## 第六步：ETF期权分析 (Options)
    记录以下 ETF 的期权IV等级和K线分析。调用 `get_commodity_iv_info` 时，
    query 必须逐字使用下列字符串，确保工具进入252日 Rank 分支，而不是只查5日IV：
    - `510300 IV等级`
    - `510500 IV等级`
    - `159915 IV等级`
    - `588000 IV等级`
    - `510050 IV等级`
    必须记录当前IV、252日IV Rank和等级，不是只记录当前IV。
    调用analyze_kline_pattern做最近几天技术面分析

    ## 第七步：选股与技术 (Picks)
    - 调用 `search_top_stocks` 选出 5 个今日出现突破的强势股
    - 再选出 5 个出现破位或下降三法的危险弱势股

    【输出要求】：
    请将你采集到的所有有价值的信息，整理成一篇**详细的素材笔记**返回。

    **特别注意**：商品期货分析部分必须包含完整的 10 个品种分析结果！
    如果某个品种查询失败，请注明并继续下一个。

    不要写成最终新闻稿，只要罗列事实、数据和你的发现即可，供主编后续使用。
    """


    system_prompt += f"""

    ## Broker holdings section must focus on day-over-day changes (MANDATORY)
    - Use `tool_analyze_broker_positions` for each broker below with exact date range `{holdings_start_date}` -> `{holdings_end_date}` and `sort_by='net'`.
    - Brokers: 海通期货、东证期货、国泰君安。
    - For each broker, extract top 3 products with the largest net increase and top 3 with the largest net decrease.
    - Prioritize "change" over absolute position size; avoid repeating fixed products unless they truly rank in daily changes.
    - If date range has no change data, then fallback to `search_broker_holdings_on_date` as backup snapshot.
    """
    reporter_agent = create_react_agent(llm, tools, prompt=system_prompt)

    try:
        trigger_msg = f"""开始今天的市场扫描任务，请确保：
        1. 覆盖宏观、资金、期权和选股四个维度
        2. ⚠️ 必须完成 10 个商品期货的技术分析（黄金、白银、原油、铜、铁矿石、碳酸锂、豆粕、橡胶、棉花、PTA）
        3. 每个商品都要给出趋势判断和隐含波动率水平（高/中/低）
        4. 调用 analyze_kline_pattern 时必须传入 trade_date="{trade_date_key}"
        """


        trigger_msg += f"""
        5. Use holdings change dates: {holdings_start_date} -> {holdings_end_date}
        6. In broker holdings, prioritize net position changes (increase/decrease top movers), not absolute net position rank.
        """
        result = reporter_agent.invoke(
            {"messages": [HumanMessage(content=trigger_msg)]},
            {"recursion_limit": 160}
        )

        collected_content = result["messages"][-1].content
        print("✅ [AI记者] 采集完成，素材已提交。")
        return collected_content

    except Exception as e:
        print(f"❌ [AI记者] 采集过程出错: {e}")
        return "AI 采集失败，请检查日志。"


def draft_report(raw_material, a_share_snapshot: dict = None, a_share_snapshot_text: str = None,
                 report_trade_date: str = None):
    """
    让 AI 主编基于记者提供的素材写稿
    🔥 v2.0 升级版：玻璃拟态 + 响应式 + 商品图标 + IV进度条
    """
    print("✏️ [AI主编] 正在撰写晚报...")

    trade_date_key, today, weekday = _report_date_context(report_trade_date)
    if a_share_snapshot is None or a_share_snapshot_text is None:
        a_share_snapshot, a_share_snapshot_text = _fetch_programmatic_a_share_snapshot(trade_date_key)
    print(
        f"🏦 [程序注入] A股/ETF/板块资金真值已生成: "
        f"交易日 {a_share_snapshot.get('report_date', '-')}"
    )
    iv_snapshot_map, iv_snapshot_text = _fetch_programmatic_commodity_iv_snapshot(trade_date_key)
    print(f"🧮 [程序注入] 商品IV真值已生成: {len(iv_snapshot_map)}/{len(COMMODITY_CARD_LIST)}")
    kline_snapshot_map, kline_snapshot_text = _fetch_programmatic_commodity_kline_snapshot(trade_date_key)
    print(f"📈 [程序注入] 商品K线形态真值已生成: {len(kline_snapshot_map)}/{len(COMMODITY_CARD_LIST)}")

    prompt = f"""
    你是【爱波塔首席投研】的主编。你的记者刚刚提交了今天的市场调研素材。
    请根据这些素材，写一份**《每日深度复盘》**。

    【程序查库得到的商品IV真值（最高优先级，必须原样使用）】
    {iv_snapshot_text}

    【程序查库得到的商品K线形态/趋势真值（最高优先级，必须原样使用）】
    {kline_snapshot_text}

    【程序查库得到的A股/ETF/板块资金真值（最高优先级，必须原样使用）】
    {a_share_snapshot_text}

    【记者提交的素材】：
    {raw_material}

    【写作要求】：

    ## 1. 格式：纯 HTML 代码（无 Markdown）

    ## 2. ⚠️ 核心设计理念
    - 配色统一简洁：所有板块标题统一用金色 #fbbf24
    - 邮件端：Table布局兜底 + 深色背景降级
    - 网页端：CSS增强（玻璃拟态、响应式）
    - 手机端：自动变为单列布局

    ## 3. 商品图标映射（必须使用）
    | 商品 | 图标 |
    |------|------|
    | 黄金 | 🪙 |
    | 白银 | 🥈 |
    | 原油 | 🛢️ |
    | 铜 | 🔶 |
    | 碳酸锂 | 🔋 |
    | 铁矿石 | ite |
    | 豆粕 | 🌱 |
    | 橡胶 | 🌴 |
    | 棉花 | 🌸 |
    | PTA | 🧪 |

    ## 4. ⚠️ 趋势标签颜色（中国市场：红涨绿跌）
    - **看多/偏多**：背景 #dc2626 (红色)，白字
    - **看空/偏空**：背景 #16a34a (绿色)，白字  
    - **震荡/中性**：背景 #d97706 (橙黄)，白字

    ## 5. IV等级进度条（数据可视化）
    ```html
    <div style="display:flex; align-items:center; gap:10px; margin:6px 0;">
      <span style="color:#94a3b8; min-width:85px; font-size:13px;">沪深300</span>
      <div style="flex:1; background:rgba(255,255,255,0.08); border-radius:4px; height:6px; overflow:hidden;">
        <div style="width:45%; height:100%; background:linear-gradient(90deg,#22c55e,#eab308,#ef4444); border-radius:4px;"></div>
      </div>
      <span style="color:#22c55e; min-width:55px; font-size:12px; text-align:right;">45% 中</span>
    </div>
    ```

    ## 6. 完整 HTML 模板：

    ```html
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <style>
        @media screen and (max-width: 640px) {{
          .two-col-table {{ width: 100% !important; }}
          .two-col-table td {{ 
            display: block !important; 
            width: 100% !important; 
            padding: 6px 0 !important;
          }}
          .main-container {{ padding: 20px 16px !important; }}
          .section-title {{ font-size: 18px !important; }}
          .card-content {{ padding: 16px !important; }}
        }}

        @media screen {{
          .glass-card {{
            backdrop-filter: blur(12px) !important;
            -webkit-backdrop-filter: blur(12px) !important;
            background: rgba(30, 41, 59, 0.8) !important;
            border: 1px solid rgba(255,255,255,0.08) !important;
            box-shadow: 0 8px 32px rgba(0,0,0,0.3) !important;
          }}
          .glass-header {{
            backdrop-filter: blur(16px) !important;
            -webkit-backdrop-filter: blur(16px) !important;
            background: rgba(15, 23, 42, 0.9) !important;
          }}
        }}
      </style>
    </head>
    <body style="margin:0; padding:0; background:#0f172a; font-family:'PingFang SC','Microsoft YaHei',sans-serif;">

    <div class="main-container" style="max-width:700px; margin:0 auto; padding:30px 24px; background:linear-gradient(180deg,#0f172a 0%,#1e293b 100%);">

      <!-- 头部 -->
      <div class="glass-header" style="text-align:center; padding:32px 24px; border-radius:20px; background:rgba(15,23,42,0.9); border:1px solid rgba(255,255,255,0.08); margin-bottom:28px;">
        <div style="font-size:13px; color:#64748b; letter-spacing:2px; margin-bottom:8px;">AIPROTA DAILY REPORT</div>
        <h1 style="color:#fbbf24; font-size:26px; margin:0; font-weight:700; letter-spacing:2px;">📊 爱波塔复盘晚报</h1>
        <p style="color:#64748b; font-size:14px; margin-top:12px;">{today} {weekday} | 深度复盘</p>
      </div>

      <!-- 🚀 市场头条 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          🚀 市场头条
        </h2>
        <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
          <p style="color:#e2e8f0; font-size:14px; margin:0; line-height:1.9;">
            <!-- 根据素材填写 -->
          </p>
        </div>
      </div>

      <!-- 💰 资金暗流 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          💰 资金暗流
        </h2>
        <table class="two-col-table" width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td width="50%" style="padding:0 6px 12px 0;" valign="top">
              <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06); height:100%;">
                <h4 style="color:#94a3b8; margin:0 0 10px 0; font-size:14px; font-weight:600;">📈 股票板块</h4>
                <p style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.8;">
                  <!-- 根据素材填写 -->
                </p>
              </div>
            </td>
            <td width="50%" style="padding:0 0 12px 6px;" valign="top">
              <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06); height:100%;">
                <h4 style="color:#94a3b8; margin:0 0 10px 0; font-size:14px; font-weight:600;">📊 期货商持仓</h4>
                <p style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.8;">
                  <!-- 根据素材填写 -->
                </p>
              </div>
            </td>
          </tr>
        </table>
      </div>

      <!-- 🏆 商品期货全景 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          🏆 商品期货全景
        </h2>

        <!-- 
        商品卡片模板：
        <td width="50%" style="padding:0 6px 10px 0;" valign="top">
          <div class="glass-card" style="background:rgba(30,41,59,0.6); padding:14px 16px; border-radius:12px; border:1px solid rgba(255,255,255,0.06);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
              <span style="color:#e2e8f0; font-weight:600; font-size:15px;">🪙 黄金</span>
              <span style="background:#dc2626; color:white; padding:3px 12px; border-radius:12px; font-size:12px; font-weight:500;">看多</span>
            </div>
            <p style="color:#94a3b8; font-size:12px; margin:0; line-height:1.6;">
              形态：xxx<br>隐含波动率：xx%（中）
            </p>
          </div>
        </td>

        趋势标签：看多=#dc2626, 看空=#16a34a, 震荡=#d97706
        -->

        <table class="two-col-table" width="100%" cellpadding="0" cellspacing="0" border="0">
          <!-- 5行2列，共10个商品 -->
        </table>
      </div>

      <!-- ⚖️ 期权波动率 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          ⚖️ 期权波动率
        </h2>
        <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
          <!-- IV进度条 + 分析文字 -->
        </div>
      </div>

      <!-- 🐂 每日牛股 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          🐂 每日牛股
        </h2>
        <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
          <p style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.9;">
            <!-- 用 <span style="color:#dc2626;">▸</span> 作为列表符号 -->
          </p>
        </div>
      </div>

      <!-- 🐻 风险警示 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          🐻 风险警示
        </h2>
        <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
          <p style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.9;">
            <!-- 用 <span style="color:#16a34a;">▸</span> 作为列表符号 -->
          </p>
        </div>
      </div>

      <!-- 💡 明日策略 -->
      <div style="margin-bottom:24px;">
        <div class="glass-card" style="background:rgba(251,191,36,0.08); padding:20px; border-radius:14px; border:1px solid rgba(251,191,36,0.25);">
          <h2 style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600;">
            💡 明日策略
          </h2>
          <p style="color:#e2e8f0; font-size:14px; line-height:1.9; margin:0;">
            <!-- 用 <strong style="color:#fbbf24;">【类别】</strong> 分类 -->
          </p>
        </div>
      </div>

      <!-- 底部 -->
      <div style="text-align:center; padding:20px 0; border-top:1px solid rgba(255,255,255,0.06);">
        <p style="color:#64748b; font-size:13px; font-style:italic; margin:0;">
          💬 "毒舌点评"
        </p>
        <p style="color:#475569; font-size:12px; margin-top:14px;">
          爱波塔 · 最懂期权的AI | www.aiprota.com
        </p>
      </div>

    </div>
    </body>
    </html>
    ```

    ## 7. 关键样式要求

    | 元素 | 字号 | 颜色 |
    |------|------|------|
    | 大标题 | 26px | #fbbf24 (金色) |
    | **所有板块标题** | 18px | **#fbbf24 (统一金色)** |
    | 卡片小标题 | 14px | #94a3b8 (灰色) |
    | 商品名称 | 15px | #e2e8f0 (亮白) |
    | 正文内容 | 13-14px | #e2e8f0 (亮白) |
    | 次要文字 | 12-13px | #94a3b8 (灰色) |

    ## 8. ⚠️ 商品期货趋势标签规则
    - 商品卡片趋势标签必须逐字匹配【程序查库得到的商品K线形态/趋势真值】中的“趋势=看多/看空/震荡”。
    - 不得根据宏观新闻、期货商持仓或主观短评覆盖商品K线趋势真值。
    - 看多背景 #dc2626；看空背景 #16a34a；震荡背景 #d97706。

    ## 9. 内容要求
    - 股票板块只允许使用【A股/ETF/板块资金真值】中的主力净流入Top3和主力净流出Top3；
      金额统一写成“行业名(+/-X.X亿)”，必须保留正负号
    - 期权波动率进度条百分比代表252日 IV Rank，数值和等级必须逐字匹配A股程序真值
    - 指数/ETF当日涨跌（收盘对前收盘）与K线阴阳（收盘对开盘）必须分别匹配A股程序真值
    - 跳空行情中允许“当日上涨但收阴线”或“当日下跌但收阳线”；不得把涨跌方向与K线阴阳混为一谈
    - 记者素材与程序真值冲突时，必须忽略记者素材，严禁融合或折中
    - 商品期货全景：必须包含 10 个商品卡片（5行2列）
    - 商品卡片第一行必须展示“形态：xxx”
    - 各商品形态与趋势标签，必须逐字匹配【程序查库得到的商品K线形态/趋势真值】
    - 商品卡片第二行必须展示“隐含波动率：xx.xx%（高/中/低）”
    - 各商品IV百分比与等级，必须逐字匹配【程序查库得到的商品IV真值】
    - 商品卡片不要再出现“支撑/压力”字段
    - 期权IV：用进度条可视化
    - 每日选的牛股或风险警示股，除了说明基本面原因，也要搭配说明K线技术面
    - 数据必须来自素材，不要编造
    - 底部写一句幽默毒舌点评
    """

    res = invoke_report_llm_with_fallback(
        llm,
        [HumanMessage(content=prompt)],
        env_prefix=REPORT_LLM_ENV_PREFIX,
        temperature=REPORT_LLM_TEMPERATURE,
    )
    raw_html = res.content.replace("```html", "").replace("```", "").strip()
    # The model contributes content only. The program re-renders the fixed shell so
    # later fact repairs cannot modify CSS, section order or outer card structure.
    initial_slots = _lock_programmatic_stock_sector(
        _extract_report_slots(raw_html),
        a_share_snapshot,
    )
    html = _render_locked_report_layout(initial_slots, today, weekday)

    # 发布前校验：固定排版 + 商品真值 + A股/ETF/板块资金真值。
    commodity_valid, commodity_anomalies = validate_commodity_cards(
        html,
        iv_snapshot_map,
        kline_snapshot_map,
    )
    a_share_anomalies = validate_a_share_report_facts(html, a_share_snapshot)
    layout_anomalies = validate_report_layout(html)
    anomalies = layout_anomalies + commodity_anomalies + a_share_anomalies
    is_valid = not layout_anomalies and commodity_valid and not a_share_anomalies
    cumulative_anomalies = list(dict.fromkeys(anomalies))
    stock_sector_failures = _programmatic_stock_sector_failures(anomalies)
    if stock_sector_failures:
        print("❌ [程序资金校验] 股票板块由程序生成但仍未通过，禁止交给AI重写。")
        for item in stock_sector_failures[:6]:
            print(f"   - {item}")
    for i in range(1, MAX_REWRITE_ROUNDS + 1):
        if is_valid or stock_sector_failures:
            break
        before_anomalies = set(anomalies)
        target_slots = [
            slot_name
            for slot_name in _classify_repair_slots(anomalies, html)
            if slot_name != "stock-sector"
        ]
        if not target_slots:
            print("❌ [局部修复] 没有可交给AI修复的内容插槽，终止重写。")
            break
        print(f"⚠️ [发布前校验] 报告异常，触发局部修复（第{i}/{MAX_REWRITE_ROUNDS}轮）")
        print(f"   - 目标插槽: {', '.join(target_slots)}")
        for a in anomalies[:8]:
            print(f"   - {a}")
        html = _rewrite_report_slots_after_validation(
            raw_material,
            html,
            anomalies,
            cumulative_anomalies,
            target_slots,
            i,
            today,
            weekday,
            iv_snapshot_text,
            kline_snapshot_text,
            a_share_snapshot_text,
            a_share_snapshot,
        )
        commodity_valid, commodity_anomalies = validate_commodity_cards(
            html,
            iv_snapshot_map,
            kline_snapshot_map,
        )
        a_share_anomalies = validate_a_share_report_facts(html, a_share_snapshot)
        layout_anomalies = validate_report_layout(html)
        anomalies = layout_anomalies + commodity_anomalies + a_share_anomalies
        is_valid = not layout_anomalies and commodity_valid and not a_share_anomalies
        stock_sector_failures = _programmatic_stock_sector_failures(anomalies)
        if stock_sector_failures:
            print("❌ [程序资金校验] 股票板块校验失败，终止后续AI重写。")
        after_anomalies = set(anomalies)
        resolved = sorted(before_anomalies - after_anomalies)
        new = sorted(after_anomalies - before_anomalies)
        repeated = sorted(before_anomalies & after_anomalies)
        print(
            f"📋 [局部修复结果] 已解决={len(resolved)}，"
            f"新增={len(new)}，重复={len(repeated)}"
        )
        for label, items in (("已解决", resolved), ("新增", new), ("重复", repeated)):
            for item in items[:6]:
                print(f"   - [{label}] {item}")
        cumulative_anomalies = list(dict.fromkeys(cumulative_anomalies + anomalies))

    if not is_valid:
        print("❌ [发布前校验] 报告事实校验仍未通过，终止发布。")
        for a in anomalies[:12]:
            print(f"   - {a}")
        try:
            failed_path = _write_failed_daily_report(html, a_share_snapshot)
            print(f"🗂️ [失败留档] 最终失败稿已保存: {failed_path}")
        except Exception as exc:
            print(f"⚠️ [失败留档] 保存失败: {exc}")
        return ""

    return _inject_report_data_provenance(html, a_share_snapshot)


# ==========================================
# 🔥 新增：发布到订阅中心
# ==========================================
def extract_summary(html_content: str) -> str:
    """从HTML中提取摘要（市场头条部分）"""
    import re
    # 尝试提取市场头条内容
    match = re.search(r'市场头条.*?<p[^>]*>(.*?)</p>', html_content, re.DOTALL)
    if match:
        # 去除HTML标签
        summary = re.sub(r'<[^>]+>', '', match.group(1))
        return summary[:200].strip()
    return ""


def publish_to_subscription_center(html_content: str):
    """
    🔥 发布晚报到订阅中心数据库
    - 插入 content_items 表
    - 自动为订阅用户创建站内消息
    """
    print("📤 [发布] 正在发布到订阅中心...")

    today_str = datetime.now().strftime("%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    title = f"{today_str} {weekday} 复盘晚报"
    summary = extract_summary(html_content)

    try:
        success, result = sub_svc.publish_content(
            channel_code="daily_report",
            title=title,
            content=html_content,
            summary=summary if summary else f"{today_str}市场复盘分析"
        )

        if success:
            print(f"✅ [发布] 成功发布到数据库，内容ID: {result}")
            return True, result
        else:
            print(f"❌ [发布] 发布失败: {result}")
            return False, result
    except Exception as e:
        print(f"❌ [发布] 发布异常: {e}")
        return False, str(e)


# ==========================================
# 🔥 更新：邮件群发（使用新订阅表）
# ==========================================
def blast_emails(html_content):
    """
    发送邮件给订阅用户
    🔥 更新：使用 user_subscriptions 表查询订阅用户
    """
    print("📧 准备群发邮件...")
    try:
        with engine.connect() as conn:
            # 🔥 使用新的订阅表查询
            sql = text("""
                       SELECT u.username, u.email
                       FROM users u
                                JOIN user_subscriptions us ON u.username = us.user_id
                                JOIN content_channels c ON us.channel_id = c.id
                       WHERE c.code = 'daily_report'
                         AND us.is_active = 1
                         AND us.notify_email = 1
                         AND (us.expire_at IS NULL OR us.expire_at > NOW())
                         AND u.email IS NOT NULL
                         AND u.email != ''
                       """)
            df = pd.read_sql(sql, conn)

        if df.empty:
            print("📭 无有效订阅用户（仅发送给未过期且开启邮件通知的订阅用户）。")
            return 0

        today_str = datetime.now().strftime("%m月%d日")
        subject = f"【爱波塔】{today_str} | 复盘晚报"

        success_cnt = 0
        for _, row in df.iterrows():
            try:
                # 预先检查邮箱格式
                email_addr = row['email']
                if not email_addr or "@" not in str(email_addr):
                    print(f" -> 跳过无效邮箱: {row['username']}")
                    continue

                if send_email(email_addr, subject, html_content):
                    success_cnt += 1
                    print(f" -> 发送成功: {row['username']}")
                else:
                    print(f" -> 发送失败: {row['username']}")

                time.sleep(1.5)  # 加上延时

            except Exception as inner_e:
                # 确保单个人出错不会卡死整个循环
                print(f" -> 处理用户 {row['username']} 时发生未知错误: {inner_e}")
                continue

        print(f"📧 邮件群发完成，成功 {success_cnt}/{len(df)} 人")
        return success_cnt

    except Exception as e:
        print(f"❌ 群发错误: {e}")
        return 0


if __name__ == "__main__":
    start_t = time.time()

    # 0. 在付出LLM调用成本前先做数据日门禁。任一核心表未到报告日即失败退出，
    # 不再静默使用昨日数据或让模型填补空白。
    report_trade_date = _current_trade_date_key()
    try:
        a_share_snapshot, a_share_snapshot_text = _fetch_programmatic_a_share_snapshot(report_trade_date)
    except ReportDataNotReadyError as exc:
        print(f"❌ [数据日门禁] {exc}")
        raise SystemExit(2)

    # 1. AI 记者出动
    material = collect_data_via_agent()
    try:
        audit_path = _write_daily_report_audit(a_share_snapshot, material)
        print(f"🧾 [审计留档] 记者素材与程序真值已保存: {audit_path}")
    except Exception as exc:
        # 留档失败不改变市场真值，但必须在日志中可见。
        print(f"⚠️ [审计留档] 保存失败: {exc}")

    # 2. AI 主编撰稿
    if len(material) > 100:
        report_html = draft_report(material, a_share_snapshot, a_share_snapshot_text)

        # 3. 发布和发送
        if len(report_html) > 300:
            # 只有通过全部事实校验的HTML才落盘/发布。
            with open("preview_report.html", "w", encoding="utf-8") as f:
                f.write(report_html)
            print("📄 预览文件已保存: preview_report.html")

            # 🔥 新增：发布到订阅中心数据库
            pub_success, pub_result = publish_to_subscription_center(report_html)

            # 发送邮件
            email_count = blast_emails(report_html)

            # 汇总
            print(f"\n{'=' * 50}")
            print(f"📊 发布结果汇总")
            print(f"{'=' * 50}")
            print(f"数据库发布: {'✅ 成功' if pub_success else '❌ 失败'}")
            print(f"邮件发送: {email_count} 人")
        else:
            print("❌ 报告内容过少或事实校验失败，取消发布")
            raise SystemExit(2)
    else:
        print("❌ 采集素材失败")
        raise SystemExit(2)

    print(f"⏱️ 总耗时: {time.time() - start_t:.1f} 秒")
