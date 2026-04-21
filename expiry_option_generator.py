"""
末日期权晚报生成器 v1.0
=====================================
功能：
- 扫描7天内即将到期的ETF期权和商品期权
- 通过K线技术分析判断标的趋势
- 根据趋势强弱映射期权策略（买看涨/卖看跌/买看跌/卖看涨/双卖/蝴蝶/铁鹰）
- 推荐具体合约（行权价 + 到期月份）
- 发布到站内消息

策略映射逻辑:
  强烈多头（红三兵/大阳线/上升三法等）→ 买看涨期权 (Buy Call)
  均线多头/震荡偏多                    → 卖看跌期权 (Sell Put)  [第一虚值]
  强烈空头（三只乌鸦/大阴线/下降三法等）→ 买看跌期权 (Buy Put)
  均线空头/震荡偏空                    → 卖看涨期权 (Sell Call) [第一虚值]
  震荡无方向                           → 双卖平值 / 蝴蝶 / 铁鹰
"""

import pandas as pd
import os
import time
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

# ==========================================
# 引入工具包
# ==========================================
from kline_tools import analyze_kline_pattern
import subscription_service as sub_svc

# 初始化环境
load_dotenv(override=True)

db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

llm = ChatTongyi(model="qwen-plus", temperature=0.1, api_key=os.getenv("DASHSCOPE_API_KEY"))

HIGH_IV_RANK_THRESHOLD = 70.0
# Match both styles:
# 1) legacy hyphen style: M2506-C-3200.ZCE
# 2) compact exchange style: MA605C3200.ZCE
CONTRACT_CODE_RE = re.compile(
    r"(?:[A-Z]{1,3}\d{4}-[CP]-\d+|[A-Z]{1,3}\d{3,4}[CP]\d+)\.[A-Z]+",
    flags=re.IGNORECASE,
)
MULTI_LEG_STRATEGY_LEGS = {
    "双卖": 2,
    "Short Straddle": 2,
    "牛市价差": 2,
    "Bull Call Spread": 2,
    "熊市价差": 2,
    "Bear Put Spread": 2,
    "蝴蝶": 3,
    "Butterfly": 3,
    "铁鹰": 4,
    "Iron Condor": 4,
}

# ==========================================
# 常量配置
# ==========================================

# ETF标的中文名映射
ETF_NAME_MAP = {
    "510050": "上证50ETF",
    "510300": "沪深300ETF(华泰)",
    "159919": "沪深300ETF(嘉实)",
    "510500": "中证500ETF",
    "159915": "创业板ETF",
    "588000": "科创50ETF",
    "159922": "中证1000ETF",
}

# 商品期权标的中文名映射（品种代码 → 中文名）
COMMODITY_NAME_MAP = {
    "M": "豆粕",
    "RM": "菜粕",
    "CF": "棉花",
    "SR": "白糖",
    "MA": "甲醇",
    "CU": "铜",
    "AU": "黄金",
    "AG": "白银",
    "RB": "螺纹钢",
    "I": "铁矿石",
    "P": "棕榈油",
    "Y": "豆油",
    "C": "玉米",
    "V": "PVC",
    "AL": "铝",
    "ZN": "锌",
    "NI": "镍",
    "PG": "LPG",
    "TA": "PTA",
    "SA": "纯碱",
    "SH": "烧碱",
    "SC": "原油",
    "BU": "沥青",
    "EB": "苯乙烯",
    "PP": "聚丙烯",
    "L": "塑料",
    "A": "大豆",
    "B": "豆二",
    "JM": "焦煤",
    "J": "焦炭",
    "HC": "热轧卷板",
    "SF": "硅铁",
    "SM": "锰硅",
    "SP": "纸浆",
    "FU": "燃油",
    "OI": "菜籽油",
    "RS": "菜籽",
    "WH": "强麦",
    "PM": "普麦",
    "RI": "粳稻",
    "LH": "生猪",
    "PK": "花生",
    "CJ": "红枣",
    "AP": "苹果",
    "SI": "工业硅",
    "PS": "多晶硅",
    "LC": "碳酸锂",
    "FG": "玻璃",
    "UR": "尿素",
    "ZC": "动力煤",
    "PF": "短纤",
    "PL": "丙烯",
    "PR": "瓶片",
    "OP": "双胶纸",
    "SN": "锡",
    "PB": "铅",
    "RU": "橡胶",
    "BR": "BR橡胶",
    "JD": "鸡蛋",
    "AO": "氧化铝",
    "AD": "铝合金",
    "BZ": "纯苯",
    "LG": "原木",
    # 股指期权（中金所，存于 commodity_option_basic）
    "IO": "沪深300股指",
    "HO": "上证50股指",
    "MO": "中证1000股指",
}


def normalize_etf_code(underlying: str) -> str:
    """Normalize ETF underlying code to 6-digit form (e.g. 159915.SZ -> 159915)."""
    raw = str(underlying or "").strip().upper()
    if not raw:
        return ""
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw


def resolve_etf_name(underlying: str) -> str:
    """Resolve ETF display name from either 6-digit code or full ts_code."""
    raw = str(underlying or "").strip().upper()
    base = normalize_etf_code(raw)
    return ETF_NAME_MAP.get(raw) or ETF_NAME_MAP.get(base) or str(underlying)


def resolve_commodity_name(underlying: str) -> str:
    code = str(underlying or "").strip().upper()
    return COMMODITY_NAME_MAP.get(code, str(underlying))


def _format_strike_text(value) -> str:
    """Format strike to concise display text like 3.2 / 2.95."""
    try:
        num = float(value)
    except Exception:
        return str(value)
    return f"{num:.4f}".rstrip("0").rstrip(".")


def _build_contract_display_name(row: pd.Series, option_type: str) -> str:
    """Build user-facing contract label; ETF prefers strike+认购/认沽."""
    ts_code = str(row.get("ts_code") or "").strip()
    strike_txt = _format_strike_text(row.get("exercise_price"))
    cp = str(row.get("call_put") or "").upper()
    cp_label = "认购" if cp == "C" else "认沽" if cp == "P" else ""

    if option_type == "ETF期权":
        if cp_label and ts_code:
            return f"{strike_txt}{cp_label}（{ts_code}）"
        if cp_label:
            return f"{strike_txt}{cp_label}"
    if ts_code and cp_label:
        return f"{ts_code}（{cp_label}）"
    if ts_code:
        return ts_code
    return f"{strike_txt}{cp_label}" if cp_label else strike_txt


# 股指期权 → 对应期货代码（用于查现价和K线分析）
INDEX_OPT_FUTURES_MAP = {
    "IO": "IF",   # 沪深300股指期权 → 沪深300期货
    "HO": "IH",   # 上证50股指期权  → 上证50期货
    "MO": "IM",   # 中证1000股指期权 → 中证1000期货
}

# ==========================================
# 工具函数 1：获取即将到期的期权列表
# ==========================================

@tool
def tool_get_expiring_options(days_ahead: int = 7) -> str:
    """
    【末日期权扫描器】
    扫描所有在 days_ahead 天内即将到期的期权合约。
    返回包含标的、到期日、剩余天数、期权类型的汇总信息。
    """
    today = datetime.now().date()
    cutoff = today + timedelta(days=days_ahead)
    today_str = today.strftime("%Y%m%d")
    cutoff_str = cutoff.strftime("%Y%m%d")

    results = []

    # ---- ETF 期权 ----
    try:
        sql_etf = f"""
            SELECT 
                b.underlying,
                b.call_put,
                b.exercise_price,
                b.delist_date AS maturity_date,
                b.ts_code,
                d.close,
                d.vol,
                d.oi
            FROM option_basic b
            LEFT JOIN option_daily d 
                ON b.ts_code = d.ts_code 
                AND d.trade_date = (SELECT MAX(trade_date) FROM option_daily WHERE ts_code = b.ts_code)
            WHERE b.delist_date > '{today_str}'
              AND b.delist_date <= '{cutoff_str}'
            ORDER BY b.underlying, b.delist_date, b.call_put, b.exercise_price
        """
        df_etf = pd.read_sql(sql_etf, engine)
        df_etf["option_type"] = "ETF期权"
        results.append(df_etf)
        print(f"✅ ETF期权扫描完成：找到 {len(df_etf)} 条合约")
    except Exception as e:
        print(f"❌ ETF期权查询出错: {e}")

    # ---- 商品期权 ----
    try:
        sql_comm = f"""
            SELECT 
                b.ts_code,
                b.call_put,
                b.exercise_price,
                b.maturity_date,
                d.close,
                d.vol,
                d.oi
            FROM commodity_option_basic b
            LEFT JOIN commodity_opt_daily d
                ON b.ts_code = d.ts_code
                AND d.trade_date = (SELECT MAX(trade_date) FROM commodity_opt_daily WHERE ts_code = b.ts_code)
            WHERE b.maturity_date > '{today_str}'
              AND b.maturity_date <= '{cutoff_str}'
            ORDER BY b.ts_code, b.call_put, b.exercise_price
        """
        df_comm = pd.read_sql(sql_comm, engine)
        df_comm["option_type"] = "商品期权"

        # 从 ts_code 提取品种代码（如 M2506C3200 → M）
        def extract_product_code(ts_code):
            m = re.match(r'^([A-Za-z]+)', str(ts_code))
            return m.group(1).upper() if m else ""

        df_comm["underlying"] = df_comm["ts_code"].apply(extract_product_code)
        results.append(df_comm)
        print(f"✅ 商品期权扫描完成：找到 {len(df_comm)} 条合约")
    except Exception as e:
        print(f"❌ 商品期权查询出错: {e}")

    if not results:
        return "未找到任何即将到期的期权合约。"

    df_all = pd.concat(results, ignore_index=True)
    df_all["maturity_date"] = pd.to_datetime(df_all["maturity_date"].astype(str), format="%Y%m%d", errors="coerce")
    df_all["days_left"] = (df_all["maturity_date"] - pd.Timestamp(today)).dt.days
    df_all = df_all[df_all["days_left"] > 0].copy()

    if df_all.empty:
        return f"未找到 {days_ahead} 天内到期的期权合约。"

    # 按标的汇总
    summary_lines = [f"📅 末日期权扫描结果（{today_str} ~ {cutoff_str}，共 {len(df_all)} 条合约）\n"]
    grouped = df_all.groupby(["underlying", "option_type", "maturity_date"])

    for (underlying, opt_type, mat_date), grp in grouped:
        days_left = grp["days_left"].iloc[0]
        calls = grp[grp["call_put"] == "C"]
        puts = grp[grp["call_put"] == "P"]

        # 获取标的中文名
        if opt_type == "ETF期权":
            name = resolve_etf_name(str(underlying))
        else:
            name = resolve_commodity_name(str(underlying))

        # 找ATM附近（持仓量最大的合约作为参考平值）
        atm_info = ""
        if not grp.empty and "oi" in grp.columns:
            atm_row = grp.dropna(subset=["oi"]).sort_values("oi", ascending=False)
            if not atm_row.empty:
                atm_strike = atm_row.iloc[0]["exercise_price"]
                atm_info = f"参考平值行权价≈{atm_strike}"

        summary_lines.append(
            f"  【{name}({underlying})】 {opt_type} | 到期:{mat_date.strftime('%Y-%m-%d')} | 剩余:{days_left}天 | "
            f"Call合约:{len(calls)}个 Put合约:{len(puts)}个 | {atm_info}"
        )

    return "\n".join(summary_lines)


# ==========================================
# 工具函数 2：获取标的当前价格（用于判断ATM/OTM行权价）
# ==========================================

@tool
def tool_get_underlying_price(underlying_code: str, option_type: str = "ETF期权") -> str:
    """
    【标的现价查询】
    查询ETF或商品期货标的的当前价格，用于确定期权行权价位置。
    参数:
    - underlying_code: 标的代码，如 '510300'（ETF）或 'M'（商品品种代码）
    - option_type: 'ETF期权' 或 '商品期权'
    """
    try:
        if option_type == "ETF期权":
            # 用精确匹配替代LIKE，彻底避免%被pymysql误当格式符的问题
            df = pd.DataFrame()
            for suffix in [".SH", ".SZ", ".OF", ""]:
                code = f"{underlying_code}{suffix}" if suffix else underlying_code
                sql = f"""
                    SELECT close_price, trade_date FROM stock_price 
                    WHERE ts_code = '{code}'
                    ORDER BY trade_date DESC LIMIT 1
                """
                df = pd.read_sql(sql, engine)
                if not df.empty:
                    break
            if df.empty:
                return f"未找到 {underlying_code} 的ETF价格数据"
            price = float(df.iloc[0]["close_price"])
            date = df.iloc[0]["trade_date"]
            name = resolve_etf_name(str(underlying_code))
            return f"{name}({underlying_code}) 最新价格: {price:.4f} 元  (数据日期: {date})"

        else:
            # 用REGEXP查商品主力合约，TAS过滤在Python层完成，SQL中不出现%
            sql = f"""
                SELECT ts_code, close_price, trade_date, oi
                FROM futures_price
                WHERE UPPER(ts_code) REGEXP '^{underlying_code.upper()}[0-9]'
                ORDER BY trade_date DESC, oi DESC
                LIMIT 10
            """
            df = pd.read_sql(sql, engine)
            if not df.empty:
                df = df[~df["ts_code"].str.upper().str.contains("TAS")]
            if df.empty:
                return f"未找到 {underlying_code} 的期货价格数据"
            price = float(df.iloc[0]["close_price"])
            ts_code = df.iloc[0]["ts_code"]
            date = df.iloc[0]["trade_date"]
            name = resolve_commodity_name(underlying_code.upper())
            return f"{name}({ts_code}) 主力合约最新价: {price:.2f}  (数据日期: {date})"

    except Exception as e:
        return f"查询 {underlying_code} 价格出错: {e}"


# ==========================================
# 工具函数 3：查询推荐行权价的具体合约信息
# ==========================================

@tool
def tool_get_recommended_strikes(underlying_code: str, option_type: str, maturity_date_str: str,
                                 current_price: float, strategy: str, iv_rank: float | None = None) -> str:
    """
    【推荐行权价查询】
    根据当前价格和策略，返回推荐的具体期权合约（行权价、权利金、持仓量）。
    参数:
    - underlying_code: 标的代码（如 '510300' 或 'M'）
    - option_type: 'ETF期权' 或 '商品期权'
    - maturity_date_str: 到期日 YYYYMMDD
    - current_price: 标的当前价格
    - strategy: 策略类型，如 '买看涨','卖看跌','买看跌','卖看涨','双卖','蝴蝶','铁鹰','牛市价差','熊市价差'
    """
    try:
        if option_type == "ETF期权":
            sql = f"""
                SELECT b.ts_code, b.call_put, b.exercise_price, b.delist_date,
                       d.close as premium, d.oi, d.vol
                FROM option_basic b
                LEFT JOIN option_daily d ON b.ts_code = d.ts_code
                    AND d.trade_date = (SELECT MAX(trade_date) FROM option_daily WHERE ts_code = b.ts_code)
                WHERE b.underlying = '{underlying_code}'
                  AND b.delist_date = '{maturity_date_str}'
                ORDER BY b.call_put, b.exercise_price
            """
        else:
            # 商品期权
            regex_pattern = f'^({underlying_code}|{underlying_code.lower()})[0-9]'
            sql = f"""
                SELECT b.ts_code, b.call_put, b.exercise_price, b.maturity_date as delist_date,
                       d.close as premium, d.oi, d.vol
                FROM commodity_option_basic b
                LEFT JOIN commodity_opt_daily d ON b.ts_code = d.ts_code
                    AND d.trade_date = (SELECT MAX(trade_date) FROM commodity_opt_daily WHERE ts_code = b.ts_code)
                WHERE b.ts_code REGEXP '{regex_pattern}'
                  AND b.maturity_date = '{maturity_date_str}'
                ORDER BY b.call_put, b.exercise_price
            """

        df = pd.read_sql(sql, engine)
        if df.empty:
            return f"未找到 {underlying_code} 在 {maturity_date_str} 的期权合约"

        df["exercise_price"] = pd.to_numeric(df["exercise_price"], errors="coerce")
        df = df.dropna(subset=["exercise_price"]).sort_values("exercise_price")
        calls = df[df["call_put"] == "C"].reset_index(drop=True)
        puts = df[df["call_put"] == "P"].reset_index(drop=True)

        def find_atm_idx(df_side, price):
            """找最接近当前价格的行权价index"""
            if df_side.empty:
                return 0
            diffs = (df_side["exercise_price"] - price).abs()
            return diffs.idxmin()

        atm_c_idx = find_atm_idx(calls, current_price)
        atm_p_idx = find_atm_idx(puts, current_price)
        is_high_iv = iv_rank is not None and iv_rank >= HIGH_IV_RANK_THRESHOLD

        def fmt_contract(row):
            premium = f"{row['premium']:.4f}" if pd.notna(row.get("premium")) else "N/A"
            oi = int(row["oi"]) if pd.notna(row.get("oi")) else 0
            strike_txt = _format_strike_text(row.get("exercise_price"))
            contract_name = _build_contract_display_name(row, option_type)
            return (f"  合约:{contract_name} | 行权价:{strike_txt} | "
                    f"权利金:{premium} | 持仓量:{oi}手")

        def pick_otm_call(depth: int = 1):
            otm_calls = calls[calls["exercise_price"] > current_price].reset_index(drop=True)
            if otm_calls.empty:
                return None
            idx = min(max(depth - 1, 0), len(otm_calls) - 1)
            return otm_calls.iloc[idx]

        def pick_otm_put(depth: int = 1):
            otm_puts = puts[puts["exercise_price"] < current_price].reset_index(drop=True)
            if otm_puts.empty:
                return None
            idx = max(len(otm_puts) - depth, 0)
            return otm_puts.iloc[idx]

        def pick_bull_call_spread_legs():
            """Return (buy_call, sell_call) with buy strike < sell strike whenever possible."""
            if len(calls) < 2:
                return None, None

            buy_call = calls.iloc[atm_c_idx]
            higher_calls = calls[calls["exercise_price"] > buy_call["exercise_price"]].reset_index(drop=True)
            if not higher_calls.empty:
                return buy_call, higher_calls.iloc[0]

            # If ATM is already the highest strike, step one level down for buy leg.
            if atm_c_idx > 0:
                return calls.iloc[atm_c_idx - 1], calls.iloc[atm_c_idx]

            return calls.iloc[-2], calls.iloc[-1]

        def pick_bear_put_spread_legs():
            """Return (buy_put, sell_put) with buy strike > sell strike whenever possible."""
            if len(puts) < 2:
                return None, None

            buy_put = puts.iloc[atm_p_idx]
            lower_puts = puts[puts["exercise_price"] < buy_put["exercise_price"]].reset_index(drop=True)
            if not lower_puts.empty:
                return buy_put, lower_puts.iloc[-1]

            # If ATM is already the lowest strike, step one level up for buy leg.
            if atm_p_idx < len(puts) - 1:
                return puts.iloc[atm_p_idx + 1], puts.iloc[atm_p_idx]

            return puts.iloc[1], puts.iloc[0]

        lines = [f"\n📌 【{underlying_code}】{strategy} 推荐合约（到期:{maturity_date_str}，标的现价≈{current_price}）"]
        if iv_rank is not None:
            lines.append(f"📊 IV Rank≈{iv_rank:.1f}% | {'高IV环境' if is_high_iv else '低/中IV环境'}")

        if strategy in ("买看涨", "买call", "Buy Call"):
            # ATM Call（平值）
            if not calls.empty:
                row = calls.iloc[atm_c_idx]
                lines.append("🟢 推荐买入 ATM Call（平值认购）：")
                lines.append(fmt_contract(row))

        elif strategy in ("牛市价差", "Bull Call Spread"):
            if len(calls) >= 2:
                buy_call, sell_call = pick_bull_call_spread_legs()
                lines.append("🟥 推荐牛市价差（买入Call + 卖出Call）：")
                if buy_call is not None and sell_call is not None and sell_call["ts_code"] != buy_call["ts_code"]:
                    lines.append(f"  买入Call: {fmt_contract(buy_call)}")
                    lines.append(f"  卖出Call: {fmt_contract(sell_call)}")
                else:
                    lines.append("  ⚠️ Call档位不足，暂未构建完整牛市价差")
            elif not calls.empty:
                lines.append("  ⚠️ Call合约数量不足（<2），无法构建完整牛市价差")

        elif strategy in ("卖看跌", "卖put", "Sell Put"):
            if not puts.empty:
                if is_high_iv:
                    row = pick_otm_put(2)
                    lines.append("🔵 高IV环境：推荐卖出更虚值 Put（第二虚值认沽）：")
                    lines.append(fmt_contract(row))
                else:
                    row = puts.iloc[atm_p_idx]
                    lines.append("🔵 低/中IV环境：推荐卖出 ATM Put（平值认沽）：")
                    lines.append(fmt_contract(row))

        elif strategy in ("买看跌", "买put", "Buy Put"):
            if not puts.empty:
                row = puts.iloc[atm_p_idx]
                lines.append("🔴 推荐买入 ATM Put（平值认沽）：")
                lines.append(fmt_contract(row))

        elif strategy in ("熊市价差", "Bear Put Spread"):
            if len(puts) >= 2:
                buy_put, sell_put = pick_bear_put_spread_legs()
                lines.append("🟩 推荐熊市价差（买入Put + 卖出Put）：")
                if buy_put is not None and sell_put is not None and sell_put["ts_code"] != buy_put["ts_code"]:
                    lines.append(f"  买入Put: {fmt_contract(buy_put)}")
                    lines.append(f"  卖出Put: {fmt_contract(sell_put)}")
                else:
                    lines.append("  ⚠️ Put档位不足，暂未构建完整熊市价差")
            elif not puts.empty:
                lines.append("  ⚠️ Put合约数量不足（<2），无法构建完整熊市价差")

        elif strategy in ("卖看涨", "卖call", "Sell Call"):
            if not calls.empty:
                if is_high_iv:
                    row = pick_otm_call(2)
                    lines.append("🟠 高IV环境：推荐卖出更虚值 Call（第二虚值认购）：")
                    lines.append(fmt_contract(row))
                else:
                    row = calls.iloc[atm_c_idx]
                    lines.append("🟠 低/中IV环境：推荐卖出 ATM Call（平值认购）：")
                    lines.append(fmt_contract(row))

        elif strategy in ("双卖", "双卖平值", "Straddle Short"):
            if is_high_iv:
                lines.append("⚖️ 高IV环境：推荐双卖宽跨式组合（卖出虚值Call + 卖出虚值Put）：")
                sell_call = pick_otm_call(1)
                sell_put = pick_otm_put(1)
                if sell_call is not None:
                    lines.append(f"  卖出 Call: {fmt_contract(sell_call)}")
                if sell_put is not None:
                    lines.append(f"  卖出 Put:  {fmt_contract(sell_put)}")
            else:
                lines.append("⚖️ 低/中IV环境：推荐双卖平值组合：")
                if not calls.empty:
                    lines.append(f"  卖出 Call: {fmt_contract(calls.iloc[atm_c_idx])}")
                if not puts.empty:
                    lines.append(f"  卖出 Put:  {fmt_contract(puts.iloc[atm_p_idx])}")

        elif strategy in ("蝴蝶", "Butterfly"):
            # 卖2手ATM + 买1手上翼Call + 买1手下翼Put
            lines.append("🦋 推荐蝴蝶策略（Call蝴蝶）：")
            if not calls.empty and atm_c_idx > 0 and atm_c_idx < len(calls) - 1:
                wing_low = calls.iloc[atm_c_idx - 1]
                body = calls.iloc[atm_c_idx]
                wing_high = calls.iloc[atm_c_idx + 1]
                lines.append(f"  买1手低翼Call: {fmt_contract(wing_low)}")
                lines.append(f"  卖2手中翼Call: {fmt_contract(body)}")
                lines.append(f"  买1手高翼Call: {fmt_contract(wing_high)}")
            else:
                lines.append("  ⚠️ 合约数量不足，无法构建完整蝴蝶")

        elif strategy in ("铁鹰", "Iron Condor"):
            # 卖OTM Call + 买更OTM Call + 卖OTM Put + 买更OTM Put
            lines.append("🦅 推荐铁鹰策略（Iron Condor）：")
            otm_calls = calls[calls["exercise_price"] > current_price].reset_index(drop=True)
            otm_puts = puts[puts["exercise_price"] < current_price].reset_index(drop=True)

            if len(otm_calls) >= 2 and len(otm_puts) >= 2:
                sell_call = otm_calls.iloc[0]  # 第一虚值Call（卖）
                buy_call = otm_calls.iloc[1]  # 第二虚值Call（买）
                sell_put = otm_puts.iloc[-1]  # 第一虚值Put（卖，最接近现价）
                buy_put = otm_puts.iloc[-2]  # 第二虚值Put（买）
                lines.append(f"  卖出Call（近端）: {fmt_contract(sell_call)}")
                lines.append(f"  买入Call（远端）: {fmt_contract(buy_call)}")
                lines.append(f"  卖出Put（近端）: {fmt_contract(sell_put)}")
                lines.append(f"  买入Put（远端）: {fmt_contract(buy_put)}")
            else:
                lines.append("  ⚠️ 虚值合约不足，无法构建完整铁鹰")

        return "\n".join(lines)

    except Exception as e:
        return f"查询推荐行权价出错: {e}"


# ==========================================
# 核心逻辑 1：K线信号 → 策略映射
# ==========================================

def parse_strategy_from_kline(kline_text: str) -> tuple[str, str]:
    """
    解析 analyze_kline_pattern 返回的自然语言文本，映射到期权策略。
    返回: (strategy_name, reason)
    """
    # 强多头信号关键词
    strong_bull_keywords = [
        "红三兵", "大阳线", "上升三法", "近5日强势上涨", "多头吞噬",
        "极致压缩突破", "短线旗形突破", "标准箱体突破", "长线平台突破",
        "放量突破", "V型反转", "多头主导", "多头持续上攻"
    ]
    # 弱多头/偏多信号
    mild_bull_keywords = [
        "均线多头排列", "震荡偏多", "小幅上涨", "晨星", "锤子",
        "站上5日线", "站稳20日线", "多头反击"
    ]
    # 强空头信号关键词
    strong_bear_keywords = [
        "三只乌鸦", "大阴线", "下降三法", "近5日持续下跌", "空头吞噬",
        "极致压缩破位", "短线旗形破位", "标准箱体破位", "长线平台破位",
        "放量下跌", "空头主导", "空头持续发力"
    ]
    # 弱空头/偏空信号
    mild_bear_keywords = [
        "均线空头排列", "震荡偏空", "小幅下跌", "夜星", "倒状锤子",
        "跌破5日线", "跌破20日线", "空头反击"
    ]
    # 震荡信号
    sideways_keywords = [
        "横盘震荡", "十字星", "波动收窄", "多空对峙", "方向待选",
        "震荡横盘"
    ]

    def count_hits(keywords):
        return sum(1 for kw in keywords if kw in kline_text)

    strong_bull = count_hits(strong_bull_keywords)
    mild_bull = count_hits(mild_bull_keywords)
    strong_bear = count_hits(strong_bear_keywords)
    mild_bear = count_hits(mild_bear_keywords)
    sideways = count_hits(sideways_keywords)

    # 计算多空净分值
    bull_score = strong_bull * 2 + mild_bull
    bear_score = strong_bear * 2 + mild_bear

    if bull_score == 0 and bear_score == 0 and sideways > 0:
        return "双卖", "技术面震荡无方向，建议双卖平值或构建铁鹰/蝴蝶策略"

    if bull_score >= 2 and strong_bull >= 1 and bull_score > bear_score:
        return "买看涨", f"强多头信号（得分:{bull_score}），建议买入末日看涨期权博取上涨收益"

    if bull_score > 0 and bull_score > bear_score and strong_bull == 0:
        return "卖看跌", f"均线偏多/震荡偏多（得分:{bull_score}），建议卖出虚值看跌期权赚取权利金"

    if bear_score >= 2 and strong_bear >= 1 and bear_score > bull_score:
        return "买看跌", f"强空头信号（得分:{bear_score}），建议买入末日看跌期权博取下跌收益"

    if bear_score > 0 and bear_score > bull_score and strong_bear == 0:
        return "卖看涨", f"均线偏空/震荡偏空（得分:{bear_score}），建议卖出虚值看涨期权赚取权利金"

    # 多空信号相当 → 震荡策略
    return "铁鹰", f"多空信号相当（多:{bull_score} 空:{bear_score}），方向不明，建议铁鹰策略控制风险"


# IV-aware strategy overlay for expiry options.
def parse_strategy_with_iv(kline_text: str, iv_context: dict | None = None) -> tuple[str, str]:
    base_strategy, base_reason = parse_strategy_from_kline(kline_text)
    iv_rank = None if not iv_context else iv_context.get("iv_rank")
    iv_level = "未知" if not iv_context else iv_context.get("iv_level", "未知")
    is_high_iv = iv_rank is not None and iv_rank >= HIGH_IV_RANK_THRESHOLD

    if base_strategy in ("买看涨", "买call", "Buy Call") and is_high_iv:
        return "牛市价差", f"{base_reason}；但IV处于{iv_level}（IV Rank {iv_rank:.1f}%），末日直接买权利金偏贵，改用买平值卖虚值的牛市价差"

    if base_strategy in ("买看跌", "买put", "Buy Put") and is_high_iv:
        return "熊市价差", f"{base_reason}；但IV处于{iv_level}（IV Rank {iv_rank:.1f}%），末日直接买权利金偏贵，改用买平值卖虚值的熊市价差"

    if base_strategy in ("卖看跌", "卖put", "Sell Put"):
        if is_high_iv:
            return "卖看跌", f"{base_reason}；当前IV处于{iv_level}（IV Rank {iv_rank:.1f}%），优先卖更虚值认沽提升安全垫"
        return "卖看跌", f"{base_reason}；当前IV处于{iv_level}，优先卖平值认沽提升时间价值效率"

    if base_strategy in ("卖看涨", "卖call", "Sell Call"):
        if is_high_iv:
            return "卖看涨", f"{base_reason}；当前IV处于{iv_level}（IV Rank {iv_rank:.1f}%），优先卖更虚值认购提高容错"
        return "卖看涨", f"{base_reason}；当前IV处于{iv_level}，优先卖平值认购提升时间价值效率"

    if base_strategy in ("双卖", "双卖平值", "Straddle Short"):
        if is_high_iv:
            return "铁鹰", f"{base_reason}；当前IV处于{iv_level}（IV Rank {iv_rank:.1f}%），改用铁鹰/宽跨式更适合高波动卖方环境"
        return "双卖", f"{base_reason}；当前IV处于{iv_level}，保留平值双卖更合适"

    if base_strategy in ("铁鹰", "Iron Condor") and not is_high_iv:
        return "双卖", f"{base_reason}；但IV不高，铁鹰收益压缩，改为平值双卖更直接"

    return base_strategy, f"{base_reason}；当前IV处于{iv_level}" if iv_context else base_reason


# ==========================================
# 核心逻辑 2：获取即将到期的标的列表（结构化）
# ==========================================

def get_expiring_underlying_list(days_ahead: int = 7) -> list[dict]:
    """
    直接从数据库查询7天内到期的期权，返回按标的归组的结构化列表。
    每个元素: {underlying, option_type, maturity_date, days_left, name}
    """
    today = datetime.now().date()
    cutoff = today + timedelta(days=days_ahead)
    today_str = today.strftime("%Y%m%d")
    cutoff_str = cutoff.strftime("%Y%m%d")

    items = []

    # ETF期权
    try:
        sql = f"""
            SELECT DISTINCT underlying, delist_date as maturity_date
            FROM option_basic
            WHERE delist_date > '{today_str}' AND delist_date <= '{cutoff_str}'
            ORDER BY delist_date, underlying
        """
        df = pd.read_sql(sql, engine)
        for _, row in df.iterrows():
            mat_str = str(row["maturity_date"]).replace("-", "").replace(" ", "").split(".")[0][:8]
            try:
                mat = datetime.strptime(mat_str, "%Y%m%d").date()
            except Exception:
                continue
            items.append({
                "underlying": str(row["underlying"]),
                "option_type": "ETF期权",
                "maturity_date": mat_str,
                "contract_month": "",  # ETF无期货合约月份概念
                "days_left": (mat - today).days,
                "name": resolve_etf_name(str(row["underlying"]))
            })
    except Exception as e:
        print(f"ETF期权扫描错误: {e}")

    # 商品期权
    try:
        sql = f"""
            SELECT DISTINCT
                REGEXP_REPLACE(ts_code, '[0-9].*', '') AS product_code,
                REGEXP_SUBSTR(ts_code, '[0-9]+') AS contract_month,
                maturity_date
            FROM commodity_option_basic
            WHERE maturity_date > '{today_str}' AND maturity_date <= '{cutoff_str}'
            ORDER BY maturity_date
        """
        df = pd.read_sql(sql, engine)
        for _, row in df.iterrows():
            product = str(row["product_code"]).upper().strip()
            contract_month = str(row["contract_month"]).strip() if row["contract_month"] else ""
            # ZCE期权用3位合约代码（如"604"），futures_price存4位（"2604"），统一补"2"
            if len(contract_month) == 3:
                contract_month = "2" + contract_month
            # 规范化日期：兼容 "20250424"、"2025-04-24"、"2025-04-24 00:00:00" 等格式
            mat_str = str(row["maturity_date"]).replace("-", "").replace(" ", "").split(".")[0][:8]
            try:
                mat = datetime.strptime(mat_str, "%Y%m%d").date()
            except Exception:
                print(f"  ⚠️ 无法解析到期日: {row['maturity_date']} → {mat_str}，跳过")
                continue
            items.append({
                "underlying": product,
                "option_type": "商品期权",
                "maturity_date": mat_str,
                "contract_month": contract_month,  # 从ts_code提取的期货合约月份（如2504）
                "days_left": (mat - today).days,
                "name": resolve_commodity_name(product)
            })
    except Exception as e:
        print(f"商品期权扫描错误: {e}")

    # 去重（同一标的同一到期日只保留一条）
    seen = set()
    unique_items = []
    for item in items:
        key = (item["underlying"], item["maturity_date"])
        if key not in seen:
            seen.add(key)
            unique_items.append(item)

    print(f"📋 共扫描到 {len(unique_items)} 个「标的-到期日」组合")
    return unique_items


# ==========================================
# 核心逻辑 3：获取标的现价（不通过tool，直接查库）
# ==========================================

def get_price_direct(underlying: str, option_type: str, contract_month: str = "") -> float | None:
    """
    直接从DB查标的现价，返回float或None。

    contract_month: 从期权ts_code提取的标的期货合约月份（如 "2504"）。
      - 商品/股指期权：优先查该月份合约，避免主力换月后价格错位。
        注意：商品期权比期货提前结算，期权到期日是3月但标的是4月期货，
        因此合约月份必须从期权ts_code取，而非从期权到期日推算。
      - ETF期权：contract_month为空，直接查最新价。

    【重要】SQL中不使用 LIKE '%xxx%'，改用REGEXP，TAS过滤在Python层完成。
    """
    try:
        if option_type == "ETF期权":
            sql = f"""
                SELECT close_price, trade_date FROM stock_price
                WHERE ts_code REGEXP '^{underlying}'
                ORDER BY trade_date DESC LIMIT 5
            """
            df = pd.read_sql(sql, engine)
            if not df.empty:
                return float(df.iloc[0]["close_price"])
        else:
            # 股指期权用对应期货查价（IO→IF, HO→IH, MO→IM）
            query_symbol = INDEX_OPT_FUTURES_MAP.get(underlying.upper(), underlying.upper())

            # ZCE期权ts_code用3位月份（如"604"），统一转4位（"2604"）
            if len(contract_month) == 3:
                contract_month = "2" + contract_month

            # 优先：查对应月份期货（直接用从ts_code提取的合约月份）
            if contract_month:
                sql = f"""
                    SELECT ts_code, close_price, trade_date, oi
                    FROM futures_price
                    WHERE UPPER(ts_code) REGEXP '^{query_symbol}{contract_month}'
                    ORDER BY trade_date DESC
                    LIMIT 5
                """
                df = pd.read_sql(sql, engine)
                if not df.empty:
                    df = df[~df["ts_code"].str.upper().str.contains("TAS")]
                    if not df.empty:
                        ts = df.iloc[0]["ts_code"]
                        price = float(df.iloc[0]["close_price"])
                        print(f"  ✅ 对应月份合约 {ts} 现价: {price}")
                        return price

            # 降级：主力合约（最高OI）
            sql = f"""
                SELECT ts_code, close_price, trade_date, oi
                FROM futures_price
                WHERE UPPER(ts_code) REGEXP '^{query_symbol}[0-9]'
                ORDER BY trade_date DESC, oi DESC
                LIMIT 10
            """
            df = pd.read_sql(sql, engine)
            if not df.empty:
                df = df[~df["ts_code"].str.upper().str.contains("TAS")]
                if not df.empty:
                    ts = df.iloc[0]["ts_code"]
                    price = float(df.iloc[0]["close_price"])
                    print(f"  ⚠️  未找到{contract_month}合约，降级使用主力 {ts} 现价: {price}")
                    return price
    except Exception as e:
        print(f"查询 {underlying} 现价失败: {e}")
    return None


def get_iv_context(underlying: str, option_type: str, contract_month: str = "", window: int = 252) -> dict:
    """Fetch current IV and IV rank for the underlying used by expiry strategy selection."""
    context = {
        "current_iv": None,
        "iv_rank": None,
        "iv_level": "未知",
        "iv_source": "",
    }

    try:
        if option_type == "ETF期权":
            sql = f"""
                SELECT REPLACE(trade_date, '-', '') AS trade_date, iv
                FROM etf_iv_history
                WHERE etf_code = '{underlying}'
                ORDER BY trade_date DESC
                LIMIT {window}
            """
            df_iv = pd.read_sql(sql, engine)
            iv_source = underlying
        else:
            code = underlying.upper()
            search_code = INDEX_OPT_FUTURES_MAP.get(code, code)
            candidates = []
            if contract_month:
                candidates.append(f"{search_code}{contract_month}")
                if search_code != code:
                    candidates.append(f"{code}{contract_month}")
            else:
                candidates.append(search_code)
                if search_code != code:
                    candidates.append(code)

            df_iv = pd.DataFrame()
            iv_source = ""
            for candidate in candidates:
                sql = f"""
                    SELECT REPLACE(trade_date, '-', '') AS trade_date, iv
                    FROM commodity_iv_history
                    WHERE ts_code = '{candidate}'
                    ORDER BY trade_date DESC
                    LIMIT {window}
                """
                df_iv = pd.read_sql(sql, engine)
                if not df_iv.empty:
                    iv_source = candidate
                    break

        if df_iv.empty:
            return context

        curr_iv = float(df_iv.iloc[0]["iv"])
        max_iv = float(df_iv["iv"].max())
        min_iv = float(df_iv["iv"].min())
        iv_rank = ((curr_iv - min_iv) / (max_iv - min_iv) * 100.0) if max_iv != min_iv else 0.0

        if iv_rank < 20:
            iv_level = "低"
        elif iv_rank < 50:
            iv_level = "中低"
        elif iv_rank < 70:
            iv_level = "中"
        elif iv_rank < 85:
            iv_level = "高"
        else:
            iv_level = "极高"

        context.update({
            "current_iv": round(curr_iv, 2),
            "iv_rank": round(iv_rank, 1),
            "iv_level": iv_level,
            "iv_source": iv_source,
        })
    except Exception as e:
        print(f"查询 {underlying} IV 失败: {e}")

    return context


# ==========================================
# 主流程：数据采集 + AI报告生成
# ==========================================

def collect_and_analyze():
    """
    主数据采集模块：
    1. 扫描7天内到期标的
    2. 对每个标的做K线分析 + 策略判断 + 推荐行权价
    3. 汇总成结构化素材，供AI生成报告
    """
    expiring_list = get_expiring_underlying_list(days_ahead=7)

    if not expiring_list:
        return "今日无7天内到期的期权，暂不生成末日期权报告。"

    all_sections = []

    for item in expiring_list:
        underlying = item["underlying"]
        option_type = item["option_type"]
        mat_date = item["maturity_date"]
        days_left = item["days_left"]
        name = item["name"]

        print(f"\n📊 正在分析: {name}({underlying}) | {option_type} | 到期:{mat_date} | 剩余:{days_left}天")

        section = {
            "name": name,
            "underlying": underlying,
            "option_type": option_type,
            "maturity_date": mat_date,
            "days_left": days_left,
            "kline_text": "",
            "iv_current": None,
            "iv_rank": None,
            "iv_level": "未知",
            "strategy": "",
            "strategy_reason": "",
            "recommended_contracts": "",
            "current_price": None,
        }

        # Step 1: K线分析
        opt_contract_month = item.get("contract_month", "")  # 从ts_code提取的期货合约月份
        try:
            # ETF：用中文名（无换月问题）
            # 商品/股指：用具体合约代码（如 PL2504、IF2504），直接从ts_code取月份
            if option_type == "ETF期权":
                kline_query = name
            else:
                fut_symbol = INDEX_OPT_FUTURES_MAP.get(underlying.upper(), underlying.upper())
                kline_query = f"{fut_symbol}{opt_contract_month}" if opt_contract_month else fut_symbol
            print(f"  📊 K线查询: {kline_query}（合约月份来自ts_code: {opt_contract_month}）")
            kline_result = analyze_kline_pattern.invoke({"query": kline_query})
            section["kline_text"] = kline_result
            print(f"  ✅ K线分析完成")
        except Exception as e:
            section["kline_text"] = f"K线分析失败: {e}"
            print(f"  ❌ K线分析失败: {e}")

        # Step 2: 获取 IV 环境并做策略映射
        iv_context = get_iv_context(underlying, option_type, opt_contract_month)
        section["iv_current"] = iv_context.get("current_iv")
        section["iv_rank"] = iv_context.get("iv_rank")
        section["iv_level"] = iv_context.get("iv_level", "未知")
        if section["iv_rank"] is not None:
            print(f"  📈 IV环境: IV={section['iv_current']} | Rank={section['iv_rank']}% | Level={section['iv_level']}")
        else:
            print("  ⚠️ 暂无可用IV数据，策略将按技术面默认逻辑处理")

        strategy, reason = parse_strategy_with_iv(section["kline_text"], iv_context)
        section["strategy"] = strategy
        section["strategy_reason"] = reason
        print(f"  💡 策略判断: {strategy} | {reason}")

        # Step 3: 获取现价（用从ts_code提取的合约月份，而非期权到期日）
        price = get_price_direct(underlying, option_type, opt_contract_month)
        section["current_price"] = price
        if price:
            print(f"  💰 标的现价: {price}")
        else:
            print(f"  ⚠️ 未能获取标的现价，跳过推荐合约")

        # Step 4: 推荐具体合约（需要现价）
        if price:
            try:
                contracts = tool_get_recommended_strikes.invoke({
                    "underlying_code": underlying,
                    "option_type": option_type,
                    "maturity_date_str": mat_date,
                    "current_price": price,
                    "strategy": strategy,
                    "iv_rank": section["iv_rank"],
                })
                section["recommended_contracts"] = contracts

                expected_legs = get_expected_leg_count(section["strategy"])
                actual_codes = extract_contract_codes(str(contracts))
                if expected_legs > 1 and len(actual_codes) < expected_legs:
                    downgrade_map = {
                        "牛市价差": "买看涨",
                        "Bull Call Spread": "Buy Call",
                        "熊市价差": "买看跌",
                        "Bear Put Spread": "Buy Put",
                    }
                    fallback_strategy = downgrade_map.get(section["strategy"])
                    if fallback_strategy:
                        print(f"  ⚠️ {section['strategy']} 合约腿不足({len(actual_codes)}/{expected_legs})，降级为 {fallback_strategy}")
                        section["strategy"] = fallback_strategy
                        section["strategy_reason"] = (
                            f"{section['strategy_reason']}；当前可交易档位不足以构建完整价差，已自动降级为单腿策略。"
                        )
                        contracts = tool_get_recommended_strikes.invoke({
                            "underlying_code": underlying,
                            "option_type": option_type,
                            "maturity_date_str": mat_date,
                            "current_price": price,
                            "strategy": fallback_strategy,
                            "iv_rank": section["iv_rank"],
                        })
                        section["recommended_contracts"] = contracts
                print(f"  ✅ 合约推荐完成")
            except Exception as e:
                section["recommended_contracts"] = f"合约查询失败: {e}"
                print(f"  ❌ 合约推荐失败: {e}")

        all_sections.append(section)
        time.sleep(0.5)  # 避免频繁查库

    return all_sections


# ==========================================
# AI 报告生成（输出完整 HTML，适配情报站 components.html 渲染）
# ==========================================

SYSTEM_PROMPT = """你是爱波塔的期权策略首席分析师，专注末日期权（即将到期期权）机会挖掘。
你的任务是根据提供的素材，生成一份完整的 HTML 格式报告，直接嵌入网页展示。

⚠️ 核心要求：
1. 只输出纯 HTML 代码，不要任何 Markdown、代码块标记（不要```html）
2. 配色风格与平台统一：深色背景 #0f172a，金色标题 #fbbf24，正文 #e2e8f0，副文本 #94a3b8
3. 趋势标签颜色（中国市场：红涨绿跌）：
   - 买看涨/强多头 → 背景 #dc2626（红），白字
   - 买看跌/强空头 → 背景 #16a34a（绿），白字
   - 牛市价差 → 背景 #dc2626（红），白字
   - 熊市价差 → 背景 #16a34a（绿），白字
   - 卖看跌/震荡偏多 → 背景 #f97316（橙），白字
   - 卖看涨/震荡偏空 → 背景 #0ea5e9（蓝），白字
   - 双卖/铁鹰/蝴蝶 → 背景 #d97706（黄），白字
4. 每个标的单独一张卡片，清晰展示：趋势研判 + 策略 + 推荐合约
5. 末尾统一风险提示区块，重点提醒Gamma风险
6. 多腿策略必须逐条完整展示，不得省略任何一条腿：
   - 双卖：2条腿
   - 牛市价差：2条腿（买入Call、卖出Call）
   - 熊市价差：2条腿（买入Put、卖出Put）
   - 蝴蝶：3条腿
   - 铁鹰：4条腿（买入Put、卖出Put、卖出Call、买入Call）
"""


def get_expected_leg_count(strategy: str) -> int:
    return MULTI_LEG_STRATEGY_LEGS.get(strategy, 0)


def extract_contract_codes(text: str) -> list[str]:
    seen = set()
    codes = []
    for code in CONTRACT_CODE_RE.findall(text or ""):
        code = str(code).upper()
        if code not in seen:
            seen.add(code)
            codes.append(code)
    return codes


def get_complete_multi_leg_codes(section: dict) -> list[str]:
    strategy = str(section.get("strategy") or "")
    expected_legs = get_expected_leg_count(strategy)
    if expected_legs <= 1:
        return []

    codes = extract_contract_codes(str(section.get("recommended_contracts") or ""))
    if len(codes) < expected_legs:
        return []
    return codes[:expected_legs]


def collect_missing_contract_codes(html: str, sections: list[dict]) -> list[dict]:
    missing = []
    for section in sections:
        codes = get_complete_multi_leg_codes(section)
        if not codes:
            continue
        for code in codes:
            if code not in html:
                missing.append({
                    "name": section.get("name"),
                    "strategy": section.get("strategy"),
                    "code": code,
                })
    return missing


def clean_generated_html(raw_html: str) -> str:
    html = (raw_html or "").strip()
    return html.replace("```html", "").replace("```", "").strip()


def _build_canonical_symbol_name_map(sections: list[dict]) -> dict[str, str]:
    """
    Build canonical symbol->name map for post-generation correction.
    Priority:
    1) static maps (commodity + ETF)
    2) per-run section values (highest priority, override static)
    """
    symbol_map: dict[str, str] = {}

    for code, name in COMMODITY_NAME_MAP.items():
        k = str(code or "").strip().upper()
        v = str(name or "").strip()
        if k and v:
            symbol_map[k] = v

    for code, name in ETF_NAME_MAP.items():
        k = normalize_etf_code(code)
        v = str(name or "").strip()
        if k and v:
            symbol_map[k] = v
            symbol_map[f"{k}.SH"] = v
            symbol_map[f"{k}.SZ"] = v

    for section in sections or []:
        code = str(section.get("underlying") or "").strip().upper()
        name = str(section.get("name") or "").strip()
        if not code or not name:
            continue
        symbol_map[code] = name
        base_code = normalize_etf_code(code)
        if re.fullmatch(r"\d{6}", base_code):
            symbol_map[base_code] = name
            symbol_map[f"{base_code}.SH"] = name
            symbol_map[f"{base_code}.SZ"] = name

    return symbol_map


def _canonical_symbol_prefix_re(code: str) -> str:
    """
    Avoid treating exchange suffixes in ETF ts_codes as commodity symbols.
    Example: `510050.SH` must not match commodity `SH` (烧碱).
    """
    if re.fullmatch(r"[A-Z]{1,3}", str(code or "").strip().upper()):
        return r"(?<![A-Za-z0-9.])"
    return r"(?<![A-Za-z0-9])"


def enforce_symbol_label_consistency(html: str, sections: list[dict]) -> str:
    """
    Normalize mismatched labels like `RM(豆粕)` or `159915.SZ(豆粕ETF)` to canonical names.
    Keep `code(name)` order for generic text blocks.
    """
    fixed = html or ""
    symbol_map = _build_canonical_symbol_name_map(sections)
    for code, canonical_name in symbol_map.items():
        if not code or not canonical_name:
            continue
        prefix = _canonical_symbol_prefix_re(code)
        pattern = re.compile(
            rf"{prefix}({re.escape(code)})\s*[（(][^）)<]{{1,30}}[）)]",
            flags=re.IGNORECASE,
        )
        fixed = pattern.sub(lambda m, c=code, n=canonical_name: f"{c}（{n}）", fixed)
    return fixed


def enforce_section_title_symbol_order(html: str, sections: list[dict]) -> str:
    """
    For section titles, force canonical display as `中文名（代码）`.
    This keeps the visual style stable (same as historical reports).
    """
    fixed = html or ""
    symbol_map = _build_canonical_symbol_name_map(sections)
    if not symbol_map:
        return fixed

    def _fix_title_block(match: re.Match) -> str:
        block = match.group(0)
        for code, canonical_name in symbol_map.items():
            if not code or not canonical_name:
                continue
            prefix = _canonical_symbol_prefix_re(code)
            # code(name) -> name(code)
            block = re.sub(
                rf"{prefix}{re.escape(code)}\s*[（(][^）)<]{{1,30}}[）)]",
                f"{canonical_name}（{code}）",
                block,
                flags=re.IGNORECASE,
            )
            # anyName(code) -> canonicalName(code)
            block = re.sub(
                rf"[A-Za-z0-9\u4e00-\u9fff·\-\s]{{1,30}}\s*[（(]{re.escape(code)}[）)]",
                f"{canonical_name}（{code}）",
                block,
                flags=re.IGNORECASE,
            )
        return block

    return re.sub(
        r"<h2 class=\"section-title\">.*?</h2>",
        _fix_title_block,
        fixed,
        flags=re.IGNORECASE | re.DOTALL,
    )


def enforce_etf_contract_display_consistency(html: str, sections: list[dict]) -> str:
    """
    Normalize ETF contract names from raw ts_code to strike+认购/认沽 form.
    Example: 10010581.SH（认购） -> 2.9认购（10010581.SH）
    """
    fixed = html or ""
    code_to_label: dict[str, str] = {}
    line_re = re.compile(r"合约:([^\n|]+)\s*\|\s*行权价:([0-9]+(?:\.[0-9]+)?)")
    code_re = re.compile(r"([0-9]{8}\.(?:SH|SZ))", re.IGNORECASE)

    for section in sections or []:
        if str(section.get("option_type") or "").strip() != "ETF期权":
            continue
        contracts_text = str(section.get("recommended_contracts") or "")
        for m in line_re.finditer(contracts_text):
            left = str(m.group(1) or "").strip()
            strike_txt = _format_strike_text(m.group(2))
            cp_label = "认购" if "认购" in left else "认沽" if "认沽" in left else ""
            code_m = code_re.search(left)
            if not code_m or not cp_label:
                continue
            code = code_m.group(1).upper()
            code_to_label[code] = f"{strike_txt}{cp_label}（{code}）"

    if not code_to_label:
        return fixed

    for code, label in code_to_label.items():
        pattern = re.compile(
            rf'(<div class="contract-name">\s*){re.escape(code)}(?:\s*[（(](?:认购|认沽)[）)])?\s*(</div>)',
            flags=re.IGNORECASE,
        )
        fixed = pattern.sub(lambda m, s=label: f"{m.group(1)}{s}{m.group(2)}", fixed)
    return fixed


def build_repair_prompt(original_html: str, sections: list[dict], missing: list[dict]) -> str:
    lines = [
        "你刚生成的末日期权晚报 HTML 存在多腿策略缺腿问题。",
        "请在保留当前 HTML 整体结构、样式、文案风格的前提下，只修复缺失的推荐合约腿。",
        "硬性要求：",
        "1. 仍然只输出纯 HTML，不要 Markdown，不要解释。",
        "2. 不要删减已有卡片和段落。",
        "3. 对缺失腿，必须在对应标的的 contract-box 内新增独立 contract-row。",
        "4. 每条缺失腿都必须按原始合约代码完整展示。",
        "",
        "缺失合约如下：",
    ]
    for item in missing:
        lines.append(f"- {item['name']} | {item['strategy']} | 缺失合约: {item['code']}")

    lines.append("")
    lines.append("对应标的的完整推荐合约素材如下：")
    for section in sections:
        codes = get_complete_multi_leg_codes(section)
        if not codes:
            continue
        lines.append(f"【{section['name']}】{section['strategy']}")
        lines.append(str(section.get("recommended_contracts") or ""))
        lines.append("-" * 30)

    lines.append("")
    lines.append("待修复 HTML 如下：")
    lines.append(original_html)
    return "\n".join(lines)


def build_prompt(sections: list[dict]) -> str:
    today_str = datetime.now().strftime("%Y年%m月%d日")

    lines = [f"""请根据以下素材，生成「末日期权晚报 · {today_str}」的完整HTML报告。

严格使用以下HTML模板结构，只填充内容，不要改变结构：

<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  body {{ margin:0; padding:0; background:#0f172a; font-family:'PingFang SC','Microsoft YaHei',sans-serif; color:#e2e8f0; }}
  .wrap {{ max-width:700px; margin:0 auto; padding:28px 20px; }}
  .header {{ text-align:center; padding:28px 20px; border-radius:16px; background:rgba(15,23,42,0.95); border:1px solid rgba(255,255,255,0.08); margin-bottom:24px; }}
  .section-title {{ color:#fbbf24; font-size:17px; margin:0 0 12px 0; font-weight:600; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }}
  .section-bar {{ width:3px; height:18px; background:#fbbf24; border-radius:2px; flex-shrink:0; }}
  .card {{ background:rgba(30,41,59,0.7); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.07); margin-bottom:16px; }}
  .tag {{ display:inline-block; padding:3px 12px; border-radius:10px; font-size:12px; font-weight:600; color:#fff; margin-left:8px; }}
  .tag-bull {{ background:#dc2626; }}
  .tag-bear {{ background:#16a34a; }}
  .tag-sell-put {{ background:#f97316; }}
  .tag-sell-call {{ background:#0ea5e9; }}
  .tag-neutral {{ background:#d97706; }}
  .label {{ color:#94a3b8; font-size:12px; min-width:70px; display:inline-block; }}
  .value {{ color:#e2e8f0; font-size:13px; }}
  .contract-box {{ background:rgba(15,23,42,0.6); border-radius:10px; padding:12px 14px; margin-top:10px; border:1px solid rgba(251,191,36,0.15); }}
  .contract-row {{ padding:8px 0; border-bottom:1px solid rgba(255,255,255,0.04); }}
  .contract-row:last-child {{ border-bottom:none; }}
  .contract-name {{ color:#e2e8f0; font-size:13px; word-break:break-all; margin-bottom:4px; }}
  .contract-meta {{ display:flex; justify-content:space-between; align-items:center; }}
  .risk-box {{ background:rgba(239,68,68,0.08); border:1px solid rgba(239,68,68,0.25); border-radius:14px; padding:18px; margin-top:8px; }}
  .divider {{ border:none; border-top:1px solid rgba(255,255,255,0.06); margin:20px 0; }}
  .days-badge {{ background:rgba(251,191,36,0.15); color:#fbbf24; border:1px solid rgba(251,191,36,0.3); border-radius:8px; padding:2px 10px; font-size:12px; font-weight:600; }}
</style>
</head>
<body>
<div class="wrap">

  <!-- 头部 -->
  <div class="header">
    <div style="font-size:12px; color:#64748b; letter-spacing:2px; margin-bottom:6px;">爱波塔</div>
    <h1 style="color:#fbbf24; font-size:24px; margin:0; font-weight:700;">📅 末日期权晚报</h1>
    <p style="color:#64748b; font-size:13px; margin-top:10px;">{today_str} | 扫描7天内到期期权 · 智能策略建议</p>
  </div>

  <!-- 导语 -->
  <div class="card" style="margin-bottom:24px;">
    <p style="color:#94a3b8; font-size:14px; margin:0; line-height:1.8;">
      <!-- 用2-3句话概括今日末日期权整体市场状况，有哪些品种即将到期、整体方向偏多偏空还是震荡 -->
    </p>
  </div>

  <!-- 逐个标的分析，每个标的一张卡片，格式如下 -->
  <!--
  <div style="margin-bottom:20px;">
    <h2 class="section-title">
      <span class="section-bar"></span>
      🌱 豆粕（M）
      <span class="days-badge">剩余2天</span>
      <span class="tag tag-bull">买看涨</span>  ← 根据策略选对应class
    </h2>
    <div class="card">
      <div style="margin-bottom:10px;">
        <span class="label">趋势研判</span>
        <span class="value">均线多头排列，近期红三兵形态，多头气势强劲</span>
      </div>
      <div style="margin-bottom:10px;">
        <span class="label">策略理由</span>
        <span class="value">强多头信号，适合买入末日看涨期权博取上涨收益</span>
      </div>
      <div style="margin-bottom:10px;">
        <span class="label">标的现价</span>
        <span class="value" style="color:#fbbf24;">3200</span>
      </div>
      <div class="contract-box">
        <div style="color:#94a3b8; font-size:11px; margin-bottom:8px;">📌 推荐合约</div>
        <div class="contract-row">
          <div class="contract-name">3.2认购（1000xxxx.SH）</div>
          <div class="contract-meta">
            <span style="color:#fbbf24; font-size:13px;">权利金 ≈ 42元</span>
            <span style="color:#64748b; font-size:12px;">持仓 1,200手</span>
          </div>
        </div>
      </div>
    </div>
  </div>
  -->

素材如下，请按照上面的模板格式生成每个标的的分析卡片：
"""]

    for i, s in enumerate(sections, 1):
        lines.append(f"\n【标的 {i}】{s['name']}（{s['underlying']}）{s['option_type']}")
        lines.append(f"名称约束：该标的中文名固定为「{s['name']}」，不得改写为其他品种名；若代码与名称同显，请写成「{s['underlying']}（{s['name']}）」")
        lines.append(f"到期日：{s['maturity_date']} | 剩余天数：{s['days_left']}天")
        if s['current_price']:
            lines.append(f"标的现价：{s['current_price']}")
        if s.get('iv_rank') is not None:
            lines.append(f"IV信息：当前IV {s['iv_current']}% | IV Rank {s['iv_rank']}% | IV等级 {s['iv_level']}")
        lines.append(f"策略判断：{s['strategy']} | 原因：{s['strategy_reason']}")
        lines.append(f"K线摘要（简洁参考）：")
        # 只取K线结果前400字，避免prompt过长
        kline_summary = str(s['kline_text'])[:400] if s['kline_text'] else "无"
        lines.append(kline_summary)
        if s['recommended_contracts']:
            lines.append("推荐合约（必须完整保留下面每一条，不得省略）：")
            lines.append(str(s['recommended_contracts']))
            if str(s.get("option_type") or "").strip() == "ETF期权":
                lines.append("ETF格式要求：contract-name优先展示“行权价+认购/认沽”（例如 3.2认购），官方合约代码仅可放在括号中。")
            multi_leg_codes = get_complete_multi_leg_codes(s)
            if multi_leg_codes:
                lines.append(
                    f"校验要求：该{s['strategy']}策略必须在HTML中完整展示{len(multi_leg_codes)}条腿，"
                    f"且以下合约代码都要出现：{', '.join(multi_leg_codes)}。"
                )
        lines.append("-" * 30)

    lines.append("""
最后在所有标的卡片之后，加上风险提示区块：

  <hr class="divider">
  <div class="risk-box">
    <h3 style="color:#ef4444; margin:0 0 10px 0; font-size:15px;">⚠️ 末日期权风险提示</h3>
    <p style="color:#94a3b8; font-size:13px; margin:0; line-height:1.8;">
      <!-- 提醒：Gamma风险、时间价值快速衰减、流动性风险、建议仓位控制等，3-4条 -->
    </p>
  </div>

  <!-- 底部 -->
  <div style="text-align:center; padding:20px 0; margin-top:20px; border-top:1px solid rgba(255,255,255,0.06);">
    <p style="color:#475569; font-size:12px; margin:0;">爱波塔 · 最懂期权的AI | www.aiprota.com</p>
  </div>

</div>
</body>
</html>

⚠️ 再次强调：只输出HTML代码，不要任何```包裹，不要任何解释文字。
""")
    return "\n".join(lines)


def generate_report(sections: list[dict]) -> str:
    """调用LLM生成HTML格式报告"""
    prompt = build_prompt(sections)
    print("\n🤖 正在调用AI生成HTML报告...")

    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=prompt)
    ]
    response = llm.invoke(messages)

    html = clean_generated_html(response.content)

    missing = collect_missing_contract_codes(html, sections)
    if missing:
        print(f"  ⚠️ 检测到多腿策略缺失 {len(missing)} 条合约腿，触发一次HTML修复重生成...")
        repair_prompt = build_repair_prompt(html, sections, missing)
        repair_messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=repair_prompt)
        ]
        repair_response = llm.invoke(repair_messages)
        repaired_html = clean_generated_html(repair_response.content)
        repaired_missing = collect_missing_contract_codes(repaired_html, sections)
        if len(repaired_missing) < len(missing):
            html = repaired_html
            missing = repaired_missing

        if missing:
            missing_codes = ", ".join(item["code"] for item in missing)
            print(f"  ⚠️ 修复后仍有缺失腿未补齐：{missing_codes}")
        else:
            print("  ✅ 多腿策略缺腿已自动修复。")

    fixed_html = enforce_symbol_label_consistency(html, sections)
    if fixed_html != html:
        print("  ⚠️ 检测到标的名称错配，已按代码映射自动纠偏。")
        html = fixed_html

    normalized_titles_html = enforce_section_title_symbol_order(html, sections)
    if normalized_titles_html != html:
        print("  ⚠️ 标题标的名已统一为“中文名（代码）”。")
        html = normalized_titles_html

    normalized_html = enforce_etf_contract_display_consistency(html, sections)
    if normalized_html != html:
        print("  ⚠️ 检测到ETF合约名仍为代码主显，已自动改为行权价样式。")
        html = normalized_html

    return html


# ==========================================
# 发布到站内消息
# ==========================================

def publish_report(report_content: str):
    """
    将报告发布到订阅中心。
    使用 sub_svc.publish_content(channel_code, title, content, summary)
    站内消息通知由 publish_content 内部自动处理，无需额外调用。

    前提：数据库 content_channels 表中需要存在 code='expiry_option_radar' 的频道记录。
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    title = f"末日期权晚报 | {today_str}"
    summary = f"扫描7天内即将到期的ETF及商品期权，结合K线技术分析给出买/卖方向策略建议。"

    # channel_code 必须与 content_channels 表中的 code 字段一致
    CHANNEL_CODE = "expiry_option_radar"

    try:
        success, result = sub_svc.publish_content(
            channel_code=CHANNEL_CODE,
            title=title,
            content=report_content,
            summary=summary
        )
        if success:
            print(f"✅ 报告已发布，content_id={result}，标题：{title}")
            print("✅ 站内消息通知已由 publish_content 自动发送给所有订阅用户")
        else:
            print(f"❌ 发布失败：{result}")
            print("   → 请确认 content_channels 表中存在 code='expiry_option_radar' 的频道")
            _fallback_print(title, report_content)

    except Exception as e:
        print(f"❌ 发布异常: {e}")
        _fallback_print(title, report_content)


def _fallback_print(title: str, content: str):
    """发布失败时降级打印到控制台"""
    print("\n" + "=" * 60)
    print(f"【{title}】")
    print(content)
    print("=" * 60)


# ==========================================
# 主入口
# ==========================================

def main():
    print("=" * 60)
    print("🚀 末日期权晚报生成器 启动")
    print(f"⏰ 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Step 1: 数据采集 + 分析
    sections = collect_and_analyze()

    if isinstance(sections, str):
        # 返回字符串说明无数据
        print(f"\n⚠️ {sections}")
        return

    if not sections:
        print("\n⚠️ 无分析数据，退出。")
        return

    print(f"\n✅ 共完成 {len(sections)} 个标的分析")

    # Step 2: AI生成报告
    report = generate_report(sections)

    # Step 3: 发布
    publish_report(report)

    print("\n🎉 末日期权晚报生成完成！")
    return report


if __name__ == "__main__":
    main()
