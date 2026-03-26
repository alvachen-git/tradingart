import os
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from langchain_core.tools import tool
from sqlalchemy import create_engine, text


load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("数据库配置缺失，请检查 .env 中 DB_HOST / DB_USER / DB_PASSWORD / DB_NAME")


@st.cache_resource
def get_db_engine():
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


engine = get_db_engine()

FRESHNESS_THRESHOLD_BY_FREQ = {
    "D": 7,
    "W": 21,
    "M": 45,
    "Q": 120,
}

FRED_CORE_CODES = [
    "FEDFUNDS",
    "SOFR",
    "DGS2",
    "DGS10",
    "T10Y3M",
    "CPIAUCSL",
    "PCEPILFE",
    "DFII10",
    "UNRATE",
    "PAYEMS",
    "BAMLH0A0HYM2",
    "WALCL",
    "GFDEBTN",
    "GDP",
    "GFDEGDQ188S",
]

CODE_ALIAS_MAP = {
    "USTOTD": "GFDEBTN",
    "USAGDP": "GDP",
    "USDEBTGDP": "GFDEGDQ188S",
    "DEBTGDP": "GFDEGDQ188S",
}

CODE_META_FALLBACK = {
    "FEDFUNDS": {"source": "fred", "frequency": "M", "unit": "%"},
    "SOFR": {"source": "fred", "frequency": "D", "unit": "%"},
    "DGS2": {"source": "fred", "frequency": "D", "unit": "%"},
    "DGS10": {"source": "fred", "frequency": "D", "unit": "%"},
    "T10Y3M": {"source": "fred", "frequency": "D", "unit": "%"},
    "CPIAUCSL": {"source": "fred", "frequency": "M", "unit": "index"},
    "PCEPILFE": {"source": "fred", "frequency": "M", "unit": "index"},
    "DFII10": {"source": "fred", "frequency": "D", "unit": "%"},
    "UNRATE": {"source": "fred", "frequency": "M", "unit": "%"},
    "PAYEMS": {"source": "fred", "frequency": "M", "unit": "thousand_persons"},
    "BAMLH0A0HYM2": {"source": "fred", "frequency": "D", "unit": "%"},
    "WALCL": {"source": "fred", "frequency": "W", "unit": "million_usd"},
    "GFDEBTN": {"source": "fred", "frequency": "Q", "unit": "million_usd"},
    "GDP": {"source": "fred", "frequency": "Q", "unit": "billion_usd"},
    "GFDEGDQ188S": {"source": "fred", "frequency": "Q", "unit": "%"},
    "US10Y": {"source": "akshare", "frequency": "D", "unit": "%"},
    "US2Y": {"source": "akshare", "frequency": "D", "unit": "%"},
    "US30Y": {"source": "akshare", "frequency": "D", "unit": "%"},
    "CN10Y": {"source": "akshare", "frequency": "D", "unit": "%"},
    "CN2Y": {"source": "akshare", "frequency": "D", "unit": "%"},
    "CN30Y": {"source": "akshare", "frequency": "D", "unit": "%"},
    "JP10Y": {"source": "fred", "frequency": "M", "unit": "%"},
    "JP2Y": {"source": "fred", "frequency": "M", "unit": "%"},
    "JP30Y": {"source": "fred", "frequency": "M", "unit": "%"},
    "DXY": {"source": "multi_source", "frequency": "D", "unit": "index"},
    "USDCNH": {"source": "multi_source", "frequency": "D", "unit": "fx"},
    "BDI": {"source": "akshare", "frequency": "D", "unit": "index"},
}


def _parse_codes(indicator_code: str) -> list[str]:
    out = []
    for raw_code in (indicator_code or "").split(","):
        code = raw_code.strip().upper()
        if not code:
            continue
        out.append(CODE_ALIAS_MAP.get(code, code))
    return out


def _infer_frequency_from_category(category: str) -> str:
    if category in {"inflation", "growth"}:
        return "M"
    if category == "liquidity":
        return "W"
    if category == "debt":
        return "Q"
    return "D"


def _load_meta_from_db() -> dict[str, dict[str, str]]:
    sql = text(
        """
        SELECT indicator_code, source, frequency, unit
        FROM macro_indicator_meta
        """
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
        if df.empty:
            return {}
        out = {}
        for _, row in df.iterrows():
            out[str(row["indicator_code"]).upper()] = {
                "source": str(row.get("source") or "unknown"),
                "frequency": str(row.get("frequency") or "D").upper(),
                "unit": str(row.get("unit") or "value"),
            }
        return out
    except Exception:
        return {}


def _resolve_meta(code: str, category: str, db_meta: dict[str, dict[str, str]]) -> dict[str, str]:
    if code in db_meta:
        return db_meta[code]
    if code in CODE_META_FALLBACK:
        return CODE_META_FALLBACK[code]
    return {
        "source": "unknown",
        "frequency": _infer_frequency_from_category(category),
        "unit": "value",
    }


def _freshness(as_of: object, frequency: str) -> tuple[str, int, int]:
    as_of_dt = pd.to_datetime(as_of, errors="coerce")
    threshold = FRESHNESS_THRESHOLD_BY_FREQ.get((frequency or "D").upper(), 45)
    if pd.isna(as_of_dt):
        return "missing", -1, threshold
    stale_days = (datetime.now().date() - as_of_dt.date()).days
    status = "fresh" if stale_days <= threshold else "stale"
    return status, stale_days, threshold


def _format_value(code: str, value: float, unit: str) -> str:
    if pd.isna(value):
        return "N/A"
    if unit == "%":
        return f"{float(value):.2f}%"
    if unit == "million_usd":
        return f"{float(value):,.0f} (million USD)"
    if unit == "billion_usd":
        return f"{float(value):,.2f} (billion USD)"
    if unit == "thousand_persons":
        return f"{float(value):,.0f} (thousand persons)"
    if code in {"BDI", "PAYEMS"}:
        return f"{float(value):,.0f}"
    return f"{float(value):.4f}"


def _suggestion_by_frequency(frequency: str) -> str:
    freq = (frequency or "D").upper()
    if freq == "D":
        return "建议检查当日/近7日更新任务与 FRED 接口连通性。"
    if freq == "W":
        return "建议确认周更节奏（通常每周更新）并核对最近一周任务日志。"
    if freq == "M":
        return "建议确认月度发布日期窗口，若已过发布窗口请检查抓取任务。"
    return "建议核对季度发布时间和历史回补窗口配置。"


@tool
def get_macro_indicator(indicator_code: str, days: int = 30) -> str:
    """
    【宏观指标查询】
    查询指定宏观指标的最近数据，支持逗号分隔多个代码。

    参数:
    - indicator_code: 指标代码，如 "US10Y,DXY,CN10Y"
    - days: 回看天数（用于统计区间与趋势）
    """
    code_list = _parse_codes(indicator_code)
    if not code_list:
        return "❌ 请提供有效的指标代码"

    db_meta = _load_meta_from_db()
    result_blocks = []

    try:
        for code in code_list:
            sql = text(
                """
                SELECT trade_date, indicator_name, category, close_value, change_value, change_pct
                FROM macro_daily
                WHERE indicator_code = :code
                ORDER BY trade_date DESC
                LIMIT :days
                """
            )
            with engine.connect() as conn:
                df = pd.read_sql(sql, conn, params={"code": code, "days": days})

            if df.empty:
                result_blocks.append(
                    f"⚠️ **{code}**\n"
                    f"- 当前状态: missing\n"
                    f"- 原因: macro_daily 无记录\n"
                    f"- 建议: 先执行宏观日更 (`run_daily2.sh` -> `update_micro_daily.py`) 再重试"
                )
                continue

            latest = df.iloc[0]
            category = str(latest.get("category") or "unknown")
            meta = _resolve_meta(code, category, db_meta)
            status, stale_days, threshold = _freshness(latest["trade_date"], meta["frequency"])

            name = str(latest.get("indicator_name") or code)
            latest_value = _format_value(code, pd.to_numeric(latest.get("close_value"), errors="coerce"), meta["unit"])
            change_pct = pd.to_numeric(latest.get("change_pct"), errors="coerce")
            date_str = pd.to_datetime(latest["trade_date"]).strftime("%Y-%m-%d")

            trend = "震荡"
            if len(df) >= 5:
                recent = pd.to_numeric(df.head(5)["close_value"], errors="coerce").mean()
                older = pd.to_numeric(df.tail(5)["close_value"], errors="coerce").mean()
                if pd.notna(recent) and pd.notna(older) and older != 0:
                    if recent > older * 1.01:
                        trend = "上行"
                    elif recent < older * 0.99:
                        trend = "下行"

            high = pd.to_numeric(df["close_value"], errors="coerce").max()
            low = pd.to_numeric(df["close_value"], errors="coerce").min()
            change_str = f"{change_pct:+.2f}%" if pd.notna(change_pct) else "N/A"

            block_lines = [
                f"📊 **{name}** ({code})",
                f"- 最新值: {latest_value}",
                f"- 日期: {date_str}",
                f"- 涨跌幅: {change_str}",
                f"- 趋势: {trend}",
                f"- 区间({len(df)}): {low:.4f} ~ {high:.4f}",
                f"- source: {meta['source']}",
                f"- as_of_date: {date_str}",
                f"- freshness_status: {status}",
                f"- stale_days: {stale_days}",
            ]
            if status == "stale":
                block_lines.append(f"- 建议: 已超过{threshold}天阈值，{_suggestion_by_frequency(meta['frequency'])}")
            result_blocks.append("\n".join(block_lines))

        return "\n\n".join(result_blocks)

    except Exception as e:
        return f"查询失败: {str(e)}"


@tool
def get_macro_health_snapshot(indicator_code: str = "") -> str:
    """
    【宏观健康快照】
    返回关键指标的一次性健康检查，包括 source/as_of/freshness、缺失原因和建议。

    参数:
    - indicator_code: 可选，逗号分隔。为空时默认检查 FRED 核心12条。
    """
    code_list = _parse_codes(indicator_code) if indicator_code else FRED_CORE_CODES
    db_meta = _load_meta_from_db()

    rows = []
    missing_details = []
    for code in code_list:
        sql = text(
            """
            SELECT indicator_name, category, close_value, trade_date
            FROM macro_daily
            WHERE indicator_code = :code
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"code": code})

        if df.empty:
            rows.append(f"| {code} | N/A | N/A | unknown | missing | - |")
            missing_details.append(
                f"- {code}: 数据缺失。建议检查 `update_micro_daily.py` 任务日志中的 `FRED_FETCH_FAIL`。"
            )
            continue

        latest = df.iloc[0]
        category = str(latest.get("category") or "unknown")
        meta = _resolve_meta(code, category, db_meta)
        as_of_date = pd.to_datetime(latest["trade_date"], errors="coerce")
        as_of_str = as_of_date.strftime("%Y-%m-%d") if pd.notna(as_of_date) else "N/A"
        status, stale_days, threshold = _freshness(as_of_date, meta["frequency"])

        val = _format_value(code, pd.to_numeric(latest.get("close_value"), errors="coerce"), meta["unit"])
        rows.append(f"| {code} | {val} | {as_of_str} | {meta['source']} | {status} | {stale_days} |")
        if status == "stale":
            missing_details.append(
                f"- {code}: 数据陈旧（{stale_days}天 > 阈值{threshold}天）。{_suggestion_by_frequency(meta['frequency'])}"
            )

    result = [
        "📋 **宏观健康快照**",
        "",
        "| 指标 | 最新值 | as_of_date | source | freshness_status | stale_days |",
        "|---|---:|---|---|---|---:|",
        *rows,
    ]

    if missing_details:
        result.extend(["", "⚠️ **异常与建议**", *missing_details])
    else:
        result.extend(["", "✅ 所有指标状态正常。"])

    return "\n".join(result)


@tool
def get_us_debt_gdp_snapshot() -> str:
    """
    【美债/GDP快照】
    返回美国联邦债务、美国GDP、债务占GDP比的最新值、来源与新鲜度。
    """
    target_codes = ["GFDEBTN", "GDP", "GFDEGDQ188S"]
    db_meta = _load_meta_from_db()
    snapshots = {}

    for code in target_codes:
        sql = text(
            """
            SELECT indicator_name, category, close_value, trade_date
            FROM macro_daily
            WHERE indicator_code = :code
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"code": code})
        if df.empty:
            snapshots[code] = None
            continue
        snapshots[code] = df.iloc[0]

    if snapshots["GFDEBTN"] is None or snapshots["GDP"] is None:
        missing_codes = [k for k, v in snapshots.items() if v is None]
        return (
            "⚠️ 美债/GDP关键数据不完整。\n"
            f"- 缺失指标: {', '.join(missing_codes)}\n"
            "- 建议: 先执行 `update_micro_daily.py`，再检查 FRED_FETCH_FAIL 日志。"
        )

    debt = snapshots["GFDEBTN"]
    gdp = snapshots["GDP"]
    ratio_official = snapshots["GFDEGDQ188S"]

    debt_val_musd = pd.to_numeric(debt["close_value"], errors="coerce")
    gdp_val_busd = pd.to_numeric(gdp["close_value"], errors="coerce")
    ratio_calc = None
    if pd.notna(debt_val_musd) and pd.notna(gdp_val_busd) and gdp_val_busd != 0:
        ratio_calc = (debt_val_musd / (gdp_val_busd * 1000.0)) * 100.0

    debt_meta = _resolve_meta("GFDEBTN", str(debt.get("category") or "debt"), db_meta)
    gdp_meta = _resolve_meta("GDP", str(gdp.get("category") or "growth"), db_meta)
    debt_status, debt_stale_days, _ = _freshness(debt["trade_date"], debt_meta["frequency"])
    gdp_status, gdp_stale_days, _ = _freshness(gdp["trade_date"], gdp_meta["frequency"])

    ratio_lines = []
    if ratio_official is not None:
        ratio_official_val = pd.to_numeric(ratio_official["close_value"], errors="coerce")
        ratio_meta = _resolve_meta("GFDEGDQ188S", "debt", db_meta)
        ratio_status, ratio_stale_days, _ = _freshness(ratio_official["trade_date"], ratio_meta["frequency"])
        ratio_lines.extend(
            [
                f"- 官方债务/GDP: {ratio_official_val:.2f}% (code=GFDEGDQ188S)",
                f"- 官方口径 as_of_date: {pd.to_datetime(ratio_official['trade_date']).strftime('%Y-%m-%d')}",
                f"- 官方口径 freshness_status: {ratio_status}, stale_days: {ratio_stale_days}",
            ]
        )

    calc_line = f"- 计算债务/GDP: {ratio_calc:.2f}%" if ratio_calc is not None else "- 计算债务/GDP: N/A"
    return "\n".join(
        [
            "📌 **美国债务/GDP快照**",
            f"- 联邦债务: {_format_value('GFDEBTN', debt_val_musd, 'million_usd')}",
            f"- 联邦债务 as_of_date: {pd.to_datetime(debt['trade_date']).strftime('%Y-%m-%d')}",
            f"- 联邦债务 source: {debt_meta['source']}, freshness_status: {debt_status}, stale_days: {debt_stale_days}",
            f"- 美国GDP: {_format_value('GDP', gdp_val_busd, 'billion_usd')}",
            f"- 美国GDP as_of_date: {pd.to_datetime(gdp['trade_date']).strftime('%Y-%m-%d')}",
            f"- 美国GDP source: {gdp_meta['source']}, freshness_status: {gdp_status}, stale_days: {gdp_stale_days}",
            calc_line,
            *ratio_lines,
        ]
    )


@tool
def get_macro_overview(category: str = "all") -> str:
    """
    【宏观环境总览】
    获取宏观环境的综合概览。

    参数:
    - category: all/bond/fx/shipping/inflation/growth/credit/liquidity
    """
    try:
        if category == "all":
            where_clause = "1=1"
        else:
            where_clause = "category = :category"

        sql = text(
            f"""
            SELECT m1.indicator_code, m1.indicator_name, m1.category,
                   m1.close_value, m1.change_pct, m1.trade_date
            FROM macro_daily m1
            INNER JOIN (
                SELECT indicator_code, MAX(trade_date) AS max_date
                FROM macro_daily
                WHERE {where_clause}
                GROUP BY indicator_code
            ) m2 ON m1.indicator_code = m2.indicator_code AND m1.trade_date = m2.max_date
            ORDER BY m1.category, m1.indicator_code
            """
        )

        with engine.connect() as conn:
            if category == "all":
                df = pd.read_sql(sql, conn)
            else:
                df = pd.read_sql(sql, conn, params={"category": category})

        if df.empty:
            return "暂无宏观数据"

        db_meta = _load_meta_from_db()
        result_lines = ["📊 **宏观环境总览**", ""]
        latest_dates = []

        for cat in df["category"].unique():
            result_lines.append(f"**{cat}**")
            cat_df = df[df["category"] == cat]
            for _, row in cat_df.iterrows():
                code = str(row["indicator_code"]).upper()
                meta = _resolve_meta(code, str(row["category"]), db_meta)
                status, stale_days, _ = _freshness(row["trade_date"], meta["frequency"])
                val = _format_value(code, pd.to_numeric(row["close_value"], errors="coerce"), meta["unit"])
                chg = pd.to_numeric(row["change_pct"], errors="coerce")
                chg_str = f"{chg:+.2f}%" if pd.notna(chg) else "-"
                result_lines.append(
                    f"- {row['indicator_name']} ({code}): {val} ({chg_str}) | {meta['source']} | {status}/{stale_days}d"
                )
                latest_dates.append(row["trade_date"])
            result_lines.append("")

        if latest_dates:
            latest = pd.to_datetime(max(latest_dates)).strftime("%Y-%m-%d")
            result_lines.append(f"🗓 数据截至: {latest}")

        return "\n".join(result_lines)

    except Exception as e:
        return f"查询失败: {str(e)}"


@tool
def analyze_yield_curve() -> str:
    """
    【收益率曲线分析】
    分析中美日国债收益率曲线形态，判断经济预期。
    """
    try:
        sql = text(
            """
            SELECT m1.indicator_code, m1.close_value
            FROM macro_daily m1
            INNER JOIN (
                SELECT indicator_code, MAX(trade_date) AS max_date
                FROM macro_daily
                WHERE indicator_code IN
                      ('US2Y', 'US10Y', 'US30Y', 'CN2Y', 'CN10Y', 'CN30Y', 'JP2Y', 'JP10Y', 'JP30Y', 'DGS2', 'DGS10')
                GROUP BY indicator_code
            ) m2 ON m1.indicator_code = m2.indicator_code AND m1.trade_date = m2.max_date
            """
        )

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        if df.empty:
            return "暂无国债利率数据"

        rates = dict(zip(df["indicator_code"], pd.to_numeric(df["close_value"], errors="coerce")))

        us2 = rates.get("US2Y") if pd.notna(rates.get("US2Y")) else rates.get("DGS2")
        us10 = rates.get("US10Y") if pd.notna(rates.get("US10Y")) else rates.get("DGS10")
        us30 = rates.get("US30Y")

        lines = ["📈 **收益率曲线分析**", ""]

        if pd.notna(us2) and pd.notna(us10):
            spread_10_2 = us10 - us2
            invert_tag = "⚠️ 倒挂(衰退预警)" if spread_10_2 < 0 else "✅ 正常"
            us30_str = f" | 30Y: {us30:.2f}%" if pd.notna(us30) else ""
            lines.append("**🇺🇸 美国国债**")
            lines.append(f"- 2Y: {us2:.2f}% | 10Y: {us10:.2f}%{us30_str}")
            lines.append(f"- 10Y-2Y利差: {spread_10_2:+.2f}% {invert_tag}")

        if all(pd.notna(rates.get(k)) for k in ["CN2Y", "CN10Y", "CN30Y"]):
            cn2, cn10, cn30 = rates["CN2Y"], rates["CN10Y"], rates["CN30Y"]
            lines.append("")
            lines.append("**🇨🇳 中国国债**")
            lines.append(f"- 2Y: {cn2:.2f}% | 10Y: {cn10:.2f}% | 30Y: {cn30:.2f}%")
            lines.append(f"- 10Y-2Y利差: {cn10 - cn2:+.2f}%")

            if pd.notna(us10):
                cn_us_spread = cn10 - us10
                side = "中国更高" if cn_us_spread > 0 else "美国更高"
                lines.append(f"- 中美10Y利差: {cn_us_spread:+.2f}% ({side})")

        if all(pd.notna(rates.get(k)) for k in ["JP2Y", "JP10Y", "JP30Y"]):
            jp2, jp10, jp30 = rates["JP2Y"], rates["JP10Y"], rates["JP30Y"]
            lines.append("")
            lines.append("**🇯🇵 日本国债**")
            lines.append(f"- 2Y: {jp2:.2f}% | 10Y: {jp10:.2f}% | 30Y: {jp30:.2f}%")

        return "\n".join(lines)

    except Exception as e:
        return f"分析失败: {str(e)}"


def get_macro_history(indicator_code: str, start_date: str = None, end_date: str = None) -> str:
    """
    【宏观指标历史数据】
    查询指定宏观指标的历史数据。
    """
    try:
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

        start_dt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        end_dt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

        sql = text(
            """
            SELECT trade_date, close_value, change_pct
            FROM macro_daily
            WHERE indicator_code = :code
              AND trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date DESC
            LIMIT 30
            """
        )

        with engine.connect() as conn:
            df = pd.read_sql(
                sql,
                conn,
                params={
                    "code": indicator_code.upper(),
                    "start_date": start_dt,
                    "end_date": end_dt,
                },
            )

        if df.empty:
            return f"未找到 {indicator_code} 在该时间段的数据"

        latest = pd.to_numeric(df.iloc[0]["close_value"], errors="coerce")
        high = pd.to_numeric(df["close_value"], errors="coerce").max()
        low = pd.to_numeric(df["close_value"], errors="coerce").min()
        avg = pd.to_numeric(df["close_value"], errors="coerce").mean()

        return (
            f"📊 **{indicator_code} 历史数据** ({start_dt} ~ {end_dt})\n\n"
            "| 统计项 | 数值 |\n"
            "|---|---:|\n"
            f"| 最新值 | {latest:.4f} |\n"
            f"| 最高值 | {high:.4f} |\n"
            f"| 最低值 | {low:.4f} |\n"
            f"| 平均值 | {avg:.4f} |\n"
            f"| 数据条数 | {len(df)} |"
        )

    except Exception as e:
        return f"查询失败: {str(e)}"
