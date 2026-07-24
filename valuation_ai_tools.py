"""Deterministic valuation context for AI tools.

The module reads local valuation tables only.  It deliberately separates a
security's own historical valuation from the valuation environment of broad
equity indices so an LLM cannot turn a low index percentile into a company
fundamental conclusion.
"""

from __future__ import annotations

import math
import re
from typing import Any

import pandas as pd
from langchain_core.tools import tool
from pydantic import BaseModel, Field
from sqlalchemy import text

from global_index_valuation import (
    INDEX_SPECS,
    build_global_index_valuation_dashboard,
    empirical_percentile,
    percentile_label,
)


STOCK_HISTORY_LIMIT = 3200
STOCK_HISTORY_YEARS = 10
RELIABLE_INDEX_QUALITY = {"ok", "proxy"}

INDEX_NAME_BY_CODE = {spec.code: spec.name for spec in INDEX_SPECS}

# Long aliases come first so "恒生科技指数" is not consumed by "恒生指数".
INDEX_ALIASES: dict[str, tuple[str, ...]] = {
    "NASDAQ100": ("纳斯达克100", "纳指100", "NASDAQ100", "NDX", "QQQ"),
    "SP500": ("标普500", "标普五百", "S&P500", "SP500", "SPX", "SPY"),
    "RUSSELL2000": ("罗素2000", "罗素两千", "RUSSELL2000", "RUT", "IWM"),
    "000300": ("沪深300", "沪深三百", "000300.SH", "000300", "300ETF", "510300"),
    "399006": ("创业板指数", "创业板指", "399006.SZ", "399006", "创业板ETF", "159915"),
    "000688": ("科创板50", "科创50", "000688.SH", "000688", "科创50ETF", "588000", "588080"),
    "000905": ("中证500", "中证五百", "000905.SH", "000905", "500ETF", "510500"),
    "000852": ("中证1000", "中证一千", "000852.SH", "000852", "1000ETF", "512100"),
    "932000": ("中证2000", "中证两千", "932000.CSI", "932000", "2000ETF"),
    "HSTECH": ("恒生科技指数", "恒生科技", "恒科", "HSTECH"),
    "HSI": ("恒生指数", "恒指", "HSI"),
}

MARKET_ALIASES = {
    "美国": ("美股", "美国股市", "美国市场", "美国指数"),
    "A股": ("A股", "a股", "沪深股市", "中国A股", "中国股市"),
    "香港": ("港股", "香港股市", "香港市场", "香港指数"),
}

ETF_REFERENCE_ALIASES = (
    "QQQ", "SPY", "IWM",
    "300ETF", "510300", "500ETF", "510500", "1000ETF", "512100",
    "创业板ETF", "159915", "科创50ETF", "588000", "588080", "2000ETF",
)


class GlobalIndexValuationInput(BaseModel):
    query: str = Field(
        default="",
        description=(
            "指数、市场或原问题，例如‘标普500’、‘A股’、‘全球哪个指数估值分位最低’；"
            "留空返回全部11个指数"
        ),
    )
    as_of_date: str = Field(
        default="",
        description="可选截止日期，格式 YYYYMMDD 或 YYYY-MM-DD；留空使用本地最新数据",
    )


def _default_engine() -> Any:
    # Lazy import avoids opening another connection pool during normal module import.
    from data_engine import engine

    return engine


def _compact_date(value: Any) -> str:
    raw = str(value or "").strip().replace("-", "").replace("/", "")
    if len(raw) != 8 or not raw.isdigit():
        return ""
    try:
        pd.to_datetime(raw, format="%Y%m%d", errors="raise")
    except (TypeError, ValueError):
        return ""
    return raw


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _format_number(value: Any, digits: int = 2) -> str:
    number = _number(value)
    return "--" if number is None else f"{number:,.{digits}f}"


def _format_percentile(value: Any) -> str:
    number = _number(value)
    return "--" if number is None else f"{number:.0f}/100"


def _format_signed_pct(value: Any) -> str:
    number = _number(value)
    return "--" if number is None else f"{number:+.1f}%"


def _contains_alias(text_value: str, alias: str) -> bool:
    if not alias:
        return False
    if re.fullmatch(r"[A-Za-z0-9.&]+", alias):
        return bool(re.search(rf"(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])", text_value, re.I))
    return alias.lower() in text_value.lower()


def match_index_codes(query: str) -> list[str]:
    """Return requested index codes in the product's fixed display order."""
    text_value = str(query or "").strip()
    selected: set[str] = set()
    for code, aliases in INDEX_ALIASES.items():
        if any(_contains_alias(text_value, alias) for alias in aliases):
            selected.add(code)
    return [spec.code for spec in INDEX_SPECS if spec.code in selected]


def _resolve_security(query: str) -> tuple[str, str]:
    try:
        import symbol_map

        text_value = str(query or "").strip()
        explicit_code = re.search(
            r"(?<!\d)(\d{6})(?:\.(SH|SZ|BJ))?(?!\d)", text_value, flags=re.I,
        )
        if explicit_code:
            digits, suffix = explicit_code.groups()
            candidate = f"{digits}.{suffix.upper()}" if suffix else digits
            resolved = symbol_map.resolve_symbol(candidate)
            if resolved and resolved[0]:
                return str(resolved[0]).upper(), str(resolved[1] or "").lower()

        resolved = symbol_map.resolve_symbol(text_value)
        if resolved and resolved[0]:
            return str(resolved[0]).upper(), str(resolved[1] or "").lower()

        normalized = text_value.upper()
        aliases = getattr(symbol_map, "COMMON_ALIASES", {}) or {}
        for alias, code in sorted(aliases.items(), key=lambda item: len(str(item[0])), reverse=True):
            alias_text = str(alias or "").strip().upper()
            code_text = str(code or "").strip().upper()
            if len(alias_text) < 2 or alias_text not in normalized:
                continue
            if code_text in getattr(symbol_map, "INDEX_CODES_SET", set()):
                return code_text, "index"
            if code_text.endswith((".SH", ".SZ", ".BJ", ".HK", ".US")):
                return code_text, "stock"

        us_aliases = symbol_map.get_us_stock_alias_map()
        for alias, ticker in sorted(us_aliases.items(), key=lambda item: len(str(item[0])), reverse=True):
            if _contains_alias(text_value, str(alias or "")):
                return f"{str(ticker).upper()}.US", "stock"
    except Exception:
        return "", ""
    return "", ""


def benchmark_codes_for_security(ts_code: str) -> tuple[list[str], str]:
    """Map a security to transparent style references, never membership claims."""
    code = str(ts_code or "").strip().upper()
    digits = code.split(".", 1)[0]
    if code.endswith(".SZ") and digits.startswith(("300", "301")):
        return ["399006", "000300", "000852"], "创业板及A股大盘、小盘风格参考"
    if code.endswith(".SH") and digits.startswith(("688", "689")):
        return ["000688", "000300", "000852"], "科创板及A股大盘、小盘风格参考"
    if code.endswith(".BJ"):
        return ["000300", "000905", "000852", "932000"], "暂无北交所专属指数，以下仅作A股风格环境参考"
    if code.endswith((".SH", ".SZ")):
        return ["000300", "000905", "000852"], "A股大盘、中盘、小盘风格参考"
    if code.endswith(".HK"):
        return ["HSI", "HSTECH"], "香港市场宽基与科技风格参考"
    if code.endswith(".US"):
        return ["NASDAQ100", "SP500", "RUSSELL2000"], "美国成长、大盘、小盘风格参考"
    return [], ""


def _select_index_scope(query: str) -> tuple[list[str], str, str, str]:
    text_value = str(query or "").strip()
    explicit_codes = match_index_codes(text_value)
    if explicit_codes:
        etf_note = "所列PE来自ETF跟踪指数，仅作为ETF估值环境参考。" if any(
            alias.lower() in text_value.lower() for alias in ETF_REFERENCE_ALIASES
        ) else ""
        return explicit_codes, "指定指数", "", etf_note

    selected_markets = [
        market for market, aliases in MARKET_ALIASES.items()
        if any(alias.lower() in text_value.lower() for alias in aliases)
    ]
    if selected_markets:
        selected = [
            spec.code for spec in INDEX_SPECS if spec.market in selected_markets
        ]
        return selected, "、".join(selected_markets), "", ""

    if not text_value or any(keyword in text_value for keyword in ("全球", "各大股市", "各市场", "全部指数")):
        return [spec.code for spec in INDEX_SPECS], "全球", "", ""

    ts_code, asset_type = _resolve_security(text_value)
    if asset_type == "stock" and ts_code:
        selected, note = benchmark_codes_for_security(ts_code)
        if selected:
            return selected, "风格参考基准", ts_code, note

    return [spec.code for spec in INDEX_SPECS], "全球", "", ""


def _reduced_index_card(card: dict[str, Any]) -> dict[str, Any]:
    return {
        key: card.get(key)
        for key in (
            "code", "name", "market", "current_pe", "percentile", "percentile_label",
            "history_label", "median_pe", "median_deviation_pct", "data_date",
            "source_name", "source_url", "is_proxy", "quality_status", "quality_message",
        )
    }


def _render_global_index_report(result: dict[str, Any]) -> str:
    if result.get("status") in {"invalid_request", "error", "missing", "no_data"}:
        lines = ["【全球主要指数估值】", "结论：数据不足"]
        for gap in result.get("gaps") or []:
            lines.append(f"- 原因：{gap}")
        return "\n".join(lines)

    lines = [
        "【全球主要指数估值】",
        f"结论：以下按各指数相对自身历史的PE分位比较，范围为{result.get('scope_label') or '指定范围'}。",
    ]
    for card in result.get("cards") or []:
        percentile = card.get("percentile")
        rank_text = (
            f"{_format_percentile(percentile)}（{card.get('percentile_label') or '样本不足'}）"
            if percentile is not None else "样本不足"
        )
        quality_text = ""
        if card.get("quality_status") == "stale":
            quality_text = "，仅作历史参考"
        elif card.get("quality_status") in {"source_mismatch", "insufficient", "missing"}:
            quality_text = "，暂不参与当前排名"
        lines.append(
            f"- {card.get('name')}：PE {_format_number(card.get('current_pe'))}，"
            f"{card.get('history_label') or '历史'} {rank_text}，"
            f"较历史中位数 {_format_signed_pct(card.get('median_deviation_pct'))}，"
            f"数据 {card.get('data_date') or '--'}{quality_text}。"
        )

    ranking = result.get("ranking") or []
    if len(ranking) >= 2:
        highest, lowest = ranking[0], ranking[-1]
        lines.append(
            f"- 分位对比：{highest['name']}最高（{_format_percentile(highest['percentile'])}），"
            f"{lowest['name']}最低（{_format_percentile(lowest['percentile'])}）。"
        )
    elif ranking:
        only = ranking[0]
        lines.append(f"- 当前历史位置：{only['name']} {_format_percentile(only['percentile'])}。")

    if result.get("benchmark_note"):
        lines.append(f"- 基准说明：{result['benchmark_note']}。")
    gaps = [str(item) for item in result.get("gaps") or [] if str(item).strip()]
    if gaps:
        lines.append(f"- 数据提示：{'；'.join(gaps)}")
    lines.append("提示：分位只说明当前PE在自身历史中的位置；不同指数行业结构不同，不能用原始PE直接横向判定高低估，也不单独构成买卖建议。")
    return "\n".join(lines)


def build_global_index_valuation_context(
    query: str = "",
    as_of_date: str = "",
    engine: Any = None,
) -> dict[str, Any]:
    requested_date = _compact_date(as_of_date)
    if as_of_date and not requested_date:
        result = {
            "status": "invalid_request", "requested_date": "", "scope_label": "",
            "cards": [], "ranking": [], "gaps": ["日期格式无效，请使用 YYYYMMDD 或 YYYY-MM-DD。"],
        }
        result["report"] = _render_global_index_report(result)
        return result

    selected_codes, scope_label, subject_code, benchmark_note = _select_index_scope(query)
    try:
        active_engine = engine if engine is not None else _default_engine()
        dashboard = build_global_index_valuation_dashboard(
            active_engine,
            as_of_date=requested_date,
            window_years=10,
        )
    except Exception as exc:
        result = {
            "status": "error", "requested_date": requested_date, "scope_label": scope_label,
            "cards": [], "ranking": [], "gaps": [f"本地指数估值读取失败：{exc}"],
        }
        result["report"] = _render_global_index_report(result)
        return result

    selected_set = set(selected_codes)
    cards = [
        _reduced_index_card(card)
        for card in dashboard.get("cards") or []
        if str(card.get("code") or "") in selected_set
    ]
    usable = [
        card for card in cards
        if card.get("percentile") is not None
        and str(card.get("quality_status") or "") in RELIABLE_INDEX_QUALITY
    ]
    ranking = sorted(
        (
            {
                "code": card["code"], "name": card["name"], "market": card["market"],
                "percentile": card["percentile"], "label": card["percentile_label"],
                "data_date": card["data_date"],
            }
            for card in usable
        ),
        key=lambda item: float(item["percentile"]),
        reverse=True,
    )
    gaps: list[str] = []
    if not cards or all(card.get("current_pe") is None for card in cards):
        status = "no_data"
        gaps.append("所选范围没有本地可用估值数据。")
    elif not ranking:
        status = "insufficient"
        gaps.append("所选指数缺少足够且可用于当前比较的历史数据。")
    elif len(usable) < len(cards):
        status = "partial"
        gaps.append("部分指数数据陈旧、历史不足或质量待核对，已从当前分位排名中排除。")
    else:
        status = "ok"

    result = {
        "status": status,
        "requested_date": requested_date,
        "as_of_date": dashboard.get("as_of_date") or requested_date,
        "scope_label": scope_label,
        "subject_code": subject_code,
        "benchmark_codes": selected_codes if subject_code else [],
        "benchmark_note": benchmark_note,
        "cards": cards,
        "ranking": ranking,
        "highest": ranking[0] if ranking else None,
        "lowest": ranking[-1] if ranking else None,
        "quality_notes": list(dashboard.get("quality_notes") or []),
        "gaps": gaps,
    }
    result["report"] = _render_global_index_report(result)
    return result


def _prepare_month_end_history(frame: pd.DataFrame, latest_date: pd.Timestamp) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    data = frame.copy()
    data["date"] = pd.to_datetime(data["trade_date"], errors="coerce")
    for column in ("pe_ttm", "pb", "dv_ratio", "total_mv"):
        if column not in data.columns:
            data[column] = None
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data[data["date"].notna()].sort_values("date")
    data = data[data["date"] >= latest_date - pd.DateOffset(years=STOCK_HISTORY_YEARS)]
    if data.empty:
        return data
    data["month"] = data["date"].dt.to_period("M")
    return data.groupby("month", as_index=False).tail(1).drop(columns=["month"])


def _empty_stock_result(status: str, reason: str, *, symbol: str = "", as_of_date: str = "") -> dict[str, Any]:
    report = "【个股估值】\n结论：数据不足"
    if reason:
        report += f"\n- 原因：{reason}"
    return {
        "status": status,
        "symbol": symbol,
        "requested_date": as_of_date,
        "ts_code": "",
        "asset_type": "",
        "data_date": "",
        "metrics": {},
        "benchmark_codes": [],
        "benchmark_names": [],
        "benchmark_note": "",
        "gaps": [reason] if reason else [],
        "report": report,
    }


def _render_stock_report(result: dict[str, Any]) -> str:
    if result.get("status") not in {"ok", "partial", "loss_making"}:
        return str(result.get("report") or "【个股估值】\n结论：数据不足")
    metrics = result.get("metrics") or {}
    pe = metrics.get("pe_ttm")
    pb = metrics.get("pb")
    pe_percentile = metrics.get("pe_percentile")
    pb_percentile = metrics.get("pb_percentile")
    if pe is not None and pe > 0:
        pe_text = f"PE-TTM {_format_number(pe)}，近10年分位 {_format_percentile(pe_percentile)}（{metrics.get('pe_label') or '样本不足'}）"
    else:
        pe_text = "PE-TTM 无效（当前盈利为负或口径不可用），不计算PE分位"
    pb_text = f"PB {_format_number(pb)}"
    if pb_percentile is not None:
        pb_text += f"，近10年分位 {_format_percentile(pb_percentile)}（{metrics.get('pb_label')}）"
    elif pb is not None:
        pb_text += "，历史分位样本不足"
    lines = [
        "【个股估值】",
        f"结论：{result.get('symbol')}（{result.get('ts_code')}）当前估值处于{metrics.get('pe_label') or metrics.get('pb_label') or '待确认'}位置。",
        f"- {pe_text}。",
        f"- {pb_text}；股息率 {_format_number(metrics.get('dividend_yield_pct'))}%。",
        f"- 总市值 {_format_number(metrics.get('total_mv_yi'))}亿元；数据日期 {result.get('data_date') or '--'}。",
    ]
    if result.get("benchmark_names"):
        lines.append(
            f"- 风格参考：{'、'.join(result['benchmark_names'])}；{result.get('benchmark_note')}。"
        )
    gaps = [str(item) for item in result.get("gaps") or [] if str(item).strip()]
    if gaps:
        lines.append(f"- 数据提示：{'；'.join(gaps)}")
    lines.append("提示：历史分位衡量的是估值位置，不等于内在价值；长期判断还需核对盈利、现金流、竞争力与风险。")
    return "\n".join(lines)


def build_stock_valuation_profile(
    symbol: str,
    as_of_date: str = "",
    engine: Any = None,
) -> dict[str, Any]:
    requested_date = _compact_date(as_of_date)
    if as_of_date and not requested_date:
        return _empty_stock_result(
            "invalid_request", "日期格式无效，请使用 YYYYMMDD 或 YYYY-MM-DD。",
            symbol=symbol,
        )
    if not str(symbol or "").strip():
        return _empty_stock_result("invalid_request", "请提供股票或指数名称。")

    matched_indices = match_index_codes(symbol)
    if matched_indices:
        result = _empty_stock_result("index_subject", "该标的是指数，请使用指数估值口径。", symbol=symbol)
        result["index_codes"] = matched_indices
        return result

    ts_code, asset_type = _resolve_security(symbol)
    if not ts_code:
        return _empty_stock_result("not_found", f"未找到 {symbol}，请确认名称或代码。", symbol=symbol)
    if asset_type not in {"stock", "index"}:
        return _empty_stock_result("unsupported", f"{symbol} 不属于支持估值的股票或指数。", symbol=symbol)
    if asset_type == "stock" and ts_code.endswith((".US", ".HK")):
        market = "美股" if ts_code.endswith(".US") else "港股"
        result = _empty_stock_result(
            "unsupported_market",
            f"当前个股历史估值仅覆盖A股；{market}可提供指数估值环境，但暂无该个股的本地历史分位。",
            symbol=symbol,
        )
        result.update(ts_code=ts_code, asset_type=asset_type)
        return result

    table_name = "stock_valuation" if asset_type == "stock" else "index_valuation"
    dividend_sql = "dv_ratio" if asset_type == "stock" else "NULL AS dv_ratio"
    query = f"""
        SELECT trade_date, pe_ttm, pb, {dividend_sql}, total_mv
        FROM {table_name}
        WHERE ts_code = :ts_code
    """
    params: dict[str, Any] = {"ts_code": ts_code}
    if requested_date:
        query += " AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') <= :as_of_date"
        params["as_of_date"] = requested_date
    query += " ORDER BY trade_date DESC LIMIT :history_limit"
    params["history_limit"] = STOCK_HISTORY_LIMIT
    try:
        active_engine = engine if engine is not None else _default_engine()
        frame = pd.read_sql(text(query), active_engine, params=params)
    except Exception as exc:
        return _empty_stock_result(
            "error", f"本地估值数据读取失败：{exc}", symbol=symbol, as_of_date=requested_date,
        )
    if frame.empty:
        return _empty_stock_result(
            "no_data", f"暂无 {symbol}（{ts_code}）的本地估值数据。",
            symbol=symbol, as_of_date=requested_date,
        )

    prepared = frame.copy()
    prepared["date"] = pd.to_datetime(prepared["trade_date"], errors="coerce")
    prepared = prepared[prepared["date"].notna()].sort_values("date")
    if prepared.empty:
        return _empty_stock_result("no_data", "估值日期字段无有效记录。", symbol=symbol)
    latest = prepared.iloc[-1]
    latest_date = pd.Timestamp(latest["date"])
    monthly = _prepare_month_end_history(prepared, latest_date)
    current_pe = _number(latest.get("pe_ttm"))
    current_pb = _number(latest.get("pb"))
    pe_percentile = empirical_percentile(monthly.get("pe_ttm", pd.Series(dtype=float)), current_pe or 0)
    pb_percentile = empirical_percentile(monthly.get("pb", pd.Series(dtype=float)), current_pb or 0)
    benchmark_codes, benchmark_note = benchmark_codes_for_security(ts_code)
    gaps: list[str] = []
    if current_pe is None or current_pe <= 0:
        status = "loss_making"
        pe_label = "亏损/无效"
    else:
        status = "ok"
        pe_label = percentile_label(pe_percentile)
        if pe_percentile is None:
            status = "partial"
            gaps.append("PE有效月度历史不足，暂不输出分位结论。")
    if current_pb is None or current_pb <= 0:
        status = "partial" if status == "ok" else status
        gaps.append("当前PB缺失或无效，未输出PB历史分位。")
    elif pb_percentile is None:
        status = "partial" if status == "ok" else status
        gaps.append("PB有效月度历史不足，暂不输出分位结论。")
    if requested_date and latest_date.strftime("%Y%m%d") < requested_date:
        gaps.append(
            f"请求日期没有同日记录，已回退到 {latest_date.strftime('%Y-%m-%d')}。"
        )
    total_mv = _number(latest.get("total_mv"))
    result = {
        "status": status,
        "symbol": str(symbol).strip(),
        "requested_date": requested_date,
        "ts_code": ts_code,
        "asset_type": asset_type,
        "data_date": latest_date.strftime("%Y-%m-%d"),
        "metrics": {
            "pe_ttm": current_pe,
            "pe_percentile": pe_percentile,
            "pe_label": pe_label,
            "pb": current_pb,
            "pb_percentile": pb_percentile,
            "pb_label": percentile_label(pb_percentile),
            "dividend_yield_pct": _number(latest.get("dv_ratio")),
            "total_mv_yi": (total_mv / 10000.0) if total_mv is not None else None,
            "pe_month_count": int(pd.to_numeric(monthly.get("pe_ttm"), errors="coerce").gt(0).sum()),
            "pb_month_count": int(pd.to_numeric(monthly.get("pb"), errors="coerce").gt(0).sum()),
        },
        "benchmark_codes": benchmark_codes,
        "benchmark_names": [INDEX_NAME_BY_CODE[code] for code in benchmark_codes],
        "benchmark_note": benchmark_note,
        "gaps": gaps,
    }
    result["report"] = _render_stock_report(result)
    return result


@tool(args_schema=GlobalIndexValuationInput)
def get_global_index_valuation(query: str = "", as_of_date: str = "") -> str:
    """Query local PE and historical percentiles for 11 major global equity indices.

    Use it for index, stock-market or ETF valuation context, PE percentile rankings,
    and market background for value-investing questions.  It reads the local cache
    only and does not provide company fundamentals or an independent buy/sell signal.
    """
    return build_global_index_valuation_context(query=query, as_of_date=as_of_date).get("report", "")


def render_stock_valuation_report(result: dict[str, Any]) -> str:
    """Public renderer kept separate so the legacy LangChain tool stays compatible."""
    return _render_stock_report(result)
