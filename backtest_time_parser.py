from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import datetime, timedelta


DATE_FMT = "%Y%m%d"


@dataclass
class ResolvedWindow:
    start_date: str
    end_date: str
    requested_time_expr: str | None = None
    note: str = ""


def _to_yyyymmdd(value: str | None) -> str | None:
    if value is None:
        return None
    raw = re.sub(r"[^0-9]", "", str(value).strip())
    if len(raw) != 8:
        return None
    try:
        datetime.strptime(raw, DATE_FMT)
    except Exception:
        return None
    return raw


def _add_months(dt: datetime, months: int) -> datetime:
    month0 = dt.month - 1 + months
    year = dt.year + month0 // 12
    month = month0 % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(dt.day, last_day)
    return dt.replace(year=year, month=month, day=day)


def _anchor_dt(anchor_date: str | None) -> datetime:
    if anchor_date:
        parsed = _to_yyyymmdd(anchor_date)
        if parsed:
            return datetime.strptime(parsed, DATE_FMT)
    return datetime.now()


def parse_time_expr(time_expr: str, anchor_date: str | None = None) -> ResolvedWindow | None:
    if not time_expr:
        return None
    expr_raw = str(time_expr).strip()
    if not expr_raw:
        return None

    anchor = _anchor_dt(anchor_date)
    expr = expr_raw.lower().replace(" ", "")

    if expr in {"ytd", "今年以来", "年初至今"}:
        start = datetime(anchor.year, 1, 1)
        return ResolvedWindow(start.strftime(DATE_FMT), anchor.strftime(DATE_FMT), expr_raw, "按今年以来解析")

    if expr in {"今年", "本年"}:
        start = datetime(anchor.year, 1, 1)
        return ResolvedWindow(start.strftime(DATE_FMT), anchor.strftime(DATE_FMT), expr_raw, "按今年解析")

    if expr in {"去年", "去年全年", "上年", "上一年"}:
        y = anchor.year - 1
        return ResolvedWindow(f"{y}0101", f"{y}1231", expr_raw, "按去年全年解析")

    q_match = re.match(r"^(\d{4})(?:年)?[qQＱ]([1-4])$", expr_raw.replace(" ", ""))
    if not q_match:
        q_match = re.match(r"^(\d{4})年?第?([1-4])季(?:度)?$", expr_raw.replace(" ", ""))
    if q_match:
        year = int(q_match.group(1))
        q = int(q_match.group(2))
        start_month = (q - 1) * 3 + 1
        end_month = start_month + 2
        start = datetime(year, start_month, 1)
        end_day = calendar.monthrange(year, end_month)[1]
        end = datetime(year, end_month, end_day)
        return ResolvedWindow(start.strftime(DATE_FMT), end.strftime(DATE_FMT), expr_raw, f"按 {year}Q{q} 解析")

    m_match = re.match(r"^(\d{4})[-/年](\d{1,2})月?$", expr_raw)
    if m_match:
        year = int(m_match.group(1))
        month = int(m_match.group(2))
        if 1 <= month <= 12:
            start = datetime(year, month, 1)
            end_day = calendar.monthrange(year, month)[1]
            end = datetime(year, month, end_day)
            return ResolvedWindow(start.strftime(DATE_FMT), end.strftime(DATE_FMT), expr_raw, f"按 {year}-{month:02d} 解析")

    rel_match = re.match(r"^(近|最近|过去)(\d+)(天|日|周|个月|月|年)$", expr)
    if not rel_match and expr in {"近一年", "最近一年", "过去一年"}:
        rel_match = ("近", "1", "年")
    if not rel_match and expr in {"近一月", "最近一月", "过去一月"}:
        rel_match = ("近", "1", "月")
    if rel_match:
        if isinstance(rel_match, tuple):
            n = int(rel_match[1])
            unit = rel_match[2]
        else:
            n = int(rel_match.group(2))
            unit = rel_match.group(3)

        if n <= 0:
            return None
        end = anchor
        if unit in {"天", "日"}:
            start = end - timedelta(days=n - 1)
        elif unit == "周":
            start = end - timedelta(days=n * 7 - 1)
        elif unit in {"月", "个月"}:
            start = _add_months(end, -n) + timedelta(days=1)
        else:
            start = _add_months(end, -12 * n) + timedelta(days=1)
        return ResolvedWindow(start.strftime(DATE_FMT), end.strftime(DATE_FMT), expr_raw, f"按{expr_raw}解析")

    return None


def resolve_window(
    start_date: str | None = None,
    end_date: str | None = None,
    time_expr: str | None = None,
    lookback_days: int | None = None,
    anchor_date: str | None = None,
) -> ResolvedWindow:
    anchor = _anchor_dt(anchor_date).strftime(DATE_FMT)
    notes: list[str] = []

    if start_date or end_date:
        s = _to_yyyymmdd(start_date)
        e = _to_yyyymmdd(end_date)
        if start_date and not s:
            raise ValueError(f"start_date 格式错误: {start_date}")
        if end_date and not e:
            raise ValueError(f"end_date 格式错误: {end_date}")
        if e is None:
            e = anchor
            notes.append(f"未提供结束日，使用锚点日 {e}")
        if s is None:
            e_dt = datetime.strptime(e, DATE_FMT)
            s = (e_dt - timedelta(days=365)).strftime(DATE_FMT)
            notes.append(f"未提供开始日，默认回溯365天至 {s}")
        if s > e:
            raise ValueError("开始日期不能晚于结束日期")
        return ResolvedWindow(s, e, time_expr or None, "；".join(notes))

    if time_expr:
        parsed = parse_time_expr(time_expr, anchor_date=anchor)
        if parsed is None:
            raise ValueError(f"无法解析 time_expr: {time_expr}")
        return parsed

    if lookback_days is not None:
        n = int(lookback_days)
        if n <= 0:
            raise ValueError("lookback_days 必须大于0")
        e_dt = datetime.strptime(anchor, DATE_FMT)
        s = (e_dt - timedelta(days=n)).strftime(DATE_FMT)
        return ResolvedWindow(s, anchor, None, f"按 lookback_days={n} 回溯")

    e_dt = datetime.strptime(anchor, DATE_FMT)
    s = (e_dt - timedelta(days=365)).strftime(DATE_FMT)
    return ResolvedWindow(s, anchor, None, "默认回溯365天")
