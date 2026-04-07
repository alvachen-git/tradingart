from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import text

from symbol_match import sql_prefix_condition, strict_futures_prefix_pattern


WINDOW_TO_DAYS: Dict[str, int] = {
    "3d": 3,
    "1w": 5,
    "2w": 10,
    "1m": 22,
}

WINDOW_LABELS: Dict[str, str] = {
    "3d": "3交易日",
    "1w": "1周",
    "2w": "2周",
    "1m": "1月",
}

ANCHOR_LABEL_START = "窗口起点"
ANCHOR_LABEL_MID = "窗口中点"
ANCHOR_LABEL_LATEST = "最新"


def normalize_contract_month(raw_month: str) -> Optional[str]:
    """Convert 3/4-digit month suffix into comparable YYMM text."""
    if raw_month is None:
        return None
    m = str(raw_month).strip()
    if not m.isdigit():
        return None
    if len(m) == 4:
        return m
    if len(m) == 3:
        return f"2{m}"
    return None


def extract_contract_meta(ts_code: str) -> Optional[Dict[str, str]]:
    """
    Parse futures contract code.
    Supported examples: RB2605, MA605, CU2606.SHF.
    """
    if not isinstance(ts_code, str):
        return None
    base = ts_code.strip().upper().split(".")[0]
    if "TAS" in base:
        return None
    m = re.match(r"^([A-Z]{1,4})(\d{3,4})$", base)
    if not m:
        return None
    product = m.group(1)
    raw_month = m.group(2)
    month = normalize_contract_month(raw_month)
    if not month:
        return None
    return {
        "product_code": product,
        "raw_month": raw_month,
        "month": month,
        "contract_code": base,
    }


def _normalize_product_code(product_code: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(product_code or "").upper())


def _to_compact_date(value: Any) -> str:
    if value is None:
        return ""
    try:
        return pd.to_datetime(value).strftime("%Y%m%d")
    except Exception:
        return str(value).replace("-", "").replace("/", "").split(" ")[0]


def _format_display_date(value: str) -> str:
    if not value:
        return ""
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def _anchor_dates(dates_desc: Sequence[str]) -> List[Tuple[str, str]]:
    if not dates_desc:
        return []
    n = len(dates_desc)
    latest = dates_desc[0]
    mid = dates_desc[(n - 1) // 2]
    start = dates_desc[n - 1]
    return [
        (ANCHOR_LABEL_START, start),
        (ANCHOR_LABEL_MID, mid),
        (ANCHOR_LABEL_LATEST, latest),
    ]


def _build_in_clause(values: Sequence[Any], key_prefix: str) -> Tuple[str, Dict[str, Any]]:
    params: Dict[str, Any] = {}
    placeholders: List[str] = []
    for i, value in enumerate(values):
        k = f"{key_prefix}{i}"
        placeholders.append(f":{k}")
        params[k] = value
    return ",".join(placeholders), params


def _strict_match_product(ts_code: str, product_code: str) -> bool:
    code = str(ts_code or "").upper().split(".")[0]
    clean = _normalize_product_code(product_code)
    if not clean:
        return False
    if len(clean) == 1:
        pattern = strict_futures_prefix_pattern(clean)
        return bool(re.match(pattern, code))
    return code.startswith(clean)


def _read_dates(engine: Any, product_code: str, window_size: int) -> pd.DataFrame:
    prefix_clause = sql_prefix_condition(product_code)
    sql = text(
        f"""
        SELECT DISTINCT trade_date
        FROM futures_price
        WHERE {prefix_clause}
          AND UPPER(ts_code) NOT LIKE '%%TAS%%'
        ORDER BY trade_date DESC
        LIMIT :limit_n
        """
    )
    try:
        return pd.read_sql(sql, engine, params={"limit_n": int(window_size)})
    except Exception:
        fallback = text(
            """
            SELECT DISTINCT trade_date
            FROM futures_price
            WHERE UPPER(ts_code) LIKE :prefix
              AND UPPER(ts_code) NOT LIKE '%TAS%'
            ORDER BY trade_date DESC
            LIMIT :limit_n
            """
        )
        df = pd.read_sql(
            fallback,
            engine,
            params={"prefix": f"{product_code}%", "limit_n": int(window_size)},
        )
        return df


def _read_rows_by_dates(engine: Any, product_code: str, dates: Sequence[Any]) -> pd.DataFrame:
    in_clause, in_params = _build_in_clause(dates, "d")
    prefix_clause = sql_prefix_condition(product_code)
    sql = text(
        f"""
        SELECT trade_date, ts_code, close_price, oi
        FROM futures_price
        WHERE trade_date IN ({in_clause})
          AND {prefix_clause}
          AND UPPER(ts_code) NOT LIKE '%%TAS%%'
        """
    )
    try:
        return pd.read_sql(sql, engine, params=in_params)
    except Exception:
        fallback = text(
            f"""
            SELECT trade_date, ts_code, close_price, oi
            FROM futures_price
            WHERE trade_date IN ({in_clause})
              AND UPPER(ts_code) LIKE :prefix
              AND UPPER(ts_code) NOT LIKE '%TAS%'
            """
        )
        params = dict(in_params)
        params["prefix"] = f"{product_code}%"
        return pd.read_sql(fallback, engine, params=params)


def _pick_contract_slots(latest_rows: pd.DataFrame, product_code: str, contract_slots: int) -> List[str]:
    contracts: List[Tuple[int, str]] = []
    seen = set()
    for _, row in latest_rows.iterrows():
        ts_code = str(row.get("ts_code") or "")
        if not _strict_match_product(ts_code, product_code):
            continue
        meta = extract_contract_meta(ts_code)
        if not meta or meta["product_code"] != product_code:
            continue
        month = meta["month"]
        if month in seen:
            continue
        seen.add(month)
        contracts.append((int(month), month))
    contracts.sort(key=lambda x: x[0])
    return [m for _, m in contracts[: int(contract_slots)]]


def _compute_min_valid_month(latest_trade_date: str) -> Optional[int]:
    """
    Keep contracts newer than (latest_trade_date - 1 month) in YYMM space.
    This filters stale historical contracts that occasionally remain in snapshots.
    """
    if not latest_trade_date:
        return None
    try:
        dt = pd.to_datetime(latest_trade_date, format="%Y%m%d", errors="coerce")
        if pd.isna(dt):
            dt = pd.to_datetime(latest_trade_date, errors="coerce")
        if pd.isna(dt):
            return None
        threshold = (dt - pd.DateOffset(months=1)).strftime("%y%m")
        return int(threshold)
    except Exception:
        return None


def _build_date_contract_map(rows: pd.DataFrame, product_code: str) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
    out: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
    if rows is None or rows.empty:
        return out
    work = rows.copy()
    work["trade_key"] = work["trade_date"].apply(_to_compact_date)
    work["oi"] = pd.to_numeric(work["oi"], errors="coerce").fillna(0.0)
    work = work.sort_values("oi", ascending=False)

    for _, row in work.iterrows():
        ts_code = str(row.get("ts_code") or "")
        if not _strict_match_product(ts_code, product_code):
            continue
        meta = extract_contract_meta(ts_code)
        if not meta or meta["product_code"] != product_code:
            continue
        date_key = str(row.get("trade_key") or "")
        if not date_key:
            continue
        month = meta["month"]
        close_px = pd.to_numeric(row.get("close_price"), errors="coerce")
        oi = pd.to_numeric(row.get("oi"), errors="coerce")
        out.setdefault(date_key, {})
        if month not in out[date_key]:
            out[date_key][month] = {
                "close_price": None if pd.isna(close_px) else float(close_px),
                "oi": None if pd.isna(oi) else float(oi),
            }
    return out


def _build_summary(latest_series_points: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    valid = [p for p in latest_series_points if p.get("close_price") is not None]
    if len(valid) < 2:
        return {
            "structure_type": "InsufficientData",
            "front_contract": None,
            "far_contract": None,
            "spread_abs": None,
            "spread_pct": None,
            "slope_per_step": None,
        }

    front = valid[0]
    far = valid[-1]
    front_px = float(front["close_price"])
    far_px = float(far["close_price"])
    spread_abs = far_px - front_px
    spread_pct = (spread_abs / front_px) if front_px else None
    step = len(valid) - 1
    slope = spread_abs / step if step > 0 else None
    if spread_abs > 0:
        structure_type = "Contango"
    elif spread_abs < 0:
        structure_type = "Backwardation"
    else:
        structure_type = "Flat"
    return {
        "structure_type": structure_type,
        "front_contract": front.get("contract"),
        "far_contract": far.get("contract"),
        "spread_abs": spread_abs,
        "spread_pct": spread_pct,
        "slope_per_step": slope,
    }


def _filter_contracts_by_coverage(
    selected_contracts: Sequence[str],
    date_map: Dict[str, Dict[str, Dict[str, Optional[float]]]],
    anchors: Sequence[Tuple[str, str]],
    min_points: int = 2,
) -> List[str]:
    """
    Keep contracts that have enough observed points across anchor dates.
    Example: min_points=2 means single-point contracts are dropped.
    """
    if not selected_contracts:
        return []
    keep: List[str] = []
    for month in selected_contracts:
        observed = 0
        for _, date_key in anchors:
            day_contract_map = date_map.get(date_key, {})
            close_px = day_contract_map.get(month, {}).get("close_price")
            if close_px is not None:
                observed += 1
        if observed >= int(min_points):
            keep.append(month)
    return keep


def build_term_structure_payload(
    engine: Any,
    product_code: str,
    window_key: str,
    contract_slots: int = 7,
) -> Dict[str, Any]:
    """
    Build term structure payload:
    anchors/contracts/series/summary/meta.
    """
    product = _normalize_product_code(product_code)
    if not product:
        return {
            "anchors": [],
            "contracts": [],
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                "product_code": product,
                "window_key": window_key,
                "latest_trade_date": None,
                "window_label": WINDOW_LABELS.get(window_key, window_key),
            },
            "error": "invalid_product_code",
        }
    if window_key not in WINDOW_TO_DAYS:
        return {
            "anchors": [],
            "contracts": [],
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                "product_code": product,
                "window_key": window_key,
                "latest_trade_date": None,
                "window_label": WINDOW_LABELS.get(window_key, window_key),
            },
            "error": "invalid_window_key",
        }
    if engine is None:
        return {
            "anchors": [],
            "contracts": [],
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                "product_code": product,
                "window_key": window_key,
                "latest_trade_date": None,
                "window_label": WINDOW_LABELS.get(window_key, window_key),
            },
            "error": "engine_unavailable",
        }

    window_size = WINDOW_TO_DAYS[window_key]
    dates_df = _read_dates(engine=engine, product_code=product, window_size=window_size)
    if dates_df is None or dates_df.empty:
        return {
            "anchors": [],
            "contracts": [],
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                "product_code": product,
                "window_key": window_key,
                "latest_trade_date": None,
                "window_label": WINDOW_LABELS.get(window_key, window_key),
            },
            "error": "no_trade_dates",
        }

    dates_desc = [_to_compact_date(x) for x in dates_df["trade_date"].tolist()]
    raw_dates = dates_df["trade_date"].tolist()
    anchors = _anchor_dates(dates_desc)
    anchor_dates = [d for _, d in anchors]
    latest_trade_date = dates_desc[0] if dates_desc else None

    rows_df = _read_rows_by_dates(engine=engine, product_code=product, dates=raw_dates)
    if rows_df is None or rows_df.empty:
        return {
            "anchors": [
                {"label": label, "trade_date": date, "display_date": _format_display_date(date)}
                for label, date in anchors
            ],
            "contracts": [],
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                "product_code": product,
                "window_key": window_key,
                "latest_trade_date": latest_trade_date,
                "window_label": WINDOW_LABELS.get(window_key, window_key),
            },
            "error": "no_contract_rows",
        }

    work = rows_df.copy()
    work["trade_key"] = work["trade_date"].apply(_to_compact_date)
    latest_rows = work[work["trade_key"] == latest_trade_date].copy()
    selected_contracts = _pick_contract_slots(
        latest_rows=latest_rows,
        product_code=product,
        contract_slots=max(int(contract_slots), 1) * 2,
    )
    min_valid_month = _compute_min_valid_month(latest_trade_date)
    if min_valid_month is not None:
        selected_contracts = [m for m in selected_contracts if int(m) >= min_valid_month]
    selected_contracts = selected_contracts[: int(contract_slots)]
    if not selected_contracts:
        return {
            "anchors": [
                {"label": label, "trade_date": date, "display_date": _format_display_date(date)}
                for label, date in anchors
            ],
            "contracts": [],
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                "product_code": product,
                "window_key": window_key,
                "latest_trade_date": latest_trade_date,
                "window_label": WINDOW_LABELS.get(window_key, window_key),
            },
            "error": "no_valid_contracts",
        }

    date_map = _build_date_contract_map(rows=work, product_code=product)
    selected_contracts = _filter_contracts_by_coverage(
        selected_contracts=selected_contracts,
        date_map=date_map,
        anchors=anchors,
        min_points=2,
    )
    if not selected_contracts:
        return {
            "anchors": [
                {"label": label, "trade_date": date, "display_date": _format_display_date(date)}
                for label, date in anchors
            ],
            "contracts": [],
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                "product_code": product,
                "window_key": window_key,
                "latest_trade_date": latest_trade_date,
                "window_label": WINDOW_LABELS.get(window_key, window_key),
            },
            "error": "no_contracts_after_coverage_filter",
        }

    series: List[Dict[str, Any]] = []
    for label, date_key in anchors:
        points: List[Dict[str, Any]] = []
        day_contract_map = date_map.get(date_key, {})
        for month in selected_contracts:
            value = day_contract_map.get(month, {})
            points.append(
                {
                    "contract": month,
                    "close_price": value.get("close_price"),
                    "oi": value.get("oi"),
                }
            )
        series.append(
            {
                "label": label,
                "trade_date": date_key,
                "display_date": _format_display_date(date_key),
                "points": points,
            }
        )

    latest_series = next((s for s in series if s["label"] == ANCHOR_LABEL_LATEST), None)
    latest_points = latest_series["points"] if latest_series else []
    summary = _build_summary(latest_points)

    return {
        "anchors": [
            {"label": label, "trade_date": date, "display_date": _format_display_date(date)}
            for label, date in anchors
        ],
        "contracts": selected_contracts,
        "series": series,
        "summary": summary,
        "meta": {
            "product_code": product,
            "window_key": window_key,
            "latest_trade_date": latest_trade_date,
            "window_label": WINDOW_LABELS.get(window_key, window_key),
        },
    }
