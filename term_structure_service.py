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


STOCK_INDEX_SPOT_MAP: Dict[str, str] = {
    "IF": "000300.SH",
    "IH": "000016.SH",
    "IC": "000905.SH",
    "IM": "000852.SH",
}


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


def _is_stock_index_future(product_code: str) -> bool:
    return _normalize_product_code(product_code) in STOCK_INDEX_SPOT_MAP


def _safe_float(value: Any) -> Optional[float]:
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num):
        return None
    return float(num)


def _next_month_yymm(yymm: int) -> int:
    yy = int(yymm) // 100
    mm = int(yymm) % 100
    if mm >= 12:
        yy = (yy + 1) % 100
        mm = 1
    else:
        mm += 1
    return yy * 100 + mm


def _target_near_month(trade_date_key: str) -> Optional[int]:
    if not trade_date_key:
        return None
    dt = pd.to_datetime(trade_date_key, format="%Y%m%d", errors="coerce")
    if pd.isna(dt):
        dt = pd.to_datetime(trade_date_key, errors="coerce")
    if pd.isna(dt):
        return None
    yymm = (dt.year % 100) * 100 + dt.month
    if dt.day >= 15:
        yymm = _next_month_yymm(yymm)
    return int(yymm)


def _pick_nearest_month(available_months: Sequence[str], target_yymm: int) -> Optional[str]:
    months_int = []
    for month in available_months:
        s = str(month or "").strip()
        if s.isdigit():
            months_int.append(int(s))
    if not months_int:
        return None
    best = min(
        months_int,
        key=lambda x: (abs(x - int(target_yymm)), 0 if x >= int(target_yymm) else 1, x),
    )
    return f"{best:04d}"


def _read_index_close_by_compact_dates(
    engine: Any, index_code: str, dates_compact: Sequence[str]
) -> pd.DataFrame:
    if not dates_compact:
        return pd.DataFrame(columns=["trade_date", "close_price"])
    in_clause, in_params = _build_in_clause(dates_compact, "idxd")
    sql = text(
        f"""
        SELECT trade_date, close_price
        FROM index_price
        WHERE ts_code = :index_code
          AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') IN ({in_clause})
        """
    )
    params = {"index_code": index_code, **in_params}
    return pd.read_sql(sql, engine, params=params)


def _build_basis_summary(latest_series_points: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    pseudo_points = []
    for p in latest_series_points:
        pseudo_points.append(
            {
                "contract": p.get("contract"),
                "close_price": p.get("basis"),
            }
        )
    return _build_summary(pseudo_points)


def build_index_basis_term_structure_payload(
    engine: Any,
    product_code: str,
    window_key: str,
    contract_slots: int = 7,
) -> Dict[str, Any]:
    product = _normalize_product_code(product_code)
    if not _is_stock_index_future(product):
        return {
            "anchors": [],
            "contracts": [],
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                "product_code": product,
                "window_key": window_key,
                "spot_index_code": None,
                "basis_formula": "futures_minus_spot",
            },
            "error": "non_stock_index_future",
        }

    payload = build_term_structure_payload(
        engine=engine,
        product_code=product,
        window_key=window_key,
        contract_slots=contract_slots,
    )
    if payload.get("error"):
        payload["meta"] = {
            **(payload.get("meta") or {}),
            "spot_index_code": STOCK_INDEX_SPOT_MAP.get(product),
            "basis_formula": "futures_minus_spot",
        }
        return payload

    anchors = payload.get("anchors") or []
    contracts = payload.get("contracts") or []
    src_series = payload.get("series") or []
    index_code = STOCK_INDEX_SPOT_MAP.get(product)
    anchor_dates = [str(x.get("trade_date") or "") for x in anchors]
    spot_df = _read_index_close_by_compact_dates(engine, index_code=index_code, dates_compact=anchor_dates)
    if spot_df is None or spot_df.empty:
        return {
            "anchors": anchors,
            "contracts": contracts,
            "series": [],
            "summary": _build_summary([]),
            "meta": {
                **(payload.get("meta") or {}),
                "spot_index_code": index_code,
                "basis_formula": "futures_minus_spot",
            },
            "error": "no_spot_rows",
        }

    spot_map: Dict[str, float] = {}
    for _, row in spot_df.iterrows():
        key = _to_compact_date(row.get("trade_date"))
        close_px = _safe_float(row.get("close_price"))
        if key and close_px is not None and key not in spot_map:
            spot_map[key] = close_px

    series: List[Dict[str, Any]] = []
    for s in src_series:
        date_key = str(s.get("trade_date") or "")
        spot_close = spot_map.get(date_key)
        points = []
        for p in s.get("points", []):
            futures_close = _safe_float(p.get("close_price"))
            basis = None
            if futures_close is not None and spot_close is not None:
                basis = futures_close - float(spot_close)
            points.append(
                {
                    "contract": p.get("contract"),
                    "close_price": futures_close,
                    "futures_close": futures_close,
                    "spot_close": spot_close,
                    "basis": basis,
                    "oi": p.get("oi"),
                }
            )
        series.append(
            {
                "label": s.get("label"),
                "trade_date": date_key,
                "display_date": s.get("display_date"),
                "points": points,
            }
        )

    latest_series = next((x for x in series if x.get("label") == ANCHOR_LABEL_LATEST), None)
    summary = _build_basis_summary(latest_series.get("points", []) if latest_series else [])
    return {
        "anchors": anchors,
        "contracts": contracts,
        "series": series,
        "summary": summary,
        "meta": {
            **(payload.get("meta") or {}),
            "spot_index_code": index_code,
            "basis_formula": "futures_minus_spot",
        },
    }


def build_index_basis_longterm_payload(
    engine: Any,
    product_code: str,
    lookback_years: int = 1,
) -> Dict[str, Any]:
    product = _normalize_product_code(product_code)
    years = max(int(lookback_years), 1)
    if not _is_stock_index_future(product):
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": None,
                "lookback_years": years,
                "basis_formula": "futures_minus_spot",
            },
            "error": "non_stock_index_future",
        }
    if engine is None:
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": STOCK_INDEX_SPOT_MAP.get(product),
                "lookback_years": years,
                "basis_formula": "futures_minus_spot",
            },
            "error": "engine_unavailable",
        }

    dates_df = _read_dates(engine=engine, product_code=product, window_size=420)
    if dates_df is None or dates_df.empty:
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": STOCK_INDEX_SPOT_MAP.get(product),
                "lookback_years": years,
                "basis_formula": "futures_minus_spot",
            },
            "error": "no_trade_dates",
        }

    compact_raw_dates: List[Tuple[str, Any]] = []
    for raw in dates_df["trade_date"].tolist():
        key = _to_compact_date(raw)
        if key:
            compact_raw_dates.append((key, raw))
    if not compact_raw_dates:
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": STOCK_INDEX_SPOT_MAP.get(product),
                "lookback_years": years,
                "basis_formula": "futures_minus_spot",
            },
            "error": "no_trade_dates",
        }

    latest_trade_date = compact_raw_dates[0][0]
    latest_dt = pd.to_datetime(latest_trade_date, format="%Y%m%d", errors="coerce")
    if pd.isna(latest_dt):
        latest_dt = pd.to_datetime(latest_trade_date, errors="coerce")
    if pd.isna(latest_dt):
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": STOCK_INDEX_SPOT_MAP.get(product),
                "lookback_years": years,
                "basis_formula": "futures_minus_spot",
            },
            "error": "invalid_latest_trade_date",
        }

    threshold_key = (latest_dt - pd.DateOffset(years=years)).strftime("%Y%m%d")
    selected_pairs = [x for x in compact_raw_dates if x[0] >= threshold_key]
    if not selected_pairs:
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": STOCK_INDEX_SPOT_MAP.get(product),
                "lookback_years": years,
                "basis_formula": "futures_minus_spot",
            },
            "error": "no_trade_dates_in_window",
        }

    selected_compact_dates = [x[0] for x in selected_pairs]
    selected_raw_dates = [x[1] for x in selected_pairs]
    rows_df = _read_rows_by_dates(engine=engine, product_code=product, dates=selected_raw_dates)
    if rows_df is None or rows_df.empty:
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": STOCK_INDEX_SPOT_MAP.get(product),
                "lookback_years": years,
                "basis_formula": "futures_minus_spot",
            },
            "error": "no_contract_rows",
        }

    date_map = _build_date_contract_map(rows=rows_df, product_code=product)
    dates_asc = sorted(set(selected_compact_dates))
    index_code = STOCK_INDEX_SPOT_MAP.get(product)
    spot_df = _read_index_close_by_compact_dates(engine=engine, index_code=index_code, dates_compact=dates_asc)
    if spot_df is None or spot_df.empty:
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": index_code,
                "lookback_years": years,
                "latest_trade_date": latest_trade_date,
                "basis_formula": "futures_minus_spot",
            },
            "error": "no_spot_rows",
        }

    spot_map: Dict[str, float] = {}
    for _, row in spot_df.iterrows():
        key = _to_compact_date(row.get("trade_date"))
        close_px = _safe_float(row.get("close_price"))
        if key and close_px is not None and key not in spot_map:
            spot_map[key] = close_px

    points: List[Dict[str, Any]] = []
    for date_key in dates_asc:
        day_contract_map = date_map.get(date_key, {})
        if not day_contract_map:
            continue
        target_month = _target_near_month(date_key)
        if target_month is None:
            continue
        selected_month = _pick_nearest_month(
            available_months=list(day_contract_map.keys()),
            target_yymm=target_month,
        )
        if not selected_month:
            continue
        day_value = day_contract_map.get(selected_month, {})
        futures_close = _safe_float(day_value.get("close_price"))
        spot_close = spot_map.get(date_key)
        basis = None
        if futures_close is not None and spot_close is not None:
            basis = futures_close - float(spot_close)
        points.append(
            {
                "trade_date": date_key,
                "display_date": _format_display_date(date_key),
                "contract": selected_month,
                "target_month": f"{int(target_month):04d}",
                "futures_close": futures_close,
                "spot_close": spot_close,
                "basis": basis,
                "oi": day_value.get("oi"),
            }
        )

    if not points:
        return {
            "points": [],
            "meta": {
                "product_code": product,
                "spot_index_code": index_code,
                "lookback_years": years,
                "latest_trade_date": latest_trade_date,
                "basis_formula": "futures_minus_spot",
            },
            "error": "no_longterm_points",
        }

    valid_basis_count = sum(1 for p in points if p.get("basis") is not None)
    if valid_basis_count == 0:
        return {
            "points": points,
            "meta": {
                "product_code": product,
                "spot_index_code": index_code,
                "lookback_years": years,
                "latest_trade_date": latest_trade_date,
                "basis_formula": "futures_minus_spot",
            },
            "error": "no_basis_values",
        }

    return {
        "points": points,
        "summary": {
            "valid_points": valid_basis_count,
            "total_points": len(points),
            "start_date": points[0].get("trade_date"),
            "end_date": points[-1].get("trade_date"),
        },
        "meta": {
            "product_code": product,
            "spot_index_code": index_code,
            "lookback_years": years,
            "latest_trade_date": latest_trade_date,
            "basis_formula": "futures_minus_spot",
        },
    }
