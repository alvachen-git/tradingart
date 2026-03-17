from __future__ import annotations

from datetime import datetime
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text

CROSS_ASSET_IV_BASKET_VERSION = "v1_20260316"
CROSS_ASSET_IV_MIN_VALID = 0.0001
CROSS_ASSET_IV_DEFAULT_LOOKBACK = 252
CROSS_ASSET_IV_DEFAULT_SMOOTH_SPAN = 5
CROSS_ASSET_IV_MIN_COVERAGE_PCT = 60.0

CROSS_ASSET_IV_BASKET = [
    {"asset_code": "159915.SZ", "asset_name": "创业板ETF", "weight": 10.0, "source": "etf"},
    {"asset_code": "IM", "asset_name": "中证1000股指", "weight": 10.0, "source": "commodity"},
    {"asset_code": "SC", "asset_name": "原油", "weight": 10.0, "source": "commodity"},
    {"asset_code": "AU", "asset_name": "黄金", "weight": 10.0, "source": "commodity"},
    {"asset_code": "CU", "asset_name": "铜", "weight": 10.0, "source": "commodity"},
    {"asset_code": "I", "asset_name": "铁矿石", "weight": 8.0, "source": "commodity"},
    {"asset_code": "M", "asset_name": "豆粕", "weight": 7.0, "source": "commodity"},
    {"asset_code": "P", "asset_name": "棕榈油", "weight": 7.0, "source": "commodity"},
    {"asset_code": "TA", "asset_name": "PTA", "weight": 7.0, "source": "commodity"},
    {"asset_code": "CF", "asset_name": "棉花", "weight": 5.0, "source": "commodity"},
    {"asset_code": "FG", "asset_name": "玻璃", "weight": 5.0, "source": "commodity"},
    {"asset_code": "AL", "asset_name": "铝", "weight": 5.0, "source": "commodity"},
    {"asset_code": "MA", "asset_name": "甲醇", "weight": 6.0, "source": "commodity"},
]

CROSS_ASSET_IV_TOTAL_WEIGHT = float(sum(item["weight"] for item in CROSS_ASSET_IV_BASKET))
if abs(CROSS_ASSET_IV_TOTAL_WEIGHT - 100.0) > 1e-9:
    raise ValueError(f"璺ㄨ祫浜?IV 绡瓙鏉冮噸閿欒锛屽綋鍓嶅悎璁?{CROSS_ASSET_IV_TOTAL_WEIGHT}锛屽簲涓?100")
CROSS_ASSET_IV_MIN_COVERAGE_WEIGHT = CROSS_ASSET_IV_TOTAL_WEIGHT * CROSS_ASSET_IV_MIN_COVERAGE_PCT / 100.0
_TABLES_READY_BY_ENGINE: dict[int, bool] = {}
_COLUMN_EXISTS_CACHE: dict[tuple[int, str, str], bool] = {}


def _normalize_trade_date_yyyymmdd(value=None) -> str:
    if value is None or str(value).strip() == "":
        return datetime.now().strftime("%Y%m%d")
    text_date = str(value).strip()
    digits = "".join(ch for ch in text_date if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return datetime.now().strftime("%Y%m%d")


def _cross_asset_iv_regime(index_raw: Optional[float], coverage_pct: Optional[float] = None) -> str:
    if coverage_pct is not None and float(coverage_pct) < CROSS_ASSET_IV_MIN_COVERAGE_PCT:
        return "样本不足"
    if index_raw is None or pd.isna(index_raw):
        return "无数据"
    value = float(index_raw)
    if value < 20:
        return "低"
    if value < 60:
        return "中"
    if value < 85:
        return "高"
    return "极高"


def _ensure_cross_asset_iv_tables(engine) -> None:
    if engine is None:
        return
    key = id(engine)
    if _TABLES_READY_BY_ENGINE.get(key):
        return

    ddl_index = """
        CREATE TABLE IF NOT EXISTS cross_asset_iv_index_daily (
            trade_date VARCHAR(8) NOT NULL,
            index_raw FLOAT NULL,
            index_ewma5 FLOAT NULL,
            coverage_pct FLOAT NOT NULL DEFAULT 0,
            available_weight FLOAT NOT NULL DEFAULT 0,
            regime VARCHAR(16) NOT NULL DEFAULT '无数据',
            basket_version VARCHAR(32) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, basket_version),
            KEY idx_cross_asset_iv_index_version_date (basket_version, trade_date)
        ) DEFAULT CHARSET=utf8mb4;
    """
    ddl_component = """
        CREATE TABLE IF NOT EXISTS cross_asset_iv_index_component_daily (
            trade_date VARCHAR(8) NOT NULL,
            basket_version VARCHAR(32) NOT NULL,
            asset_code VARCHAR(32) NOT NULL,
            asset_name VARCHAR(64) NOT NULL,
            iv FLOAT NULL,
            iv_rank FLOAT NULL,
            weight FLOAT NOT NULL DEFAULT 0,
            weighted_contribution FLOAT NOT NULL DEFAULT 0,
            valid_flag TINYINT(1) NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, basket_version, asset_code),
            KEY idx_cross_asset_iv_component_version_date (basket_version, trade_date)
        ) DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl_index))
        conn.execute(text(ddl_component))
    _TABLES_READY_BY_ENGINE[key] = True


def _prepare_iv_series(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["trade_date", "iv"])
    out = df.copy()
    out["trade_date"] = out["trade_date"].astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
    out["iv"] = pd.to_numeric(out["iv"], errors="coerce")
    out = out.dropna(subset=["trade_date", "iv"])
    out = out[out["iv"] > CROSS_ASSET_IV_MIN_VALID]
    out = out.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")
    return out[["trade_date", "iv"]]


def _table_has_column(engine, table_name: str, column_name: str) -> bool:
    if engine is None:
        return False
    cache_key = (id(engine), table_name, column_name)
    cached = _COLUMN_EXISTS_CACHE.get(cache_key)
    if cached is not None:
        return bool(cached)

    exists = False
    try:
        sql = text(f"SHOW COLUMNS FROM {table_name} LIKE :column_name")
        with engine.connect() as conn:
            exists = conn.execute(sql, {"column_name": column_name}).first() is not None
    except Exception:
        exists = False
    _COLUMN_EXISTS_CACHE[cache_key] = exists
    return exists


def _query_etf_iv_series(engine, etf_code: str, trade_date: str, limit_rows: int) -> pd.DataFrame:
    sql = text(
        f"""
        SELECT REPLACE(trade_date, '-', '') AS trade_date, iv
        FROM etf_iv_history
        WHERE etf_code = :etf_code
          AND REPLACE(trade_date, '-', '') <= :trade_date
          AND iv > :min_valid
        ORDER BY REPLACE(trade_date, '-', '') DESC
        LIMIT {int(limit_rows)}
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "etf_code": etf_code,
                "trade_date": trade_date,
                "min_valid": CROSS_ASSET_IV_MIN_VALID,
            },
        )
    return _prepare_iv_series(df)


def _query_commodity_iv_series(
    engine,
    sql_prefix_condition_fn: Callable[[str], str],
    asset_code: str,
    trade_date: str,
    limit_rows: int,
) -> pd.DataFrame:
    # 方案 A：若表中存在 commodity_code 列，优先按 commodity_code 读取
    if _table_has_column(engine, "commodity_iv_history", "commodity_code"):
        sql_by_code = text(
            f"""
            SELECT REPLACE(trade_date, '-', '') AS trade_date, AVG(iv) AS iv
            FROM commodity_iv_history
            WHERE commodity_code = :asset_code
              AND REPLACE(trade_date, '-', '') <= :trade_date
              AND iv > :min_valid
            GROUP BY REPLACE(trade_date, '-', '')
            ORDER BY trade_date DESC
            LIMIT {int(limit_rows)}
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(
                sql_by_code,
                conn,
                params={
                    "asset_code": asset_code,
                    "trade_date": trade_date,
                    "min_valid": CROSS_ASSET_IV_MIN_VALID,
                },
            )
        prepared = _prepare_iv_series(df)
        if not prepared.empty:
            return prepared

    # 鏂规 B锛氬厹搴曟寜 ts_code 鍓嶇紑
    prefix_condition = sql_prefix_condition_fn(asset_code)
    sql_by_prefix = text(
        f"""
        SELECT REPLACE(trade_date, '-', '') AS trade_date, AVG(iv) AS iv
        FROM commodity_iv_history
        WHERE {prefix_condition}
          AND REPLACE(trade_date, '-', '') <= :trade_date
          AND iv > :min_valid
        GROUP BY REPLACE(trade_date, '-', '')
        ORDER BY trade_date DESC
        LIMIT {int(limit_rows)}
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(
            sql_by_prefix,
            conn,
            params={
                "trade_date": trade_date,
                "min_valid": CROSS_ASSET_IV_MIN_VALID,
            },
        )
    return _prepare_iv_series(df)


def _resolve_cross_asset_trade_date(engine, end_date: str) -> Optional[str]:
    if engine is None:
        return None
    normalized_end = _normalize_trade_date_yyyymmdd(end_date)
    etf_codes = [item["asset_code"] for item in CROSS_ASSET_IV_BASKET if item["source"] == "etf"]
    etf_codes_literal = ",".join([f"'{code}'" for code in etf_codes]) or "''"

    sql_etf = text(
        f"""
        SELECT MAX(REPLACE(trade_date, '-', '')) AS d
        FROM etf_iv_history
        WHERE etf_code IN ({etf_codes_literal})
          AND REPLACE(trade_date, '-', '') <= :end_date
          AND iv > :min_valid
        """
    )
    sql_commodity = text(
        """
        SELECT MAX(REPLACE(trade_date, '-', '')) AS d
        FROM commodity_iv_history
        WHERE REPLACE(trade_date, '-', '') <= :end_date
          AND iv > :min_valid
        """
    )
    with engine.connect() as conn:
        etf_date = conn.execute(
            sql_etf,
            {"end_date": normalized_end, "min_valid": CROSS_ASSET_IV_MIN_VALID},
        ).scalar()
        commodity_date = conn.execute(
            sql_commodity,
            {"end_date": normalized_end, "min_valid": CROSS_ASSET_IV_MIN_VALID},
        ).scalar()

    candidates = [str(x)[:8] for x in [etf_date, commodity_date] if x]
    if not candidates:
        return None
    return max(candidates)


def _build_cross_asset_component(
    engine,
    sql_prefix_condition_fn: Callable[[str], str],
    asset_def: dict,
    trade_date: str,
    lookback: int,
) -> dict:
    asset_code = asset_def["asset_code"]
    asset_name = asset_def["asset_name"]
    weight = float(asset_def["weight"])
    source = asset_def["source"]
    limit_rows = max(int(lookback) * 3, int(lookback) + 20, 320)

    if source == "etf":
        series = _query_etf_iv_series(engine, asset_code, trade_date, limit_rows)
    else:
        series = _query_commodity_iv_series(
            engine=engine,
            sql_prefix_condition_fn=sql_prefix_condition_fn,
            asset_code=asset_code,
            trade_date=trade_date,
            limit_rows=limit_rows,
        )

    if series.empty:
        return {
            "trade_date": trade_date,
            "asset_code": asset_code,
            "asset_name": asset_name,
            "iv": None,
            "iv_rank": None,
            "weight": weight,
            "weighted_contribution": 0.0,
            "valid_flag": 0,
        }

    current_row = series[series["trade_date"] == trade_date]
    if current_row.empty:
        return {
            "trade_date": trade_date,
            "asset_code": asset_code,
            "asset_name": asset_name,
            "iv": None,
            "iv_rank": None,
            "weight": weight,
            "weighted_contribution": 0.0,
            "valid_flag": 0,
        }

    window = series[series["trade_date"] <= trade_date].tail(max(int(lookback), 1))
    current_iv = float(current_row.iloc[-1]["iv"])
    iv_min = float(window["iv"].min()) if not window.empty else current_iv
    iv_max = float(window["iv"].max()) if not window.empty else current_iv
    if iv_max <= iv_min:
        iv_rank = 0.0
    else:
        iv_rank = (current_iv - iv_min) / (iv_max - iv_min) * 100.0
    iv_rank = max(0.0, min(100.0, float(iv_rank)))

    return {
        "trade_date": trade_date,
        "asset_code": asset_code,
        "asset_name": asset_name,
        "iv": current_iv,
        "iv_rank": iv_rank,
        "weight": weight,
        "weighted_contribution": 0.0,
        "valid_flag": 1,
    }


def _calculate_cross_asset_components(
    engine,
    sql_prefix_condition_fn: Callable[[str], str],
    trade_date: str,
    lookback: int,
):
    components = [
        _build_cross_asset_component(engine, sql_prefix_condition_fn, item, trade_date, lookback)
        for item in CROSS_ASSET_IV_BASKET
    ]
    available_weight = float(sum(c["weight"] for c in components if int(c["valid_flag"]) == 1))
    coverage_pct = (available_weight / CROSS_ASSET_IV_TOTAL_WEIGHT * 100.0) if CROSS_ASSET_IV_TOTAL_WEIGHT > 0 else 0.0

    if available_weight <= 0:
        return components, None, coverage_pct, 0.0
    if available_weight < CROSS_ASSET_IV_MIN_COVERAGE_WEIGHT:
        for comp in components:
            comp["weighted_contribution"] = 0.0
        return components, None, coverage_pct, available_weight

    index_raw = 0.0
    for comp in components:
        if int(comp["valid_flag"]) == 1:
            contrib = float(comp["weight"]) * float(comp["iv_rank"]) / available_weight
            comp["weighted_contribution"] = contrib
            index_raw += contrib
        else:
            comp["weighted_contribution"] = 0.0

    index_raw = max(0.0, min(100.0, float(index_raw)))
    return components, index_raw, coverage_pct, available_weight


def _calculate_cross_asset_ewma(
    engine,
    trade_date: str,
    index_raw: Optional[float],
    smooth_span: int,
    basket_version: str,
) -> Optional[float]:
    if index_raw is None or engine is None:
        return None

    _ensure_cross_asset_iv_tables(engine)
    sql = text(
        """
        SELECT trade_date, index_raw
        FROM cross_asset_iv_index_daily
        WHERE basket_version = :basket_version
          AND trade_date < :trade_date
          AND index_raw IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT 180
        """
    )
    with engine.connect() as conn:
        hist_df = pd.read_sql(
            sql,
            conn,
            params={"basket_version": basket_version, "trade_date": trade_date},
        )

    values = []
    if not hist_df.empty:
        hist_df["index_raw"] = pd.to_numeric(hist_df["index_raw"], errors="coerce")
        hist_df = hist_df.dropna(subset=["index_raw"]).sort_values("trade_date")
        values.extend(hist_df["index_raw"].tolist())
    values.append(float(index_raw))
    ewma_value = pd.Series(values).ewm(span=max(int(smooth_span), 1), adjust=False).mean().iloc[-1]
    return float(ewma_value)


def _save_cross_asset_iv_result(engine, payload: dict) -> None:
    if engine is None:
        return
    _ensure_cross_asset_iv_tables(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO cross_asset_iv_index_daily (
                    trade_date, index_raw, index_ewma5, coverage_pct, available_weight, regime, basket_version
                ) VALUES (
                    :trade_date, :index_raw, :index_ewma5, :coverage_pct, :available_weight, :regime, :basket_version
                )
                ON DUPLICATE KEY UPDATE
                    index_raw = VALUES(index_raw),
                    index_ewma5 = VALUES(index_ewma5),
                    coverage_pct = VALUES(coverage_pct),
                    available_weight = VALUES(available_weight),
                    regime = VALUES(regime)
                """
            ),
            {
                "trade_date": payload["trade_date"],
                "index_raw": payload["index_raw"],
                "index_ewma5": payload["index_ewma5"],
                "coverage_pct": payload["coverage_pct"],
                "available_weight": payload["available_weight"],
                "regime": payload["regime"],
                "basket_version": payload["basket_version"],
            },
        )
        conn.execute(
            text(
                """
                DELETE FROM cross_asset_iv_index_component_daily
                WHERE trade_date = :trade_date AND basket_version = :basket_version
                """
            ),
            {"trade_date": payload["trade_date"], "basket_version": payload["basket_version"]},
        )

        component_rows = []
        for comp in payload.get("components", []):
            component_rows.append(
                {
                    "trade_date": payload["trade_date"],
                    "basket_version": payload["basket_version"],
                    "asset_code": comp["asset_code"],
                    "asset_name": comp["asset_name"],
                    "iv": comp["iv"],
                    "iv_rank": comp["iv_rank"],
                    "weight": comp["weight"],
                    "weighted_contribution": comp["weighted_contribution"],
                    "valid_flag": int(comp["valid_flag"]),
                }
            )
        if component_rows:
            conn.execute(
                text(
                    """
                    INSERT INTO cross_asset_iv_index_component_daily (
                        trade_date, basket_version, asset_code, asset_name, iv, iv_rank, weight, weighted_contribution, valid_flag
                    ) VALUES (
                        :trade_date, :basket_version, :asset_code, :asset_name, :iv, :iv_rank, :weight, :weighted_contribution, :valid_flag
                    )
                    """
                ),
                component_rows,
            )


def _read_cross_asset_iv_index_row(engine, end_date=None, basket_version: str = CROSS_ASSET_IV_BASKET_VERSION):
    if engine is None:
        return None
    _ensure_cross_asset_iv_tables(engine)
    end_date_str = _normalize_trade_date_yyyymmdd(end_date)
    sql = text(
        """
        SELECT trade_date, index_raw, index_ewma5, coverage_pct, available_weight, regime, basket_version
        FROM cross_asset_iv_index_daily
        WHERE basket_version = :basket_version
          AND trade_date <= :end_date
        ORDER BY trade_date DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(
            sql,
            {"basket_version": basket_version, "end_date": end_date_str},
        ).mappings().fetchone()
    return dict(row) if row else None


def _read_cross_asset_components_df(engine, trade_date: str, basket_version: str = CROSS_ASSET_IV_BASKET_VERSION) -> pd.DataFrame:
    if engine is None or not trade_date:
        return pd.DataFrame()
    _ensure_cross_asset_iv_tables(engine)
    sql = text(
        """
        SELECT trade_date, asset_code, asset_name, iv, iv_rank, weight, weighted_contribution, valid_flag
        FROM cross_asset_iv_index_component_daily
        WHERE basket_version = :basket_version
          AND trade_date = :trade_date
        ORDER BY weight DESC, asset_code
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"basket_version": basket_version, "trade_date": trade_date},
        )
    return df


def _get_backfill_trade_dates(
    engine,
    end_date=None,
    days: Optional[int] = 252,
    start_date=None,
) -> list[str]:
    if engine is None:
        return []
    end_date_str = _normalize_trade_date_yyyymmdd(end_date)
    params = {"end_date": end_date_str, "min_valid": CROSS_ASSET_IV_MIN_VALID}
    if start_date:
        start_date_str = _normalize_trade_date_yyyymmdd(start_date)
        sql = text(
            """
            SELECT DISTINCT t.trade_date
            FROM (
                SELECT REPLACE(trade_date, '-', '') AS trade_date
                FROM etf_iv_history
                WHERE etf_code = '159915.SZ'
                  AND REPLACE(trade_date, '-', '') >= :start_date
                  AND REPLACE(trade_date, '-', '') <= :end_date
                  AND iv > :min_valid
                UNION
                SELECT REPLACE(trade_date, '-', '') AS trade_date
                FROM commodity_iv_history
                WHERE REPLACE(trade_date, '-', '') >= :start_date
                  AND REPLACE(trade_date, '-', '') <= :end_date
                  AND iv > :min_valid
            ) t
            ORDER BY trade_date
            """
        )
        params["start_date"] = start_date_str
    else:
        limit_days = max(int(days or 252), 1)
        sql = text(
            f"""
            SELECT trade_date
            FROM (
                SELECT DISTINCT t.trade_date
                FROM (
                    SELECT REPLACE(trade_date, '-', '') AS trade_date
                    FROM etf_iv_history
                    WHERE etf_code = '159915.SZ'
                      AND REPLACE(trade_date, '-', '') <= :end_date
                      AND iv > :min_valid
                    UNION
                    SELECT REPLACE(trade_date, '-', '') AS trade_date
                    FROM commodity_iv_history
                    WHERE REPLACE(trade_date, '-', '') <= :end_date
                      AND iv > :min_valid
                ) t
                ORDER BY REPLACE(trade_date, '-', '') DESC
                LIMIT {limit_days}
            ) t
            ORDER BY trade_date
            """
        )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        return []
    return [str(x)[:8] for x in df["trade_date"].tolist() if str(x).strip()]


def refresh_cross_asset_iv_index_for_date(
    engine,
    sql_prefix_condition_fn: Callable[[str], str],
    trade_date=None,
    lookback: int = CROSS_ASSET_IV_DEFAULT_LOOKBACK,
    smooth_span: int = CROSS_ASSET_IV_DEFAULT_SMOOTH_SPAN,
    basket_version: str = CROSS_ASSET_IV_BASKET_VERSION,
    persist: bool = True,
) -> dict:
    end_date = _normalize_trade_date_yyyymmdd(trade_date)
    resolved_date = _resolve_cross_asset_trade_date(engine, end_date)
    if resolved_date is None:
        return {
            "trade_date": None,
            "index_raw": None,
            "index_ewma5": None,
            "coverage_pct": 0.0,
            "available_weight": 0.0,
            "regime": "无数据",
            "basket_version": basket_version,
            "components": [],
        }

    components, index_raw, coverage_pct, available_weight = _calculate_cross_asset_components(
        engine=engine,
        sql_prefix_condition_fn=sql_prefix_condition_fn,
        trade_date=resolved_date,
        lookback=int(lookback),
    )
    index_ewma5 = _calculate_cross_asset_ewma(
        engine=engine,
        trade_date=resolved_date,
        index_raw=index_raw,
        smooth_span=int(smooth_span),
        basket_version=basket_version,
    )
    payload = {
        "trade_date": resolved_date,
        "index_raw": None if index_raw is None else float(index_raw),
        "index_ewma5": None if index_ewma5 is None else float(index_ewma5),
        "coverage_pct": float(coverage_pct),
        "available_weight": float(available_weight),
        "regime": _cross_asset_iv_regime(index_raw, coverage_pct=coverage_pct),
        "basket_version": basket_version,
        "components": components,
    }
    if persist and payload["trade_date"]:
        _save_cross_asset_iv_result(engine, payload)
    return payload


def backfill_cross_asset_iv_index_history(
    engine,
    sql_prefix_condition_fn: Callable[[str], str],
    end_date=None,
    days: int = 252,
    start_date=None,
    lookback: int = CROSS_ASSET_IV_DEFAULT_LOOKBACK,
    smooth_span: int = CROSS_ASSET_IV_DEFAULT_SMOOTH_SPAN,
    basket_version: str = CROSS_ASSET_IV_BASKET_VERSION,
    only_missing: bool = False,
) -> dict:
    if engine is None:
        return {"requested": 0, "computed": 0, "skipped": 0, "first_date": None, "last_date": None}

    _ensure_cross_asset_iv_tables(engine)
    trade_dates = _get_backfill_trade_dates(
        engine=engine,
        end_date=end_date,
        days=days,
        start_date=start_date,
    )
    if not trade_dates:
        return {"requested": 0, "computed": 0, "skipped": 0, "first_date": None, "last_date": None}

    existing_dates = set()
    if only_missing:
        sql_existing = text(
            """
            SELECT trade_date
            FROM cross_asset_iv_index_daily
            WHERE basket_version = :basket_version
              AND trade_date >= :start_date
              AND trade_date <= :end_date
            """
        )
        with engine.connect() as conn:
            existing_df = pd.read_sql(
                sql_existing,
                conn,
                params={
                    "basket_version": basket_version,
                    "start_date": trade_dates[0],
                    "end_date": trade_dates[-1],
                },
            )
        if not existing_df.empty:
            existing_dates = set(existing_df["trade_date"].astype(str).tolist())

    computed = 0
    skipped = 0
    for d in trade_dates:
        if only_missing and d in existing_dates:
            skipped += 1
            continue
        payload = refresh_cross_asset_iv_index_for_date(
            engine=engine,
            sql_prefix_condition_fn=sql_prefix_condition_fn,
            trade_date=d,
            lookback=lookback,
            smooth_span=smooth_span,
            basket_version=basket_version,
            persist=True,
        )
        if payload.get("trade_date"):
            computed += 1

    return {
        "requested": len(trade_dates),
        "computed": computed,
        "skipped": skipped,
        "first_date": trade_dates[0],
        "last_date": trade_dates[-1],
    }


def get_cross_asset_iv_index(
    engine,
    sql_prefix_condition_fn: Callable[[str], str],
    end_date=None,
    lookback: int = CROSS_ASSET_IV_DEFAULT_LOOKBACK,
    smooth_span: int = CROSS_ASSET_IV_DEFAULT_SMOOTH_SPAN,
    basket_version: str = CROSS_ASSET_IV_BASKET_VERSION,
    auto_compute: bool = True,
) -> dict:
    row = _read_cross_asset_iv_index_row(engine=engine, end_date=end_date, basket_version=basket_version)
    if row:
        comp_df = _read_cross_asset_components_df(
            engine=engine,
            trade_date=str(row["trade_date"]),
            basket_version=basket_version,
        )
        return {
            "trade_date": str(row.get("trade_date")),
            "index_raw": None if row.get("index_raw") is None else float(row.get("index_raw")),
            "index_ewma5": None if row.get("index_ewma5") is None else float(row.get("index_ewma5")),
            "coverage_pct": float(row.get("coverage_pct") or 0.0),
            "available_weight": float(row.get("available_weight") or 0.0),
            "regime": row.get("regime") or "无数据",
            "basket_version": row.get("basket_version") or basket_version,
            "components": comp_df.to_dict("records") if not comp_df.empty else [],
        }

    if not auto_compute:
        return {
            "trade_date": None,
            "index_raw": None,
            "index_ewma5": None,
            "coverage_pct": 0.0,
            "available_weight": 0.0,
            "regime": "无数据",
            "basket_version": basket_version,
            "components": [],
        }

    return refresh_cross_asset_iv_index_for_date(
        engine=engine,
        sql_prefix_condition_fn=sql_prefix_condition_fn,
        trade_date=end_date,
        lookback=lookback,
        smooth_span=smooth_span,
        basket_version=basket_version,
        persist=True,
    )


def get_cross_asset_iv_components(
    engine,
    sql_prefix_condition_fn: Callable[[str], str],
    trade_date=None,
    lookback: int = CROSS_ASSET_IV_DEFAULT_LOOKBACK,
    smooth_span: int = CROSS_ASSET_IV_DEFAULT_SMOOTH_SPAN,
    basket_version: str = CROSS_ASSET_IV_BASKET_VERSION,
    auto_compute: bool = True,
) -> pd.DataFrame:
    target_date = None
    if trade_date:
        target_date = _normalize_trade_date_yyyymmdd(trade_date)
    else:
        latest = _read_cross_asset_iv_index_row(engine=engine, basket_version=basket_version)
        if latest:
            target_date = str(latest.get("trade_date"))

    if target_date:
        df = _read_cross_asset_components_df(engine=engine, trade_date=target_date, basket_version=basket_version)
        if not df.empty:
            return df

    if not auto_compute:
        return pd.DataFrame()

    fallback = refresh_cross_asset_iv_index_for_date(
        engine=engine,
        sql_prefix_condition_fn=sql_prefix_condition_fn,
        trade_date=target_date,
        lookback=lookback,
        smooth_span=smooth_span,
        basket_version=basket_version,
        persist=True,
    )
    return pd.DataFrame(fallback.get("components") or [])


def get_cross_asset_iv_index_history(
    engine,
    end_date=None,
    days: int = 252,
    start_date=None,
    basket_version: str = CROSS_ASSET_IV_BASKET_VERSION,
    min_coverage_pct: Optional[float] = None,
) -> pd.DataFrame:
    if engine is None:
        return pd.DataFrame()
    _ensure_cross_asset_iv_tables(engine)

    end_date_str = _normalize_trade_date_yyyymmdd(end_date)
    if start_date:
        start_date_str = _normalize_trade_date_yyyymmdd(start_date)
        sql = text(
            """
            SELECT trade_date, index_raw, index_ewma5, coverage_pct, available_weight, regime
            FROM cross_asset_iv_index_daily
            WHERE basket_version = :basket_version
              AND trade_date >= :start_date
              AND trade_date <= :end_date
            ORDER BY trade_date
            """
        )
        params = {
            "basket_version": basket_version,
            "start_date": start_date_str,
            "end_date": end_date_str,
        }
    else:
        limit_days = max(int(days), 1)
        sql = text(
            f"""
            SELECT trade_date, index_raw, index_ewma5, coverage_pct, available_weight, regime
            FROM cross_asset_iv_index_daily
            WHERE basket_version = :basket_version
              AND trade_date <= :end_date
            ORDER BY trade_date DESC
            LIMIT {limit_days}
            """
        )
        params = {"basket_version": basket_version, "end_date": end_date_str}
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        return df

    for col in ["index_raw", "index_ewma5", "coverage_pct", "available_weight"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trade_date"] = df["trade_date"].astype(str)
    if min_coverage_pct is not None:
        threshold = max(float(min_coverage_pct), 0.0)
        df = df[df["coverage_pct"] >= threshold]
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


