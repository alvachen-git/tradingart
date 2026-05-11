"""
Safe stock report generator for the Intelligence Station.

The first version is intentionally rule-driven: it reuses the AI simulation
service's sector rotation, technical candidate pool, and CSI500 risk gate, then
renders a deterministic HTML report that can be previewed before publishing.
"""

from __future__ import annotations

import argparse
import html
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text

import ai_simulation_service as sim
import subscription_service as sub_svc
from data_engine import get_latest_data_date


CHANNEL_CODE = "safe_stock_report"
PREVIEW_PATH = "preview_safe_stock_report.html"
MAX_BUY_TRACKING = 10
WATCH_LIMIT = 5
TAKE_PROFIT_HALF_PCT = 0.15
TAKE_PROFIT_EXIT_PCT = 0.25
BOTTOM_BREAK_BUFFER = 0.99
BOTTOM_BOX_MAX_WIDTH_PCT = 0.18
BEARISH_PATTERN_TOKENS = (
    "\u7a7a\u5934\u541e\u566c",
    "\u770b\u8dcc\u541e\u566c",
    "\u9634\u5305\u9633",
    "\u7834\u4f4d",
    "\u8dcc\u7834",
    "\u4e09\u53ea\u4e4c\u9e26",
    "\u591c\u661f",
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _esc(value: Any) -> str:
    return html.escape(str(value or ""), quote=True)


def _fmt_pct(value: Any, digits: int = 2) -> str:
    return f"{_safe_float(value) * 100:.{digits}f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    return f"{_safe_float(value):.{digits}f}"


def _status_label(status: Any) -> str:
    return {
        "watching": "观察",
        "bought": "持有",
        "half_taken": "已卖出一半",
        "exited": "已出场",
    }.get(str(status or ""), str(status or ""))


def _action_label(action: Any) -> str:
    return {
        "hold": "持有",
        "add": "加仓",
        "take_half": "卖出一半",
        "exit": "全部出场",
    }.get(str(action or ""), str(action or ""))


def _market_style_commentary(
    sectors: List[Dict[str, Any]],
    buys: List[Dict[str, Any]],
    watches: List[Dict[str, Any]],
) -> str:
    sector_names = [str(s.get("industry") or "").strip() for s in sectors[:4] if str(s.get("industry") or "").strip()]
    sector_text = "、".join(sector_names[:3]) if sector_names else "低位修复方向"
    extra_sector = f"，{sector_names[3]}也有资金照顾" if len(sector_names) >= 4 else ""
    buy_count = len(buys)
    watch_count = len(watches)
    if buy_count > 0:
        opportunity = f"今天先看{buy_count}个可买标的，另有{watch_count}个观察标的等确认。"
    elif watch_count > 0:
        opportunity = f"暂时不急着追，先把{watch_count}个观察标的放进雷达。"
    else:
        opportunity = "机会还没到特别顺手的位置，耐心比冲动更值钱。"
    text_value = (
        f"今天盘面不是一条线猛冲，更多是结构性轮动，资金回流集中在{sector_text}{extra_sector}。"
        f"小爱看好中国资产的长期韧性，但短线仍讲纪律，不追高、不硬买，重点找刚从底部转强的机会。{opportunity}"
    )
    return text_value[:200]


def _normalize_trade_date(trade_date: Optional[str] = None) -> str:
    raw = str(trade_date or get_latest_data_date() or "")
    digits = "".join(ch for ch in raw if ch.isdigit())[:8]
    if len(digits) != 8:
        raise ValueError(f"invalid trade_date: {trade_date}")
    return digits


def ensure_safe_stock_tables() -> None:
    if sim.engine is None:
        raise ValueError("database engine is unavailable")
    ddl = """
    CREATE TABLE IF NOT EXISTS safe_stock_recommendations (
        id BIGINT NOT NULL AUTO_INCREMENT,
        symbol VARCHAR(32) NOT NULL,
        name VARCHAR(128) DEFAULT '',
        sector_name VARCHAR(128) DEFAULT '',
        recommendation_type VARCHAR(16) NOT NULL DEFAULT 'watch',
        status VARCHAR(24) NOT NULL DEFAULT 'watching',
        first_signal_date VARCHAR(16) NOT NULL,
        last_report_date VARCHAR(16) NOT NULL,
        entry_price DOUBLE NOT NULL DEFAULT 0,
        stop_price DOUBLE NOT NULL DEFAULT 0,
        score DOUBLE NOT NULL DEFAULT 0,
        sector_rank INT NOT NULL DEFAULT 999,
        take_profit_count INT NOT NULL DEFAULT 0,
        add_count INT NOT NULL DEFAULT 0,
        weak_count INT NOT NULL DEFAULT 0,
        bottom_low DOUBLE NOT NULL DEFAULT 0,
        bottom_high DOUBLE NOT NULL DEFAULT 0,
        bottom_range_date VARCHAR(16) DEFAULT '',
        exit_reason VARCHAR(255) DEFAULT '',
        notes VARCHAR(255) DEFAULT '',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uq_safe_stock_symbol (symbol),
        KEY idx_safe_stock_status (status),
        KEY idx_safe_stock_report_date (last_report_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with sim.engine.begin() as conn:
        conn.execute(text(ddl))
        col_count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'safe_stock_recommendations'
                  AND COLUMN_NAME = 'add_count'
                """
            )
        ).scalar()
        if int(col_count or 0) == 0:
            conn.execute(text("ALTER TABLE safe_stock_recommendations ADD COLUMN add_count INT NOT NULL DEFAULT 0 AFTER take_profit_count"))
        for col_name, alter_sql in {
            "weak_count": "ALTER TABLE safe_stock_recommendations ADD COLUMN weak_count INT NOT NULL DEFAULT 0 AFTER add_count",
            "bottom_low": "ALTER TABLE safe_stock_recommendations ADD COLUMN bottom_low DOUBLE NOT NULL DEFAULT 0 AFTER weak_count",
            "bottom_high": "ALTER TABLE safe_stock_recommendations ADD COLUMN bottom_high DOUBLE NOT NULL DEFAULT 0 AFTER bottom_low",
            "bottom_range_date": "ALTER TABLE safe_stock_recommendations ADD COLUMN bottom_range_date VARCHAR(16) DEFAULT '' AFTER bottom_high",
        }.items():
            exists = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'safe_stock_recommendations'
                      AND COLUMN_NAME = :col_name
                    """
                ),
                {"col_name": col_name},
            ).scalar()
            if int(exists or 0) == 0:
                conn.execute(text(alter_sql))


def load_active_recommendations() -> pd.DataFrame:
    if sim.engine is None:
        return pd.DataFrame()
    try:
        ensure_safe_stock_tables()
    except Exception:
        pass
    sql = text(
        """
        SELECT symbol, name, sector_name, recommendation_type, status, first_signal_date,
               last_report_date, entry_price, stop_price, score, sector_rank,
               take_profit_count, add_count, weak_count, bottom_low, bottom_high,
               bottom_range_date, exit_reason, notes
        FROM safe_stock_recommendations
        WHERE status IN ('watching', 'bought', 'half_taken')
        ORDER BY first_signal_date ASC, score DESC
        """
    )
    try:
        with sim.engine.connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception:
        return pd.DataFrame()


def _active_bought_symbols(active_df: pd.DataFrame) -> set[str]:
    if active_df is None or active_df.empty:
        return set()
    status = active_df["status"].astype(str) if "status" in active_df.columns else pd.Series(dtype=str)
    rec_type = active_df["recommendation_type"].astype(str) if "recommendation_type" in active_df.columns else pd.Series("", index=active_df.index)
    work = active_df[status.isin(["bought", "half_taken"]) | ((rec_type == "buy") & ~status.isin(["exited", "watching"]))]
    return {sim._normalize_symbol(x) for x in work["symbol"].tolist() if sim._normalize_symbol(x)}


def _candidate_row_map(candidates_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if candidates_df is None or candidates_df.empty:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for _, row in candidates_df.iterrows():
        symbol = sim._normalize_symbol(row.get("symbol", ""))
        if symbol:
            out[symbol] = dict(row)
    return out


def _is_noise_bearish(pattern_text: str) -> bool:
    text_value = str(pattern_text or "")
    tokens = ["\u5047\u7a81\u7834", "\u8bf1\u591a", "\u5047\u8dcc\u7834"]
    return any(token in text_value for token in tokens)


def _is_hard_bearish(pattern_text: str) -> bool:
    text_value = str(pattern_text or "")
    tokens = [
        "\u7834\u4f4d",
        "\u8dcc\u7834",
        "\u7a7a\u5934\u541e\u566c",
        "\u770b\u8dcc\u541e\u566c",
        "\u9634\u5305\u9633",
        "\u4e09\u53ea\u4e4c\u9e26",
        "\u591c\u661f",
    ]
    return any(token in text_value for token in tokens) or any(token in text_value for token in BEARISH_PATTERN_TOKENS)


def _candidate_bottom_range(row: Any) -> Tuple[float, float, str]:
    low = _safe_float(row.get("platform_low"), 0.0)
    high = _safe_float(row.get("platform_high"), 0.0)
    close = _safe_float(row.get("close"), _safe_float(row.get("entry_price"), 0.0))
    if low > 0 and high > low and close > 0 and (high - low) / close <= BOTTOM_BOX_MAX_WIDTH_PCT:
        return low, high, str(row.get("breakout_date") or "")
    candle_low = _safe_float(row.get("breakout_candle_low"), 0.0)
    stop = _safe_float(row.get("stop_price"), 0.0)
    low = max(x for x in [candle_low, stop, 0.0] if x >= 0)
    return low, high, str(row.get("breakout_date") or "")


def _history_bottom_range(symbol: str, trade_date: str) -> Tuple[float, float, str]:
    try:
        hist = sim._fetch_recent_price_history([symbol], trade_date, lookback=40)
    except Exception:
        return 0.0, 0.0, ""
    if hist is None or hist.empty:
        return 0.0, 0.0, ""
    d = hist[hist["ts_code"].map(sim._normalize_symbol) == sim._normalize_symbol(symbol)].copy()
    if d.empty:
        return 0.0, 0.0, ""
    d = d.sort_values("trade_date").reset_index(drop=True)
    box = d.tail(31).iloc[:-1].copy() if len(d) >= 21 else d.tail(20).copy()
    if box.empty:
        return 0.0, 0.0, ""
    low = _safe_float(pd.to_numeric(box["low_price"], errors="coerce").min(), 0.0)
    high = _safe_float(pd.to_numeric(box["high_price"], errors="coerce").max(), 0.0)
    return low, high, str(d.iloc[-1].get("trade_date") or trade_date)


def _tracking_bottom_range(row: pd.Series, cand: Dict[str, Any], symbol: str, trade_date: str) -> Tuple[float, float, str]:
    low = _safe_float(row.get("bottom_low"), 0.0)
    high = _safe_float(row.get("bottom_high"), 0.0)
    range_date = str(row.get("bottom_range_date") or "")
    if low > 0:
        return low, high, range_date
    cand_low, cand_high, cand_date = _candidate_bottom_range(cand)
    if cand_low > 0:
        return cand_low, cand_high, cand_date
    return _history_bottom_range(symbol, trade_date)


def _prepared_candidates(candidates_df: pd.DataFrame) -> pd.DataFrame:
    if candidates_df is None or candidates_df.empty:
        return pd.DataFrame()
    work = candidates_df.copy()
    for col in [
        "sector_rank",
        "score",
        "amount",
        "bottom_turn_score",
        "reversal_signal_score",
        "anti_chase_flag",
        "right_confirm",
        "ret20",
        "ret60",
        "drawdown_120d_high",
        "position_pct_120d",
    ]:
        if col not in work.columns:
            work[col] = 0
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0)
    work["symbol"] = work["symbol"].map(sim._normalize_symbol)
    work["sector_rank"] = work["sector_rank"].replace(0, 999)
    return work


def _buy_ready_candidates(candidates_df: pd.DataFrame) -> pd.DataFrame:
    work = _prepared_candidates(candidates_df)
    if work.empty:
        return work
    ready = work[
        (work["bottom_turn_score"] >= sim.V2_BOTTOM_BUY_SCORE)
        & (work["reversal_signal_score"] >= sim.V2_MIN_REVERSAL_SIGNAL_SCORE)
        & (work["anti_chase_flag"] == 0)
        & (work["right_confirm"] == 1)
        & (work["sector_rank"] <= sim.V2_SECTOR_BUY_RANK_LIMIT)
    ].copy()
    return ready.sort_values(
        ["sector_rank", "bottom_turn_score", "reversal_signal_score", "amount"],
        ascending=[True, False, False, False],
    )


def _tracking_actions(active_df: pd.DataFrame, candidates_df: pd.DataFrame, trade_date: str) -> List[Dict[str, Any]]:
    if active_df is None or active_df.empty:
        return []
    status = active_df["status"].astype(str) if "status" in active_df.columns else pd.Series(dtype=str)
    rec_type = active_df["recommendation_type"].astype(str) if "recommendation_type" in active_df.columns else pd.Series("", index=active_df.index)
    active_df = active_df[status.isin(["bought", "half_taken"]) | ((rec_type == "buy") & ~status.isin(["exited", "watching"]))].copy()
    if active_df.empty:
        return []
    candidate_map = _candidate_row_map(candidates_df)
    buy_ready_symbols = set(_buy_ready_candidates(candidates_df).get("symbol", pd.Series(dtype=str)).tolist())
    symbols = [sim._normalize_symbol(x) for x in active_df["symbol"].tolist()]
    price_map = sim._fetch_price_snapshot(symbols, trade_date)
    actions: List[Dict[str, Any]] = []
    for _, row in active_df.iterrows():
        symbol = sim._normalize_symbol(row.get("symbol", ""))
        if not symbol:
            continue
        status = str(row.get("status") or "")
        if status not in {"bought", "half_taken"} and str(row.get("recommendation_type") or "") == "buy":
            status = "bought"
        price_info = price_map.get(symbol, {})
        close = _safe_float(price_info.get("close"), _safe_float(row.get("entry_price"), 0.0))
        entry = _safe_float(row.get("entry_price"), close)
        stop = _safe_float(row.get("stop_price"), 0.0)
        gain = (close / entry - 1.0) if entry > 0 and close > 0 else 0.0
        cand = candidate_map.get(symbol, {})
        pattern_text = f"{cand.get('pattern', '')} {cand.get('ma_trend', '')}"
        take_profit_count = int(_safe_float(row.get("take_profit_count"), 0.0))
        add_count = int(_safe_float(row.get("add_count"), 0.0))
        weak_count = int(_safe_float(row.get("weak_count"), 0.0))
        bottom_low, bottom_high, bottom_range_date = _tracking_bottom_range(row, cand, symbol, trade_date)
        ma5 = _safe_float(cand.get("ma5"), 0.0)
        ma10 = _safe_float(cand.get("ma10"), 0.0)
        ma20 = _safe_float(cand.get("ma20"), 0.0)
        ma60 = _safe_float(cand.get("ma60"), 0.0)
        noise_bearish = _is_noise_bearish(pattern_text)
        hard_bearish = _is_hard_bearish(pattern_text) and not noise_bearish
        bottom_break = bottom_low > 0 and close < bottom_low * BOTTOM_BREAK_BUFFER
        stop_break = stop > 0 and close < stop
        below_ma10 = ma10 > 0 and close < ma10
        below_ma20 = ma20 > 0 and close < ma20
        bear_ma_stack = ma5 > 0 and ma20 > 0 and ma60 > 0 and ma5 < ma20 < ma60
        next_weak_count = weak_count + 1 if below_ma10 or below_ma20 else 0

        action = "hold"
        next_status = status
        next_take_profit_count = take_profit_count
        next_add_count = add_count
        reason = "未跌破底部区间或动态止损，继续跟踪。"

        if bottom_break:
            action = "exit"
            next_status = "exited"
            reason = f"收盘价 {_fmt_num(close)} 跌破底部区间下沿 {_fmt_num(bottom_low)}，底部转折失败，全部出场。"
        elif stop_break:
            action = "exit"
            next_status = "exited"
            reason = f"收盘价 {_fmt_num(close)} 跌破动态止损位 {_fmt_num(stop)}，全部出场。"
        elif bear_ma_stack and below_ma20:
            action = "exit"
            next_status = "exited"
            reason = "均线进入空头排列且收盘跌破MA20，底部修复失败，全部出场。"
        elif take_profit_count <= 0 and gain >= TAKE_PROFIT_HALF_PCT:
            action = "take_half"
            next_status = "half_taken"
            next_take_profit_count = 1
            reason = f"相对推荐价涨幅达到 {_fmt_pct(gain)}，第一次止盈一半。"
        elif take_profit_count >= 1 and gain >= TAKE_PROFIT_EXIT_PCT:
            action = "exit"
            next_status = "exited"
            reason = f"相对推荐价涨幅达到 {_fmt_pct(gain)}，第二次止盈全部出场。"
        elif status == "bought" and take_profit_count <= 0 and (next_weak_count >= 2 or (hard_bearish and below_ma20)):
            action = "take_half"
            next_status = "half_taken"
            next_take_profit_count = 1
            reason = "未跌破底部区间，但连续走弱或跌破MA20，先卖出一半，保留底部转折仓位。"
        elif status == "bought" and add_count <= 0 and symbol in buy_ready_symbols and not noise_bearish and not below_ma20:
            action = "add"
            next_status = "bought"
            next_add_count = 1
            reason = "既有推荐再度满足右侧底部转折买入口径，执行一次加仓；后续不再重复加仓。"
        elif noise_bearish:
            reason = "出现假突破/诱多等短线噪音，但未跌破底部区间，继续持有观察。"
        elif below_ma10 or below_ma20:
            reason = "价格回落到短中期均线下方，但底部区间未破，继续持有并累计弱势天数。"

        actions.append(
            {
                "symbol": symbol,
                "name": str(row.get("name") or price_info.get("name") or ""),
                "status": status,
                "action": action,
                "next_status": next_status,
                "close": close,
                "entry_price": entry,
                "stop_price": stop,
                "bottom_low": bottom_low,
                "bottom_high": bottom_high,
                "bottom_range_date": bottom_range_date,
                "gain": gain,
                "take_profit_count": take_profit_count,
                "next_take_profit_count": next_take_profit_count,
                "add_count": add_count,
                "next_add_count": next_add_count,
                "weak_count": weak_count,
                "next_weak_count": next_weak_count,
                "reason": reason,
            }
        )
    return actions

def _weakness_score(row: pd.Series, price_map: Dict[str, Dict[str, Any]]) -> float:
    symbol = sim._normalize_symbol(row.get("symbol", ""))
    close = _safe_float(price_map.get(symbol, {}).get("close"), _safe_float(row.get("entry_price"), 0.0))
    entry = _safe_float(row.get("entry_price"), close)
    gain = (close / entry - 1.0) if entry > 0 and close > 0 else 0.0
    score = _safe_float(row.get("score"), 0.0)
    sector_rank = _safe_float(row.get("sector_rank"), 999)
    return gain * 100.0 + score * 0.05 - sector_rank


def _forced_weak_exits(
    active_df: pd.DataFrame,
    trade_date: str,
    incoming_buy_count: int = 0,
    excluded_symbols: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    if active_df is None or active_df.empty:
        return []
    excluded_symbols = excluded_symbols or set()
    bought_df = active_df[active_df["status"].isin(["bought", "half_taken"])].copy()
    bought_df["symbol"] = bought_df["symbol"].map(sim._normalize_symbol)
    bought_df = bought_df[~bought_df["symbol"].isin(excluded_symbols)].copy()
    exit_count = len(bought_df) + max(0, incoming_buy_count) - MAX_BUY_TRACKING
    if exit_count <= 0:
        return []
    symbols = [sim._normalize_symbol(x) for x in bought_df["symbol"].tolist()]
    price_map = sim._fetch_price_snapshot(symbols, trade_date)
    bought_df["weakness_score"] = bought_df.apply(lambda r: _weakness_score(r, price_map), axis=1)
    weakest = bought_df.sort_values("weakness_score", ascending=True).head(exit_count)
    out: List[Dict[str, Any]] = []
    for _, row in weakest.iterrows():
        symbol = sim._normalize_symbol(row.get("symbol", ""))
        out.append(
            {
                "symbol": symbol,
                "name": str(row.get("name") or ""),
                "status": str(row.get("status") or ""),
                "action": "exit",
                "next_status": "exited",
                "close": _safe_float(price_map.get(symbol, {}).get("close"), 0.0),
                "entry_price": _safe_float(row.get("entry_price"), 0.0),
                "stop_price": _safe_float(row.get("stop_price"), 0.0),
                "gain": 0.0,
                "take_profit_count": int(_safe_float(row.get("take_profit_count"), 0.0)),
                "next_take_profit_count": int(_safe_float(row.get("take_profit_count"), 0.0)),
                "add_count": int(_safe_float(row.get("add_count"), 0.0)),
                "next_add_count": int(_safe_float(row.get("add_count"), 0.0)),
                "weak_count": int(_safe_float(row.get("weak_count"), 0.0)),
                "next_weak_count": int(_safe_float(row.get("weak_count"), 0.0)),
                "bottom_low": _safe_float(row.get("bottom_low"), 0.0),
                "bottom_high": _safe_float(row.get("bottom_high"), 0.0),
                "bottom_range_date": str(row.get("bottom_range_date") or ""),
                "reason": f"模型持有数超过 {MAX_BUY_TRACKING}，按弱度排序退出。",
            }
        )
    return out


def _select_new_recommendations(
    candidates_df: pd.DataFrame,
    regime: Dict[str, Any],
    active_df: pd.DataFrame,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if candidates_df is None or candidates_df.empty:
        return [], []
    work = _prepared_candidates(candidates_df)
    active_symbols = _active_bought_symbols(active_df)
    buy_slots = max(0, int(regime.get("buy_slots", 0) or 0))

    buy_df = _buy_ready_candidates(work)
    buy_df = buy_df[~buy_df["symbol"].isin(active_symbols)].head(buy_slots)
    buy_symbols = set(buy_df["symbol"].tolist())
    watch_pool = work[
        (~work["symbol"].isin(active_symbols))
        & (~work["symbol"].isin(buy_symbols))
        & (work["sector_rank"].replace(0, 999) <= sim.V2_SECTOR_WATCH_RANK_LIMIT)
    ].copy()
    watch_pool["sector_rank"] = watch_pool["sector_rank"].replace(0, 999)
    watch_pool = watch_pool[watch_pool["bottom_turn_score"] >= sim.V2_BOTTOM_WATCH_SCORE].copy()
    watch_df = watch_pool.sort_values(
        ["anti_chase_flag", "sector_rank", "bottom_turn_score", "reversal_signal_score", "amount"],
        ascending=[True, True, False, False, False],
    ).head(WATCH_LIMIT)

    return [_candidate_to_recommendation(row, "buy") for _, row in buy_df.iterrows()], [
        _candidate_to_recommendation(row, "watch") for _, row in watch_df.iterrows()
    ]


def _candidate_to_recommendation(row: pd.Series, rec_type: str) -> Dict[str, Any]:
    stage_note = _candidate_stage_note(row)
    bottom_low, bottom_high, bottom_range_date = _candidate_bottom_range(row)
    return {
        "symbol": sim._normalize_symbol(row.get("symbol", "")),
        "name": str(row.get("name") or ""),
        "sector_name": str(row.get("industry") or ""),
        "recommendation_type": rec_type,
        "entry_price": _safe_float(row.get("close"), 0.0),
        "stop_price": _safe_float(row.get("stop_price"), 0.0),
        "score": _safe_float(row.get("score"), 0.0),
        "sector_rank": int(_safe_float(row.get("sector_rank"), 999)),
        "pattern": str(row.get("pattern") or ""),
        "ma_trend": str(row.get("ma_trend") or ""),
        "bottom_turn_score": _safe_float(row.get("bottom_turn_score"), 0.0),
        "bottom_stage_score": _safe_float(row.get("bottom_stage_score"), 0.0),
        "reversal_signal_score": _safe_float(row.get("reversal_signal_score"), 0.0),
        "anti_chase_flag": int(_safe_float(row.get("anti_chase_flag"), 0.0)),
        "anti_chase_reasons": str(row.get("anti_chase_reasons") or ""),
        "reversal_signal_desc": str(row.get("reversal_signal_desc") or ""),
        "ret20": _safe_float(row.get("ret20"), 0.0),
        "ret60": _safe_float(row.get("ret60"), 0.0),
        "drawdown_120d_high": _safe_float(row.get("drawdown_120d_high"), 0.0),
        "position_pct_120d": _safe_float(row.get("position_pct_120d"), 0.0),
        "bottom_low": bottom_low,
        "bottom_high": bottom_high,
        "bottom_range_date": bottom_range_date,
        "stage_note": stage_note,
        "notes": "模型买入跟踪" if rec_type == "buy" else "观察候选",
    }


def _candidate_stage_note(row: pd.Series) -> str:
    parts = [
        f"底部分{_safe_float(row.get('bottom_stage_score'), 0.0):.0f}",
        f"转折分{_safe_float(row.get('reversal_signal_score'), 0.0):.0f}",
        f"20日{_safe_float(row.get('ret20'), 0.0):+.1%}",
        f"60日{_safe_float(row.get('ret60'), 0.0):+.1%}",
        f"距高点{_safe_float(row.get('drawdown_120d_high'), 0.0):.1%}",
        f"120日位置{_safe_float(row.get('position_pct_120d'), 0.0):.0%}",
    ]
    reasons = str(row.get("anti_chase_reasons") or "").strip()
    if reasons:
        parts.append(f"追高排除:{reasons}")
    return "；".join(parts)


def collect_safe_stock_data(trade_date: Optional[str] = None) -> Dict[str, Any]:
    td = _normalize_trade_date(trade_date)
    regime = sim._get_csi500_regime(td)
    sectors = sim._get_v2_top_sectors(td, limit=sim.V2_SECTOR_WATCH_RANK_LIMIT)
    active_df = load_active_recommendations()
    current_positions = {
        sim._normalize_symbol(row.get("symbol", "")): {
            "symbol": sim._normalize_symbol(row.get("symbol", "")),
            "name": str(row.get("name") or ""),
            "quantity": 1.0,
            "avg_cost": _safe_float(row.get("entry_price"), 0.0),
        }
        for _, row in active_df.iterrows()
        if str(row.get("status") or "") in {"bought", "half_taken"} and sim._normalize_symbol(row.get("symbol", ""))
    } if active_df is not None and not active_df.empty else {}
    candidates_df, sector_notes = sim._build_candidate_pool_v2(td, current_positions)
    tracking = _tracking_actions(active_df, candidates_df, td)
    buys, watches = _select_new_recommendations(candidates_df, regime, active_df)
    already_exiting = {x["symbol"] for x in tracking if x.get("action") == "exit"}
    forced_exits = _forced_weak_exits(active_df, td, incoming_buy_count=len(buys), excluded_symbols=already_exiting)
    forced_symbols = {x["symbol"] for x in forced_exits}
    tracking = [x for x in tracking if x["symbol"] not in forced_symbols] + forced_exits
    return {
        "trade_date": td,
        "regime": regime,
        "sectors": sectors,
        "sector_notes": sector_notes,
        "candidates": candidates_df,
        "active": active_df,
        "tracking": tracking,
        "buys": buys,
        "watches": watches,
    }


def _badge_class(value: Any) -> str:
    text_value = str(value or "")
    if text_value in {"持有", "观察"}:
        return "badge-hold"
    if text_value == "加仓":
        return "badge-add"
    if text_value == "卖出一半":
        return "badge-half"
    if text_value in {"全部出场", "已出场"}:
        return "badge-exit"
    return "badge-neutral"


def _render_table(
    headers: List[str],
    rows: List[List[Any]],
    table_class: str = "",
    wrap_cols: Optional[set[int]] = None,
    numeric_cols: Optional[set[int]] = None,
    badge_cols: Optional[set[int]] = None,
) -> str:
    if not rows:
        return "<div class='empty'>暂无符合条件的标的。</div>"
    wrap_cols = wrap_cols or set()
    numeric_cols = numeric_cols or set()
    badge_cols = badge_cols or set()
    head = "".join(
        f"<th class='{'wrap-text' if i in wrap_cols else 'nowrap'}{' num' if i in numeric_cols else ''}'>{_esc(h)}</th>"
        for i, h in enumerate(headers)
    )
    body = []
    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            cls = ["wrap-text" if i in wrap_cols else "nowrap"]
            if i in numeric_cols:
                cls.append("num")
            if i in badge_cols:
                content = f"<span class='badge {_badge_class(cell)}'>{_esc(cell)}</span>"
            else:
                content = _esc(cell)
            cells.append(f"<td class='{' '.join(cls)}'>{content}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    table_cls = f"report-table {table_class}".strip()
    return f"<div class='table-scroll'><table class='{_esc(table_cls)}'><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table></div>"


def draft_safe_stock_report(data: Dict[str, Any]) -> str:
    td = str(data.get("trade_date") or "")
    regime = data.get("regime") or {}
    sectors = data.get("sectors") or []
    buys = data.get("buys") or []
    watches = data.get("watches") or []
    tracking = data.get("tracking") or []
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    market_note = _market_style_commentary(sectors, buys, watches)

    sector_rows = [
        [
            int(s.get("rank") or i + 1),
            s.get("industry", ""),
            s.get("sector_type", ""),
            _fmt_num(s.get("score"), 2),
            _fmt_num(s.get("improvement"), 4),
            int(_safe_float(s.get("positive_days"), 0)),
            f"{_safe_float(s.get('recent_pct_change'), 0.0):+.2f}%",
        ]
        for i, s in enumerate(sectors)
    ]
    buy_rows = [
        [
            x["symbol"],
            x["name"],
            x["sector_name"],
            x["sector_rank"],
            _fmt_num(x["entry_price"]),
            f"底部转折{_fmt_num(x.get('bottom_turn_score'), 1)}；风控{_fmt_num(x['stop_price'])}；{x.get('stage_note', '')}；信号:{x.get('reversal_signal_desc') or x['pattern'][:40]}",
        ]
        for x in buys
    ]
    watch_rows = [
        [
            x["symbol"],
            x["name"],
            x["sector_name"],
            x["sector_rank"],
            _fmt_num(x["entry_price"]),
            f"底部转折{_fmt_num(x.get('bottom_turn_score'), 1)}；风控{_fmt_num(x['stop_price'])}；{x.get('stage_note', '')}；信号:{x.get('reversal_signal_desc') or x['pattern'][:40]}",
        ]
        for x in watches
    ]
    tracking_rows = [
        [
            x["symbol"],
            x["name"],
            _status_label(x["status"]),
            _action_label(x["action"]),
            _fmt_num(x["close"]),
            _fmt_pct(x["gain"]),
            f"底部下沿{_fmt_num(x.get('bottom_low'), 2)}；{x['reason']}",
        ]
        for x in tracking
    ]

    html_doc = f"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <style>
    body {{ margin:0; background:#07101d; color:#e5edf7; font-family:Arial,'Microsoft YaHei',sans-serif; }}
    .wrap {{ max-width:1360px; margin:0 auto; padding:30px 34px; }}
    .hero {{ border:1px solid rgba(148,163,184,.22); background:linear-gradient(135deg,#0f1b2d,#101827); padding:22px 24px; border-radius:10px; box-shadow:0 18px 50px rgba(0,0,0,.22); }}
    h1 {{ margin:0 0 8px; font-size:26px; white-space:nowrap; letter-spacing:0; }}
    h2 {{ margin:30px 0 12px; font-size:19px; color:#f8fafc; white-space:nowrap; letter-spacing:0; display:flex; align-items:center; gap:10px; }}
    h2::before {{ content:""; display:inline-block; width:4px; height:18px; border-radius:2px; background:#38bdf8; }}
    .muted {{ color:#94a3b8; font-size:13px; line-height:1.6; }}
    .gate {{ display:inline-block; padding:6px 10px; border-radius:6px; font-weight:700; }}
    .gate.open {{ background:#064e3b; color:#bbf7d0; }}
    .gate.cautious {{ background:#713f12; color:#fde68a; }}
    .gate.blocked {{ background:#7f1d1d; color:#fecaca; }}
    .table-scroll {{ width:100%; overflow-x:auto; border:1px solid rgba(148,163,184,.20); background:#0b1626; }}
    table.report-table {{ width:100%; min-width:1040px; border-collapse:collapse; table-layout:auto; }}
    th,td {{ border-bottom:1px solid rgba(148,163,184,.13); padding:12px 14px; text-align:center; font-size:13px; vertical-align:middle; line-height:1.45; }}
    th {{ color:#bfdbfe; background:#111f33; font-weight:700; }}
    tbody tr:nth-child(even) {{ background:rgba(15,23,42,.32); }}
    tbody tr:hover {{ background:rgba(56,189,248,.08); }}
    .nowrap {{ white-space:nowrap; word-break:keep-all; }}
    .wrap-text {{ white-space:normal; word-break:normal; min-width:360px; line-height:1.55; text-align:center; }}
    .num {{ text-align:center; font-variant-numeric:tabular-nums; }}
    .table-sectors .wrap-text {{ min-width:220px; }}
    .table-picks .wrap-text {{ min-width:560px; }}
    .table-tracking .wrap-text {{ min-width:520px; }}
    .badge {{ display:inline-flex; align-items:center; padding:3px 8px; border-radius:999px; font-size:12px; font-weight:700; line-height:1.2; white-space:nowrap; }}
    .badge-hold {{ background:rgba(34,197,94,.14); color:#bbf7d0; }}
    .badge-add {{ background:rgba(56,189,248,.14); color:#bae6fd; }}
    .badge-half {{ background:rgba(245,158,11,.16); color:#fde68a; }}
    .badge-exit {{ background:rgba(248,113,113,.16); color:#fecaca; }}
    .badge-neutral {{ background:rgba(148,163,184,.16); color:#cbd5e1; }}
    .market-note {{ margin-top:14px; max-width:980px; color:#dbeafe; font-size:14px; line-height:1.75; }}
    .empty {{ padding:14px; background:#0b1626; border:1px solid rgba(148,163,184,.18); color:#94a3b8; }}
    .risk {{ color:#cbd5e1; font-size:12px; line-height:1.7; border-top:1px solid rgba(148,163,184,.18); margin-top:24px; padding-top:16px; }}
  </style>
</head>
<body>
<div class="wrap">
  <section class="hero">
    <h1>小爱抄底选股晚报</h1>
    <div class="muted">交易日：{_esc(td)} · 生成时间：{_esc(generated_at)}</div>
    <div class="market-note">{_esc(market_note)}</div>
  </section>

  <h2>资金回流</h2>
  {_render_table(["排名", "板块", "类型", "分数", "资金改善", "流入天数", "近窗涨幅"], sector_rows, table_class="table-sectors", numeric_cols={0, 3, 4, 5, 6})}

  <h2>可买标的</h2>
  {_render_table(["代码", "名称", "板块", "板块排名", "价格", "信号/说明"], buy_rows, table_class="table-picks", wrap_cols={5}, numeric_cols={3, 4})}

  <h2>观察标的</h2>
  {_render_table(["代码", "名称", "板块", "板块排名", "价格", "信号/说明"], watch_rows, table_class="table-picks", wrap_cols={5}, numeric_cols={3, 4})}

  <h2>已买跟踪</h2>
  {_render_table(["代码", "名称", "当前状态", "动作", "现价", "收益", "原因"], tracking_rows, table_class="table-tracking", wrap_cols={6}, numeric_cols={4, 5}, badge_cols={2, 3})}

  <div class="risk">
    本报告为模型跟踪信号，不构成个性化投资建议。底部转折策略以底部区间失效作为核心退出条件；单日假突破等短线噪音不直接触发全部出场。
  </div>
</div>
</body>
</html>
"""
    return html_doc.strip()

def extract_summary_from_html(html_content: str) -> str:
    plain = html.unescape(str(html_content or ""))
    plain = " ".join(plain.replace("<", " <").split())
    return plain[:180] or "小爱抄底选股晚报"


def validate_safe_stock_report(html_content: str, data: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    text_value = str(html_content or "")
    for required in ["小爱抄底选股晚报", "资金回流", "可买标的", "观察标的", "已买跟踪"]:
        if required not in text_value:
            errors.append(f"missing section: {required}")
    if len(text_value) < 800:
        errors.append("report html too short")
    gate = str((data.get("regime") or {}).get("gate") or "")
    if gate == "blocked" and data.get("buys"):
        errors.append("blocked gate must not generate new buy recommendations")
    buy_slots = int(_safe_float((data.get("regime") or {}).get("buy_slots"), 0))
    if len(data.get("buys") or []) > buy_slots:
        errors.append("buy recommendations exceed gate slots")
    return errors


def persist_report_state(data: Dict[str, Any]) -> None:
    if sim.engine is None:
        raise ValueError("database engine is unavailable")
    ensure_safe_stock_tables()
    td = str(data.get("trade_date") or "")
    tracking = data.get("tracking") or []
    recs = list(data.get("buys") or []) + list(data.get("watches") or [])

    with sim.engine.begin() as conn:
        for item in tracking:
            conn.execute(
                text(
                    """
                    UPDATE safe_stock_recommendations
                    SET status=:status, last_report_date=:td, take_profit_count=:take_profit_count,
                        add_count=:add_count, weak_count=:weak_count,
                        bottom_low=IF(bottom_low > 0, bottom_low, :bottom_low),
                        bottom_high=IF(bottom_high > 0, bottom_high, :bottom_high),
                        bottom_range_date=IF(bottom_range_date <> '', bottom_range_date, :bottom_range_date),
                        exit_reason=:exit_reason, updated_at=CURRENT_TIMESTAMP
                    WHERE symbol=:symbol
                    """
                ),
                {
                    "symbol": item["symbol"],
                    "status": item["next_status"],
                    "td": td,
                    "take_profit_count": int(item.get("next_take_profit_count") or 0),
                    "add_count": int(item.get("next_add_count", item.get("add_count", 0)) or 0),
                    "weak_count": int(item.get("next_weak_count", item.get("weak_count", 0)) or 0),
                    "bottom_low": _safe_float(item.get("bottom_low"), 0.0),
                    "bottom_high": _safe_float(item.get("bottom_high"), 0.0),
                    "bottom_range_date": str(item.get("bottom_range_date") or ""),
                    "exit_reason": item.get("reason", ""),
                },
            )
        for item in recs:
            status = "bought" if item["recommendation_type"] == "buy" else "watching"
            conn.execute(
                text(
                    """
                    INSERT INTO safe_stock_recommendations (
                        symbol, name, sector_name, recommendation_type, status, first_signal_date,
                        last_report_date, entry_price, stop_price, score, sector_rank, add_count,
                        weak_count, bottom_low, bottom_high, bottom_range_date, notes
                    ) VALUES (
                        :symbol, :name, :sector_name, :recommendation_type, :status, :td,
                        :td, :entry_price, :stop_price, :score, :sector_rank, 0,
                        0, :bottom_low, :bottom_high, :bottom_range_date, :notes
                    )
                    ON DUPLICATE KEY UPDATE
                        name=VALUES(name),
                        sector_name=VALUES(sector_name),
                        recommendation_type=VALUES(recommendation_type),
                        status=VALUES(status),
                        last_report_date=VALUES(last_report_date),
                        entry_price=IF(entry_price > 0, entry_price, VALUES(entry_price)),
                        stop_price=VALUES(stop_price),
                        score=VALUES(score),
                        sector_rank=VALUES(sector_rank),
                        add_count=IF(VALUES(status)='bought' AND status='watching', 0, add_count),
                        weak_count=IF(VALUES(status)='bought' AND status='watching', 0, weak_count),
                        bottom_low=IF(bottom_low > 0, bottom_low, VALUES(bottom_low)),
                        bottom_high=IF(bottom_high > 0, bottom_high, VALUES(bottom_high)),
                        bottom_range_date=IF(bottom_range_date <> '', bottom_range_date, VALUES(bottom_range_date)),
                        notes=VALUES(notes),
                        updated_at=CURRENT_TIMESTAMP
                    """
                ),
                {**item, "status": status, "td": td},
            )


def publish_safe_stock_report(html_content: str, data: Dict[str, Any]) -> Tuple[bool, Any]:
    errors = validate_safe_stock_report(html_content, data)
    if errors:
        return False, "; ".join(errors)
    ensure_safe_stock_tables()
    td = str(data.get("trade_date") or "")
    title = f"{td} 小爱抄底选股晚报"
    summary = extract_summary_from_html(html_content)
    success, result = sub_svc.publish_content(
        channel_code=CHANNEL_CODE,
        title=title,
        content=html_content,
        summary=summary,
    )
    if success:
        persist_report_state(data)
    return success, result


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Generate safe stock report")
    parser.add_argument("--trade-date", default=None, help="YYYYMMDD, default latest data date")
    parser.add_argument("--dry-run", action="store_true", help="write preview html only")
    parser.add_argument("--publish", action="store_true", help="publish to intelligence station")
    parser.add_argument("--preview-path", default=PREVIEW_PATH)
    args = parser.parse_args(argv)

    if not args.dry_run and not args.publish:
        args.dry_run = True

    data = collect_safe_stock_data(args.trade_date)
    report_html = draft_safe_stock_report(data)
    errors = validate_safe_stock_report(report_html, data)
    if errors:
        print("鎶ュ憡鏍￠獙澶辫触:", "; ".join(errors))
        return 2

    with open(args.preview_path, "w", encoding="utf-8") as f:
        f.write(report_html)
    print(f"棰勮鏂囦欢宸茬敓鎴? {args.preview_path}")
    print(f"鎬婚椄: {(data.get('regime') or {}).get('gate')} | 涔板叆: {len(data.get('buys') or [])} | 瑙傚療: {len(data.get('watches') or [])}")

    if args.publish:
        success, result = publish_safe_stock_report(report_html, data)
        if not success:
            print(f"鍙戝竷澶辫触: {result}")
            return 3
        print(f"鍙戝竷鎴愬姛锛宑ontent_id={result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
