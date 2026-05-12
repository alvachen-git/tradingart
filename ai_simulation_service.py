import json
import os
import random
import re
import uuid
from calendar import monthrange
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent
from sqlalchemy import text

from data_engine import engine, get_latest_data_date
from llm_compat import ChatTongyiCompat
from market_tools import get_finance_related_trends, get_market_snapshot
from news_tools import get_financial_news
from screener_tool import search_top_stocks
from fund_flow_tools import tool_get_retail_money_flow
from knowledge_tools import search_investment_knowledge


OFFICIAL_PORTFOLIO_ID = "official_cn_a_etf_v1"
OFFICIAL_PORTFOLIO_2_ID = "official_cn_a_etf_v2"
INITIAL_CAPITAL = 1_000_000.0
DEFAULT_LOT_SIZE = 100


DEFAULT_CONFIG: Dict[str, Any] = {
    "portfolio_id": OFFICIAL_PORTFOLIO_ID,
    "model_name": "qwen3.6-plus",
    "tool_scope": "all",
    "execution_mode": "close_t0",
    "cost_model": "off",
    "risk_mode": "soft",
    "max_positions": 10,
    "max_daily_trades": 5,
    "max_single_weight_soft": 0.20,
    "max_single_weight_hard": 0.30,
    "max_turnover_soft": 0.35,
    "max_turnover_hard": 0.60,
    "min_cash_ratio_soft": 0.10,
    "review_use_llm": 1,
    "is_active": 1,
}

DEFAULT_CONFIG_V2: Dict[str, Any] = {
    **DEFAULT_CONFIG,
    "portfolio_id": OFFICIAL_PORTFOLIO_2_ID,
}

V2_MIN_BREAKOUT_SCORE = 70.0
V2_BOTTOM_BUY_SCORE = 80.0
V2_BOTTOM_WATCH_SCORE = 55.0
V2_MIN_REVERSAL_SIGNAL_SCORE = 30.0
V2_SECTOR_BUY_RANK_LIMIT = 3
V2_SECTOR_WATCH_RANK_LIMIT = 5
V2_PULLBACK_NEAR_PCT = 0.02
V2_CHASE_LIMIT_PCT = 0.03
V2_MIN_TRADE_WEIGHT = 0.02
V2_WATCHLIST_LIMIT = 60


TOOLS_FOR_SIMULATION = [
    search_top_stocks,
    get_market_snapshot,
    get_finance_related_trends,
    get_financial_news,
    tool_get_retail_money_flow,
    search_investment_knowledge,
]


@dataclass
class SimulationContext:
    portfolio_id: str
    trade_date: str
    prev_trade_date: Optional[str]
    cash: float
    nav_prev: float
    current_positions: Dict[str, Dict[str, Any]]
    config: Dict[str, Any]


def ensure_ai_sim_tables() -> None:
    if engine is None:
        raise ValueError("鏁版嵁搴撹繛鎺ヤ笉鍙敤")

    ddl_list = [
        """
        CREATE TABLE IF NOT EXISTS ai_sim_config (
            portfolio_id VARCHAR(64) NOT NULL,
            model_name VARCHAR(64) NOT NULL,
            tool_scope VARCHAR(32) NOT NULL DEFAULT 'all',
            execution_mode VARCHAR(32) NOT NULL DEFAULT 'close_t0',
            cost_model VARCHAR(32) NOT NULL DEFAULT 'off',
            risk_mode VARCHAR(32) NOT NULL DEFAULT 'soft',
            max_positions INT NOT NULL DEFAULT 10,
            max_daily_trades INT NOT NULL DEFAULT 5,
            max_single_weight_soft DOUBLE NOT NULL DEFAULT 0.20,
            max_single_weight_hard DOUBLE NOT NULL DEFAULT 0.30,
            max_turnover_soft DOUBLE NOT NULL DEFAULT 0.35,
            max_turnover_hard DOUBLE NOT NULL DEFAULT 0.60,
            min_cash_ratio_soft DOUBLE NOT NULL DEFAULT 0.10,
            is_active TINYINT NOT NULL DEFAULT 1,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (portfolio_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_sim_orders (
            id BIGINT NOT NULL AUTO_INCREMENT,
            portfolio_id VARCHAR(64) NOT NULL,
            trade_date VARCHAR(16) NOT NULL,
            order_id VARCHAR(64) NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            side VARCHAR(16) NOT NULL,
            target_weight DOUBLE DEFAULT 0,
            reason_short VARCHAR(255) DEFAULT '',
            reason_detail TEXT,
            confidence DOUBLE DEFAULT 0,
            gate_status VARCHAR(32) DEFAULT 'pending',
            gate_notes TEXT,
            raw_model_output_json LONGTEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_ai_sim_order_id (order_id),
            KEY idx_ai_sim_orders_portfolio_date (portfolio_id, trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_sim_trades (
            id BIGINT NOT NULL AUTO_INCREMENT,
            portfolio_id VARCHAR(64) NOT NULL,
            trade_date VARCHAR(16) NOT NULL,
            trade_id VARCHAR(64) NOT NULL,
            order_id VARCHAR(64) DEFAULT '',
            symbol VARCHAR(32) NOT NULL,
            side VARCHAR(16) NOT NULL,
            quantity DOUBLE NOT NULL,
            price DOUBLE NOT NULL,
            amount DOUBLE NOT NULL,
            cost DOUBLE NOT NULL DEFAULT 0,
            slippage DOUBLE NOT NULL DEFAULT 0,
            exec_mode VARCHAR(32) NOT NULL DEFAULT 'close_t0',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_ai_sim_trade_id (trade_id),
            KEY idx_ai_sim_trades_portfolio_date (portfolio_id, trade_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_sim_positions (
            id BIGINT NOT NULL AUTO_INCREMENT,
            portfolio_id VARCHAR(64) NOT NULL,
            trade_date VARCHAR(16) NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            name VARCHAR(128) DEFAULT '',
            quantity DOUBLE NOT NULL DEFAULT 0,
            avg_cost DOUBLE NOT NULL DEFAULT 0,
            close_price DOUBLE NOT NULL DEFAULT 0,
            market_value DOUBLE NOT NULL DEFAULT 0,
            unrealized_pnl DOUBLE NOT NULL DEFAULT 0,
            weight DOUBLE NOT NULL DEFAULT 0,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY idx_ai_sim_positions_portfolio_date (portfolio_id, trade_date),
            UNIQUE KEY uq_ai_sim_positions_snapshot (portfolio_id, trade_date, symbol)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_sim_nav_daily (
            id BIGINT NOT NULL AUTO_INCREMENT,
            portfolio_id VARCHAR(64) NOT NULL,
            trade_date VARCHAR(16) NOT NULL,
            cash DOUBLE NOT NULL,
            position_value DOUBLE NOT NULL,
            nav DOUBLE NOT NULL,
            daily_return DOUBLE NOT NULL,
            cum_return DOUBLE NOT NULL,
            max_drawdown DOUBLE NOT NULL,
            turnover DOUBLE NOT NULL,
            bench_hs300 DOUBLE NOT NULL,
            bench_zz1000 DOUBLE NOT NULL,
            alpha_vs_hs300 DOUBLE NOT NULL,
            alpha_vs_zz1000 DOUBLE NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_ai_sim_nav_portfolio_date (portfolio_id, trade_date),
            KEY idx_ai_sim_nav_portfolio (portfolio_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_sim_review_daily (
            id BIGINT NOT NULL AUTO_INCREMENT,
            portfolio_id VARCHAR(64) NOT NULL,
            trade_date VARCHAR(16) NOT NULL,
            summary_md TEXT,
            buys_md TEXT,
            sells_md TEXT,
            risk_md TEXT,
            next_watchlist_json LONGTEXT,
            model_name VARCHAR(64) DEFAULT '',
            tool_calls_json LONGTEXT,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_ai_sim_review_portfolio_date (portfolio_id, trade_date),
            KEY idx_ai_sim_review_portfolio (portfolio_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS ai_sim_watchlist (
            portfolio_id VARCHAR(64) NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            name VARCHAR(128) DEFAULT '',
            status VARCHAR(16) NOT NULL DEFAULT 'watching',
            sector_name VARCHAR(128) DEFAULT '',
            score DOUBLE NOT NULL DEFAULT 0,
            breakout_date VARCHAR(16) DEFAULT '',
            breakout_price DOUBLE NOT NULL DEFAULT 0,
            stop_price DOUBLE NOT NULL DEFAULT 0,
            ma10 DOUBLE NOT NULL DEFAULT 0,
            ma20 DOUBLE NOT NULL DEFAULT 0,
            pullback_ready TINYINT NOT NULL DEFAULT 0,
            chase_ok TINYINT NOT NULL DEFAULT 1,
            has_platform TINYINT NOT NULL DEFAULT 0,
            platform_low DOUBLE NOT NULL DEFAULT 0,
            breakout_candle_low DOUBLE NOT NULL DEFAULT 0,
            last_signal_date VARCHAR(16) DEFAULT '',
            notes VARCHAR(255) DEFAULT '',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (portfolio_id, symbol),
            KEY idx_ai_sim_watchlist_portfolio_status (portfolio_id, status),
            KEY idx_ai_sim_watchlist_portfolio_signal_date (portfolio_id, last_signal_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]

    with engine.begin() as conn:
        for ddl in ddl_list:
            conn.execute(text(ddl))

        realized_pnl_col = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'ai_sim_trades'
                  AND COLUMN_NAME = 'realized_pnl'
                LIMIT 1
                """
            )
        ).fetchone()
        if not realized_pnl_col:
            conn.execute(
                text(
                    """
                    ALTER TABLE ai_sim_trades
                    ADD COLUMN realized_pnl DOUBLE NOT NULL DEFAULT 0 AFTER amount
                    """
                )
            )

        watchlist_columns = {
            "has_platform": "ALTER TABLE ai_sim_watchlist ADD COLUMN has_platform TINYINT NOT NULL DEFAULT 0 AFTER chase_ok",
            "platform_low": "ALTER TABLE ai_sim_watchlist ADD COLUMN platform_low DOUBLE NOT NULL DEFAULT 0 AFTER has_platform",
            "breakout_candle_low": "ALTER TABLE ai_sim_watchlist ADD COLUMN breakout_candle_low DOUBLE NOT NULL DEFAULT 0 AFTER platform_low",
            "notes": "ALTER TABLE ai_sim_watchlist ADD COLUMN notes VARCHAR(255) DEFAULT '' AFTER last_signal_date",
        }
        for col_name, ddl in watchlist_columns.items():
            col_row = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'ai_sim_watchlist'
                      AND COLUMN_NAME = :col
                    LIMIT 1
                    """
                ),
                {"col": col_name},
            ).fetchone()
            if not col_row:
                conn.execute(text(ddl))

        upsert_sql = text(
            """
            INSERT INTO ai_sim_config (
                portfolio_id, model_name, tool_scope, execution_mode, cost_model, risk_mode,
                max_positions, max_daily_trades, max_single_weight_soft, max_single_weight_hard,
                max_turnover_soft, max_turnover_hard, min_cash_ratio_soft, is_active
            ) VALUES (
                :portfolio_id, :model_name, :tool_scope, :execution_mode, :cost_model, :risk_mode,
                :max_positions, :max_daily_trades, :max_single_weight_soft, :max_single_weight_hard,
                :max_turnover_soft, :max_turnover_hard, :min_cash_ratio_soft, :is_active
            )
            ON DUPLICATE KEY UPDATE
                model_name = VALUES(model_name),
                tool_scope = VALUES(tool_scope),
                execution_mode = VALUES(execution_mode),
                cost_model = VALUES(cost_model),
                risk_mode = VALUES(risk_mode),
                max_positions = VALUES(max_positions),
                max_daily_trades = VALUES(max_daily_trades),
                max_single_weight_soft = VALUES(max_single_weight_soft),
                max_single_weight_hard = VALUES(max_single_weight_hard),
                max_turnover_soft = VALUES(max_turnover_soft),
                max_turnover_hard = VALUES(max_turnover_hard),
                min_cash_ratio_soft = VALUES(min_cash_ratio_soft),
                is_active = VALUES(is_active)
            """
        )
        conn.execute(upsert_sql, DEFAULT_CONFIG)
        conn.execute(upsert_sql, DEFAULT_CONFIG_V2)


def _normalize_trade_date(trade_date: Optional[str]) -> str:
    if not trade_date:
        trade_date = str(get_latest_data_date())
    normalized = re.sub(r"[^0-9]", "", str(trade_date))[:8]
    if len(normalized) != 8:
        raise ValueError(f"trade_date 闈炴硶: {trade_date}")
    return normalized


def _normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper().replace(" ", "")
    if not raw:
        return ""

    raw = raw.replace(".XSHG", ".SH").replace(".XSHE", ".SZ")
    m = re.match(r"^(\d{6})(?:\.(SH|SZ|BJ))?$", raw)
    if m:
        code = m.group(1)
        suffix = m.group(2)
        if suffix:
            return f"{code}.{suffix}"
        if code.startswith(("6", "5", "9")):
            return f"{code}.SH"
        if code.startswith(("0", "1", "2", "3")):
            return f"{code}.SZ"
        return f"{code}.BJ"
    return raw


def _is_etf_symbol(symbol: str, name: str = "") -> bool:
    code = symbol.split(".")[0]
    n = str(name or "").upper()
    return code.startswith(("510", "159", "588")) or "ETF" in n


def _is_valid_universe_symbol(symbol: str, name: str = "") -> bool:
    code = symbol.split(".")[0]
    if re.match(r"^\d{6}\.(SH|SZ|BJ)$", symbol):
        return True
    if _is_etf_symbol(symbol, name):
        return True
    return False


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _load_config(portfolio_id: str) -> Dict[str, Any]:
    sql = text("SELECT * FROM ai_sim_config WHERE portfolio_id = :pid LIMIT 1")
    with engine.connect() as conn:
        row = conn.execute(sql, {"pid": portfolio_id}).mappings().fetchone()
    if not row:
        if str(portfolio_id or "").strip() == OFFICIAL_PORTFOLIO_2_ID:
            return dict(DEFAULT_CONFIG_V2)
        return dict(DEFAULT_CONFIG)
    config = dict(row)
    for k, v in DEFAULT_CONFIG.items():
        config.setdefault(k, v)
    return config


def _get_previous_nav_row(portfolio_id: str, trade_date: str) -> Optional[Dict[str, Any]]:
    sql = text(
        """
        SELECT *
        FROM ai_sim_nav_daily
        WHERE portfolio_id = :pid AND trade_date < :td
        ORDER BY trade_date DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"pid": portfolio_id, "td": trade_date}).mappings().fetchone()
    return dict(row) if row else None


def _load_previous_positions(
    portfolio_id: str, trade_date: str, prev_trade_date: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    target_trade_date = _normalize_trade_date(prev_trade_date) if prev_trade_date else ""
    if not target_trade_date:
        prev_nav_row = _get_previous_nav_row(portfolio_id, trade_date)
        target_trade_date = _normalize_trade_date(prev_nav_row.get("trade_date")) if prev_nav_row else ""
    if not target_trade_date:
        return {}

    sql = text(
        """
        SELECT *
        FROM ai_sim_positions
        WHERE portfolio_id = :pid AND trade_date = :td
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"pid": portfolio_id, "td": target_trade_date})

    out: Dict[str, Dict[str, Any]] = {}
    if df.empty:
        return out

    for _, r in df.iterrows():
        symbol = _normalize_symbol(r.get("symbol", ""))
        if not symbol:
            continue
        out[symbol] = {
            "symbol": symbol,
            "name": str(r.get("name") or ""),
            "quantity": _to_float(r.get("quantity")),
            "avg_cost": _to_float(r.get("avg_cost")),
        }
    return out


def _fetch_price_snapshot(symbols: List[str], trade_date: str) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}

    cleaned = sorted({_normalize_symbol(s) for s in symbols if _normalize_symbol(s)})
    if not cleaned:
        return {}

    placeholders = ",".join([f":s{i}" for i in range(len(cleaned))])
    params: Dict[str, Any] = {f"s{i}": s for i, s in enumerate(cleaned)}
    params["td"] = trade_date

    sql = text(
        f"""
        SELECT s.ts_code, s.name, s.close_price, s.amount, s.vol
        FROM stock_price s
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_td
            FROM stock_price
            WHERE trade_date <= :td
              AND ts_code IN ({placeholders})
            GROUP BY ts_code
        ) t ON t.ts_code = s.ts_code AND t.max_td = s.trade_date
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    price_map: Dict[str, Dict[str, Any]] = {}
    if df.empty:
        return price_map

    for _, r in df.iterrows():
        symbol = _normalize_symbol(r.get("ts_code", ""))
        if not symbol:
            continue
        price_map[symbol] = {
            "symbol": symbol,
            "name": str(r.get("name") or ""),
            "close": _to_float(r.get("close_price")),
            "amount": _to_float(r.get("amount")),
            "vol": _to_float(r.get("vol")),
        }
    return price_map


def _latest_screener_date(trade_date: str) -> Optional[str]:
    sql = text(
        """
        SELECT MAX(trade_date) AS d
        FROM daily_stock_screener
        WHERE trade_date <= :td
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"td": trade_date}).fetchone()
    if not row or not row[0]:
        return None
    return re.sub(r"[^0-9]", "", str(row[0]))[:8]


def _build_candidate_pool(trade_date: str, current_positions: Dict[str, Dict[str, Any]], limit: int = 120) -> pd.DataFrame:
    candidates_date = _latest_screener_date(trade_date)

    if candidates_date:
        sql = text(
            """
            SELECT ts_code, name, industry, score, close, pct_chg
            FROM daily_stock_screener
            WHERE trade_date = :td
            ORDER BY score DESC
            LIMIT 350
            """
        )
        with engine.connect() as conn:
            base_df = pd.read_sql(sql, conn, params={"td": candidates_date})
    else:
        base_df = pd.DataFrame()

    if base_df.empty:
        sql = text(
            """
            SELECT ts_code, name, NULL AS industry, 0 AS score, close_price AS close, 0 AS pct_chg
            FROM stock_price
            WHERE trade_date = :td
            ORDER BY amount DESC
            LIMIT 300
            """
        )
        with engine.connect() as conn:
            base_df = pd.read_sql(sql, conn, params={"td": trade_date})

    if base_df.empty:
        return pd.DataFrame(columns=["symbol", "name", "industry", "score", "close", "pct_chg", "amount", "vol", "from_holdings_fallback"])

    base_df["symbol"] = base_df["ts_code"].apply(_normalize_symbol)
    base_df = base_df[base_df["symbol"].astype(bool)]

    all_symbols = sorted(set(base_df["symbol"].tolist()) | set(current_positions.keys()))
    price_map = _fetch_price_snapshot(all_symbols, trade_date)

    rows: List[Dict[str, Any]] = []
    for _, r in base_df.iterrows():
        symbol = r["symbol"]
        price_info = price_map.get(symbol)
        if not price_info:
            continue

        name = str(r.get("name") or price_info.get("name") or "")
        if "ST" in name.upper() or "*ST" in name.upper() or "閫€" in name:
            continue
        if not _is_valid_universe_symbol(symbol, name):
            continue

        close = _to_float(price_info.get("close"), _to_float(r.get("close")))
        amount = _to_float(price_info.get("amount"), 0.0)
        if close <= 0:
            continue

        # 鍊欓€夋睜杩囨护锛氭祦鍔ㄦ€с€佹瀬绔綆浠蜂繚鎶?
        if amount < 1e8 and symbol not in current_positions:
            continue
        if close < 1.0:
            continue

        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "industry": str(r.get("industry") or ""),
                "score": _to_float(r.get("score"), 0.0),
                "close": close,
                "pct_chg": _to_float(r.get("pct_chg"), 0.0),
                "amount": amount,
                "vol": _to_float(price_info.get("vol"), 0.0),
                "from_holdings_fallback": 0,
            }
        )

    # 鍊欓€夋睜杩囦弗鏃讹紝闄嶇骇鏀惧娴佸姩鎬ч棬妲涳紝闃叉闀挎湡绌轰粨
    if not rows:
        for _, r in base_df.sort_values(["score"], ascending=[False]).head(limit * 2).iterrows():
            symbol = r["symbol"]
            price_info = price_map.get(symbol)
            if not price_info:
                continue
            name = str(r.get("name") or price_info.get("name") or "")
            if "ST" in name.upper() or "*ST" in name.upper() or "閫€" in name:
                continue
            if not _is_valid_universe_symbol(symbol, name):
                continue
            close = _to_float(price_info.get("close"), _to_float(r.get("close")))
            if close <= 0:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "industry": str(r.get("industry") or ""),
                    "score": _to_float(r.get("score"), 0.0),
                    "close": close,
                    "pct_chg": _to_float(r.get("pct_chg"), 0.0),
                    "amount": _to_float(price_info.get("amount"), 0.0),
                    "vol": _to_float(price_info.get("vol"), 0.0),
                    "from_holdings_fallback": 0,
                }
            )

    # 纭繚褰撳墠鎸佷粨涓嶄細琚繃婊ゆ帀锛堢敤浜庡噺浠?娓呬粨锛?
    for symbol, pos in current_positions.items():
        if any(x["symbol"] == symbol for x in rows):
            continue
        p = price_map.get(symbol)
        if not p:
            continue
        rows.append(
            {
                "symbol": symbol,
                "name": pos.get("name") or p.get("name") or "",
                "industry": "",
                "score": -1.0,
                "close": _to_float(p.get("close")),
                "pct_chg": 0.0,
                "amount": _to_float(p.get("amount"), 0.0),
                "vol": _to_float(p.get("vol"), 0.0),
                "from_holdings_fallback": 1,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["symbol", "name", "industry", "score", "close", "pct_chg", "amount", "vol", "from_holdings_fallback"])

    df = pd.DataFrame(rows).drop_duplicates(subset=["symbol"], keep="first")
    df = df.sort_values(["score", "amount"], ascending=[False, False]).head(limit).reset_index(drop=True)
    return df


def _is_v2_portfolio(portfolio_id: str) -> bool:
    return str(portfolio_id or "").strip() == OFFICIAL_PORTFOLIO_2_ID


def _latest_sector_date(trade_date: str) -> Optional[str]:
    sql = text(
        """
        SELECT MAX(trade_date) AS d
        FROM sector_moneyflow
        WHERE trade_date <= :td
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"td": trade_date}).fetchone()
    if not row or not row[0]:
        return None
    return re.sub(r"[^0-9]", "", str(row[0]))[:8]


def _get_v2_top_sectors(trade_date: str, limit: int = 3) -> List[Dict[str, Any]]:
    sec_date = _latest_sector_date(trade_date)
    if not sec_date:
        return []
    sql = text(
        """
        SELECT industry, sector_type, pct_change, main_net_inflow, medium_net_inflow,
               small_net_inflow, total_turnover, net_rate
        FROM sector_moneyflow
        WHERE trade_date = :td
          AND sector_type IN ('行业', '琛屼笟')
        ORDER BY pct_change DESC, main_net_inflow DESC, industry ASC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"td": sec_date, "lim": int(max(1, limit))})
    out: List[Dict[str, Any]] = []
    if df.empty:
        return out
    for _, row in df.iterrows():
        name = str(row.get("industry") or "").strip()
        if not name:
            continue
        main_flow = _to_float(row.get("main_net_inflow"), 0.0)
        pct_change = _to_float(row.get("pct_change"), 0.0)
        out.append(
            {
                "rank": int(len(out) + 1),
                "industry": name,
                "sector_type": str(row.get("sector_type") or "行业"),
                "score": pct_change,
                "improvement": main_flow,
                "positive_days": 1 if main_flow > 0 else 0,
                "recent_pct_change": pct_change,
                "pct_change": pct_change,
                "main_net_inflow": main_flow,
                "medium_net_inflow": _to_float(row.get("medium_net_inflow"), 0.0),
                "small_net_inflow": _to_float(row.get("small_net_inflow"), 0.0),
                "total_turnover": _to_float(row.get("total_turnover"), 0.0),
                "net_rate": _to_float(row.get("net_rate"), 0.0),
                "trade_date": sec_date,
            }
        )
    return out


def _fetch_recent_price_history(symbols: List[str], trade_date: str, lookback: int = 100) -> pd.DataFrame:
    cleaned = sorted({_normalize_symbol(s) for s in symbols if _normalize_symbol(s)})
    if not cleaned:
        return pd.DataFrame(
            columns=[
                "ts_code",
                "trade_date",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "amount",
                "vol",
            ]
        )

    placeholders = ",".join([f":s{i}" for i in range(len(cleaned))])
    params: Dict[str, Any] = {f"s{i}": s for i, s in enumerate(cleaned)}
    params["td"] = trade_date
    params["lb"] = int(max(30, lookback))

    sql = text(
        f"""
        SELECT ts_code, trade_date, open_price, high_price, low_price, close_price, amount, vol
        FROM (
            SELECT
                ts_code, trade_date, open_price, high_price, low_price, close_price, amount, vol,
                ROW_NUMBER() OVER(PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
            FROM stock_price
            WHERE trade_date <= :td
              AND ts_code IN ({placeholders})
        ) q
        WHERE q.rn <= :lb
        ORDER BY ts_code, trade_date
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def _is_v2_breakout_candidate(pattern: str, ma_trend: str, score: float) -> bool:
    p = str(pattern or "")
    t = str(ma_trend or "")
    if _to_float(score, 0.0) < V2_MIN_BREAKOUT_SCORE:
        return False

    bearish_tokens = ["跌破", "空头", "假突破", "假跌破", "诱多"]
    if any(tok in p for tok in bearish_tokens):
        return False
    if "空头" in t:
        return False

    positive_tokens = ["突破", "破底翻", "反转", "晨星", "锤子", "创新高", "多头吞噬"]
    return any(tok in p for tok in positive_tokens)


def _compute_v2_symbol_features(hist_df: pd.DataFrame) -> Dict[str, Any]:
    out = {
        "ma10": 0.0,
        "ma20": 0.0,
        "breakout_date": "",
        "breakout_price": 0.0,
        "breakout_candle_low": 0.0,
        "has_platform": 0,
        "platform_low": 0.0,
        "stop_price": 0.0,
        "pullback_ready": 0,
        "chase_ok": 1,
    }
    if hist_df.empty:
        return out

    d = hist_df.sort_values("trade_date").reset_index(drop=True)
    for c in ["open_price", "high_price", "low_price", "close_price"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["close_price", "high_price", "low_price"])
    if d.empty:
        return out

    last = d.iloc[-1]
    close_now = _to_float(last.get("close_price"), 0.0)
    ma10 = _to_float(d["close_price"].tail(10).mean(), 0.0)
    ma20 = _to_float(d["close_price"].tail(20).mean(), 0.0)
    out["ma10"] = ma10
    out["ma20"] = ma20

    breakout_date = re.sub(r"[^0-9]", "", str(last.get("trade_date") or ""))[:8]
    breakout_price = close_now
    breakout_candle_low = _to_float(last.get("low_price"), close_now)

    for i in range(max(1, len(d) - 12), len(d)):
        cur = d.iloc[i]
        prev = d.iloc[:i]
        if len(prev) < 20:
            continue
        prev_high = _to_float(prev["high_price"].tail(20).max(), 0.0)
        prev_close = _to_float(d.iloc[i - 1].get("close_price"), 0.0)
        cur_close = _to_float(cur.get("close_price"), 0.0)
        cur_open = _to_float(cur.get("open_price"), cur_close)
        day_ret = (cur_close / prev_close - 1.0) if prev_close > 0 else 0.0
        if cur_close > prev_high and cur_close > cur_open and day_ret >= 0.03:
            breakout_date = re.sub(r"[^0-9]", "", str(cur.get("trade_date") or ""))[:8]
            breakout_price = cur_close
            breakout_candle_low = _to_float(cur.get("low_price"), breakout_candle_low)

    box = d.tail(16).iloc[:-1].copy() if len(d) >= 12 else pd.DataFrame()
    has_platform = 0
    platform_low = 0.0
    if not box.empty:
        box_high = _to_float(box["high_price"].max(), 0.0)
        platform_low = _to_float(box["low_price"].min(), 0.0)
        width = (box_high - platform_low) / max(1e-9, close_now)
        if width <= 0.18:
            has_platform = 1
    out["has_platform"] = has_platform
    out["platform_low"] = platform_low
    out["breakout_date"] = breakout_date
    out["breakout_price"] = breakout_price
    out["breakout_candle_low"] = breakout_candle_low
    out["stop_price"] = platform_low if has_platform and platform_low > 0 else breakout_candle_low

    near_ma10 = ma10 > 0 and abs(close_now - ma10) / ma10 <= V2_PULLBACK_NEAR_PCT
    near_ma20 = ma20 > 0 and abs(close_now - ma20) / ma20 <= V2_PULLBACK_NEAR_PCT
    out["pullback_ready"] = int(near_ma10 or near_ma20)
    out["chase_ok"] = int(close_now <= max(1e-9, breakout_price) * (1.0 + V2_CHASE_LIMIT_PCT))
    return out


def _ensure_v2_candidate_columns(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "name",
                "industry",
                "sector_rank",
                "score",
                "close",
                "pct_chg",
                "amount",
                "vol",
                "pattern",
                "ma_trend",
                "signal_active",
                "breakout_date",
                "breakout_price",
                "stop_price",
                "ma10",
                "ma20",
                "pullback_ready",
                "chase_ok",
                "has_platform",
                "platform_low",
                "breakout_candle_low",
                "from_holdings_fallback",
            ]
        )

    out = df.copy()
    defaults: Dict[str, Any] = {
        "pattern": "",
        "ma_trend": "",
        "sector_rank": 999,
        "signal_active": 0,
        "breakout_date": trade_date,
        "breakout_price": out.get("close", 0),
        "stop_price": 0.0,
        "ma10": 0.0,
        "ma20": 0.0,
        "pullback_ready": 0,
        "chase_ok": 1,
        "has_platform": 0,
        "platform_low": 0.0,
        "breakout_candle_low": 0.0,
        "from_holdings_fallback": 0,
    }
    for col, dv in defaults.items():
        if col not in out.columns:
            out[col] = dv
    return out


def _apply_v2_sector_rank(df: pd.DataFrame, sector_rank_map: Dict[str, int]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()

    def _rank_for(row: pd.Series) -> int:
        current = _to_float(row.get("sector_rank"), 0.0)
        if current > 0:
            return int(current)
        industry = str(row.get("industry") or "").strip()
        return int(sector_rank_map.get(industry, 999))

    out["sector_rank"] = out.apply(_rank_for, axis=1)
    return out


def _build_candidate_pool_v2(
    trade_date: str,
    current_positions: Dict[str, Dict[str, Any]],
    limit: int = 120,
) -> Tuple[pd.DataFrame, List[str]]:
    screener_date = _latest_screener_date(trade_date)
    top_sectors = _get_v2_top_sectors(trade_date, limit=3)
    sector_names = [str(x.get("industry") or "").strip() for x in top_sectors if str(x.get("industry") or "").strip()]
    sector_rank_map = {name: i + 1 for i, name in enumerate(sector_names)}
    sector_notes = [
        f"{x['industry']}({x['pct_change']:+.2f}%)" for x in top_sectors if str(x.get("industry") or "").strip()
    ]

    base_df = pd.DataFrame()
    if screener_date:
        if sector_names:
            placeholders = ",".join([f":sec{i}" for i in range(len(sector_names))])
            params: Dict[str, Any] = {"td": screener_date}
            for i, sec in enumerate(sector_names):
                params[f"sec{i}"] = sec
            sql = text(
                f"""
                SELECT ts_code, name, industry, score, close, pct_chg, pattern, ma_trend
                FROM daily_stock_screener
                WHERE trade_date = :td
                  AND industry IN ({placeholders})
                ORDER BY score DESC
                LIMIT 600
                """
            )
            with engine.connect() as conn:
                base_df = pd.read_sql(sql, conn, params=params)
        else:
            sql = text(
                """
                SELECT ts_code, name, industry, score, close, pct_chg, pattern, ma_trend
                FROM daily_stock_screener
                WHERE trade_date = :td
                ORDER BY score DESC
                LIMIT 400
                """
            )
            with engine.connect() as conn:
                base_df = pd.read_sql(sql, conn, params={"td": screener_date})

    if base_df.empty:
        fallback = _build_candidate_pool(trade_date, current_positions, limit=limit)
        fallback = _apply_v2_sector_rank(fallback, sector_rank_map)
        return _ensure_v2_candidate_columns(fallback, trade_date), sector_notes

    base_df["symbol"] = base_df["ts_code"].apply(_normalize_symbol)
    base_df = base_df[base_df["symbol"].astype(bool)]

    all_symbols = sorted(set(base_df["symbol"].tolist()) | set(current_positions.keys()))
    price_map = _fetch_price_snapshot(all_symbols, trade_date)
    hist_df = _fetch_recent_price_history(all_symbols, trade_date, lookback=100)
    feature_map: Dict[str, Dict[str, Any]] = {}
    if not hist_df.empty:
        hist_df["symbol"] = hist_df["ts_code"].apply(_normalize_symbol)
        for symbol, g in hist_df.groupby("symbol"):
            feature_map[symbol] = _compute_v2_symbol_features(g)

    rows: List[Dict[str, Any]] = []
    for _, r in base_df.iterrows():
        symbol = _normalize_symbol(r.get("symbol", ""))
        if not symbol:
            continue
        p = price_map.get(symbol)
        if not p:
            continue
        name = str(r.get("name") or p.get("name") or "")
        if "ST" in name.upper() or "*ST" in name.upper() or "退" in name:
            continue
        if not _is_valid_universe_symbol(symbol, name):
            continue

        close = _to_float(p.get("close"), _to_float(r.get("close"), 0.0))
        amount = _to_float(p.get("amount"), 0.0)
        if close <= 0:
            continue
        if amount < 1e8 and symbol not in current_positions:
            continue
        if close < 1.0:
            continue

        score = _to_float(r.get("score"), 0.0)
        industry = str(r.get("industry") or "")
        pattern = str(r.get("pattern") or "")
        ma_trend = str(r.get("ma_trend") or "")
        signal_active = int(_is_v2_breakout_candidate(pattern, ma_trend, score))
        if signal_active != 1 and symbol not in current_positions:
            continue

        feat = feature_map.get(symbol, {})
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "industry": industry,
                "sector_rank": int(sector_rank_map.get(industry, 999)),
                "score": score,
                "close": close,
                "pct_chg": _to_float(r.get("pct_chg"), 0.0),
                "amount": amount,
                "vol": _to_float(p.get("vol"), 0.0),
                "pattern": pattern,
                "ma_trend": ma_trend,
                "signal_active": signal_active,
                "breakout_date": str(feat.get("breakout_date") or screener_date or trade_date),
                "breakout_price": _to_float(feat.get("breakout_price"), close),
                "stop_price": _to_float(feat.get("stop_price"), 0.0),
                "ma10": _to_float(feat.get("ma10"), 0.0),
                "ma20": _to_float(feat.get("ma20"), 0.0),
                "pullback_ready": int(_to_float(feat.get("pullback_ready"), 0.0)),
                "chase_ok": int(_to_float(feat.get("chase_ok"), 1.0)),
                "has_platform": int(_to_float(feat.get("has_platform"), 0.0)),
                "platform_low": _to_float(feat.get("platform_low"), 0.0),
                "breakout_candle_low": _to_float(feat.get("breakout_candle_low"), 0.0),
                "from_holdings_fallback": 0,
            }
        )

    for symbol, pos in current_positions.items():
        if any(x["symbol"] == symbol for x in rows):
            continue
        p = price_map.get(symbol)
        if not p:
            continue
        feat = feature_map.get(symbol, {})
        rows.append(
            {
                "symbol": symbol,
                "name": pos.get("name") or p.get("name") or "",
                "industry": "",
                "sector_rank": 999,
                "score": -1.0,
                "close": _to_float(p.get("close")),
                "pct_chg": 0.0,
                "amount": _to_float(p.get("amount"), 0.0),
                "vol": _to_float(p.get("vol"), 0.0),
                "pattern": "",
                "ma_trend": "",
                "signal_active": 0,
                "breakout_date": str(feat.get("breakout_date") or trade_date),
                "breakout_price": _to_float(feat.get("breakout_price"), _to_float(p.get("close"), 0.0)),
                "stop_price": _to_float(feat.get("stop_price"), 0.0),
                "ma10": _to_float(feat.get("ma10"), 0.0),
                "ma20": _to_float(feat.get("ma20"), 0.0),
                "pullback_ready": int(_to_float(feat.get("pullback_ready"), 0.0)),
                "chase_ok": int(_to_float(feat.get("chase_ok"), 1.0)),
                "has_platform": int(_to_float(feat.get("has_platform"), 0.0)),
                "platform_low": _to_float(feat.get("platform_low"), 0.0),
                "breakout_candle_low": _to_float(feat.get("breakout_candle_low"), 0.0),
                "from_holdings_fallback": 1,
            }
        )

    if not rows:
        fallback = _build_candidate_pool(trade_date, current_positions, limit=limit)
        fallback = _apply_v2_sector_rank(fallback, sector_rank_map)
        return _ensure_v2_candidate_columns(fallback, trade_date), sector_notes

    df = pd.DataFrame(rows).drop_duplicates(subset=["symbol"], keep="first")
    df = df.sort_values(["signal_active", "score", "amount"], ascending=[False, False, False]).head(limit).reset_index(drop=True)
    return _ensure_v2_candidate_columns(df, trade_date), sector_notes


def _v2_regime_limits(csi500_regime: Dict[str, Any]) -> Dict[str, Any]:
    regime = str(csi500_regime.get("regime") or "neutral").lower()
    if regime == "bull":
        return {"regime": "bull", "single_cap": 0.10, "total_cap": 0.80, "tiers": [0.10, 0.08, 0.05]}
    if regime == "bear":
        return {"regime": "bear", "single_cap": 0.045, "total_cap": 0.20, "tiers": [0.045, 0.04, 0.035]}
    return {"regime": "neutral", "single_cap": 0.05, "total_cap": 0.50, "tiers": [0.05, 0.045, 0.04]}


def _v2_build_rule_targets(
    current_positions: Dict[str, Dict[str, Any]],
    current_weights: Dict[str, float],
    candidates_df: pd.DataFrame,
    price_map: Dict[str, Dict[str, Any]],
    csi500_regime: Dict[str, Any],
    max_positions: int,
) -> Tuple[Dict[str, float], Dict[str, str], List[str], set[str], set[str]]:
    limits = _v2_regime_limits(csi500_regime)
    single_cap = _to_float(limits.get("single_cap"), 0.05)
    total_cap = _to_float(limits.get("total_cap"), 0.5)
    tiers = list(limits.get("tiers") or [single_cap])

    candidate_rows: Dict[str, Dict[str, Any]] = {}
    for _, row in candidates_df.iterrows():
        symbol = _normalize_symbol(row.get("symbol", ""))
        if symbol:
            candidate_rows[symbol] = dict(row)

    notes: List[str] = []
    reasons: Dict[str, str] = {}
    forced_stop_sell: set[str] = set()
    buy_eligible: set[str] = set()
    targets: Dict[str, float] = {}
    used = 0.0

    for symbol in sorted(current_positions.keys(), key=lambda x: current_weights.get(x, 0.0), reverse=True):
        row = candidate_rows.get(symbol, {})
        stop_price = _to_float(row.get("stop_price"), 0.0)
        close = _to_float(price_map.get(symbol, {}).get("close"), 0.0)
        if stop_price > 0 and close > 0 and close < stop_price:
            forced_stop_sell.add(symbol)
            reasons[symbol] = f"收盘价 {close:.3f} 跌破止损位 {stop_price:.3f}，执行卖出。"
            continue

        w = min(current_weights.get(symbol, 0.0), single_cap)
        if w <= 1e-6:
            continue
        remain = total_cap - used
        if remain < V2_MIN_TRADE_WEIGHT:
            reasons[symbol] = "总仓位上限受限，先行降仓。"
            continue
        w = min(w, remain)
        targets[symbol] = w
        used += w
        reasons[symbol] = "未触发止损，继续持有。"

    buy_df = candidates_df.copy()
    if not buy_df.empty:
        buy_df = buy_df[
            (buy_df["signal_active"] == 1)
            & (buy_df["pullback_ready"] == 1)
            & (buy_df["chase_ok"] == 1)
        ].copy()
        buy_df = buy_df[~buy_df["symbol"].isin(list(current_positions.keys()))]
        buy_df = buy_df.sort_values(["score", "amount"], ascending=[False, False])

    tier_idx = 0
    for _, row in buy_df.iterrows() if not buy_df.empty else []:
        symbol = _normalize_symbol(row.get("symbol", ""))
        if not symbol:
            continue
        buy_eligible.add(symbol)
        if len([x for x, w in targets.items() if w > 1e-8]) >= int(max_positions):
            break
        remain = total_cap - used
        if remain < V2_MIN_TRADE_WEIGHT:
            break
        tier_w = _to_float(tiers[min(tier_idx, len(tiers) - 1)], single_cap)
        tier_idx += 1
        w = min(single_cap, tier_w, remain)
        if w < V2_MIN_TRADE_WEIGHT:
            continue
        targets[symbol] = w
        used += w
        reasons[symbol] = "行业前3中的强势底部突破标的，回踩MA10/MA20附近且未追高，执行买入。"

    for symbol in current_positions.keys():
        if symbol not in targets and symbol not in forced_stop_sell:
            reasons[symbol] = "仓位预算收缩或优先级不足，执行降仓腾挪。"

    notes.append(f"2号仓位规则: 单票<= {single_cap:.2%}，总仓<= {total_cap:.2%}。")
    if forced_stop_sell:
        notes.append(f"收盘止损触发 {len(forced_stop_sell)} 只: {'、'.join(sorted(forced_stop_sell)[:5])}")
    return targets, reasons, notes, forced_stop_sell, buy_eligible


def _v2_merge_llm_targets(
    rule_targets: Dict[str, float],
    llm_actions: List[Dict[str, Any]],
    current_positions: Dict[str, Dict[str, Any]],
    allowed_symbols: set[str],
    buy_eligible: set[str],
    forced_stop_sell: set[str],
    csi500_regime: Dict[str, Any],
    max_positions: int,
) -> Tuple[Dict[str, float], List[str], set[str]]:
    limits = _v2_regime_limits(csi500_regime)
    single_cap = _to_float(limits.get("single_cap"), 0.05)
    total_cap = _to_float(limits.get("total_cap"), 0.5)
    merged = dict(rule_targets)
    notes: List[str] = []
    override_symbols: set[str] = set()

    rejected = 0
    accepted = 0
    for item in llm_actions:
        symbol = _normalize_symbol(item.get("symbol", ""))
        action = str(item.get("action") or "").lower()
        tw = _to_float(item.get("target_weight"), 0.0)
        if not symbol or symbol not in allowed_symbols:
            rejected += 1
            continue
        if symbol in forced_stop_sell and action != "sell":
            rejected += 1
            continue
        if action in {"buy", "hold"} and symbol not in current_positions and symbol not in buy_eligible:
            rejected += 1
            continue

        if action == "sell":
            merged[symbol] = 0.0
            override_symbols.add(symbol)
            accepted += 1
            continue
        if action in {"buy", "hold"}:
            merged[symbol] = min(max(0.0, tw), single_cap)
            override_symbols.add(symbol)
            accepted += 1

    merged = {k: max(0.0, _to_float(v)) for k, v in merged.items()}

    non_zero = [(k, v) for k, v in merged.items() if v > 1e-8]
    non_zero.sort(key=lambda x: x[1], reverse=True)
    keep = dict(non_zero[: int(max_positions)])
    for k, _ in non_zero[int(max_positions):]:
        keep[k] = 0.0

    total_w = sum(v for v in keep.values() if v > 0)
    if total_w > total_cap > 0:
        scale = total_cap / total_w
        for k in list(keep.keys()):
            keep[k] = keep[k] * scale
        notes.append(f"LLM目标超出总仓上限，按 {scale:.3f} 比例缩放。")

    if accepted > 0:
        notes.append(f"LLM在策略池内调整 {accepted} 笔指令。")
    if rejected > 0:
        notes.append(f"LLM有 {rejected} 笔池外/违规指令被拦截。")
    return {k: round(v, 6) for k, v in keep.items() if v > 1e-8}, notes, override_symbols


def _v2_targets_to_actions(
    target_weights: Dict[str, float],
    current_weights: Dict[str, float],
    reasons_map: Dict[str, str],
    override_symbols: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    override_symbols = override_symbols or set()
    actions: List[Dict[str, Any]] = []
    all_symbols = sorted(set(target_weights.keys()) | set(current_weights.keys()))
    for symbol in all_symbols:
        current_w = _to_float(current_weights.get(symbol), 0.0)
        target_w = _to_float(target_weights.get(symbol), 0.0)
        if target_w <= 1e-8 and current_w <= 1e-8:
            continue
        if target_w <= 1e-8 and current_w > 1e-8:
            action = "sell"
        elif current_w <= 1e-8 and target_w > 1e-8:
            action = "buy"
        else:
            action = "hold"
        reason = str(reasons_map.get(symbol) or "2号策略规则执行")
        if symbol in override_symbols:
            reason = f"{reason}（LLM在策略池内微调）"
        actions.append(
            {
                "symbol": symbol,
                "action": action,
                "target_weight": round(target_w, 6),
                "reason": reason,
                "confidence": 0.70 if symbol not in override_symbols else 0.66,
                "gate_status": "passed",
                "gate_notes": "",
            }
        )
    return actions


def _save_v2_watchlist(
    portfolio_id: str,
    trade_date: str,
    candidates_df: pd.DataFrame,
    current_positions: Dict[str, Dict[str, Any]],
    final_positions: Dict[str, Dict[str, Any]],
    executed_trades: List[Dict[str, Any]],
) -> None:
    if candidates_df.empty:
        return

    old_df = get_watchlist(
        portfolio_id=portfolio_id,
        as_of_date=trade_date,
        limit=V2_WATCHLIST_LIMIT,
        statuses=["watching", "bought", "exited", "invalid"],
    )
    old_map = {
        _normalize_symbol(r.get("symbol", "")): dict(r)
        for _, r in old_df.iterrows()
        if _normalize_symbol(r.get("symbol", ""))
    } if not old_df.empty else {}

    sold_today = {
        _normalize_symbol(t.get("symbol", ""))
        for t in executed_trades
        if str(t.get("side") or "").lower() == "sell" and _normalize_symbol(t.get("symbol", ""))
    }

    records: Dict[str, Dict[str, Any]] = {}
    for _, row in candidates_df.iterrows():
        symbol = _normalize_symbol(row.get("symbol", ""))
        if not symbol or int(_to_float(row.get("from_holdings_fallback"), 0.0)) == 1:
            continue
        prev = old_map.get(symbol, {})
        status = "bought" if symbol in final_positions else "watching"
        if symbol not in final_positions and symbol in sold_today and str(prev.get("status") or "") == "bought":
            status = "exited"

        records[symbol] = {
            "portfolio_id": portfolio_id,
            "symbol": symbol,
            "name": str(row.get("name") or prev.get("name") or ""),
            "status": status,
            "sector_name": str(row.get("industry") or prev.get("sector_name") or ""),
            "score": _to_float(row.get("score"), _to_float(prev.get("score"), 0.0)),
            "breakout_date": str(row.get("breakout_date") or prev.get("breakout_date") or trade_date),
            "breakout_price": _to_float(row.get("breakout_price"), _to_float(prev.get("breakout_price"), 0.0)),
            "stop_price": _to_float(row.get("stop_price"), _to_float(prev.get("stop_price"), 0.0)),
            "ma10": _to_float(row.get("ma10"), _to_float(prev.get("ma10"), 0.0)),
            "ma20": _to_float(row.get("ma20"), _to_float(prev.get("ma20"), 0.0)),
            "pullback_ready": int(_to_float(row.get("pullback_ready"), _to_float(prev.get("pullback_ready"), 0.0))),
            "chase_ok": int(_to_float(row.get("chase_ok"), _to_float(prev.get("chase_ok"), 1.0))),
            "has_platform": int(_to_float(row.get("has_platform"), _to_float(prev.get("has_platform"), 0.0))),
            "platform_low": _to_float(row.get("platform_low"), _to_float(prev.get("platform_low"), 0.0)),
            "breakout_candle_low": _to_float(
                row.get("breakout_candle_low"),
                _to_float(prev.get("breakout_candle_low"), 0.0),
            ),
            "last_signal_date": re.sub(r"[^0-9]", "", str(trade_date or ""))[:8],
            "notes": str(prev.get("notes") or ""),
        }

    for symbol, prev in old_map.items():
        if symbol in records:
            continue
        if symbol in final_positions:
            status = "bought"
        elif symbol in sold_today and str(prev.get("status") or "") == "bought":
            status = "exited"
        else:
            status = str(prev.get("status") or "watching")
        records[symbol] = {
            "portfolio_id": portfolio_id,
            "symbol": symbol,
            "name": str(prev.get("name") or ""),
            "status": status,
            "sector_name": str(prev.get("sector_name") or ""),
            "score": _to_float(prev.get("score"), 0.0),
            "breakout_date": str(prev.get("breakout_date") or ""),
            "breakout_price": _to_float(prev.get("breakout_price"), 0.0),
            "stop_price": _to_float(prev.get("stop_price"), 0.0),
            "ma10": _to_float(prev.get("ma10"), 0.0),
            "ma20": _to_float(prev.get("ma20"), 0.0),
            "pullback_ready": int(_to_float(prev.get("pullback_ready"), 0.0)),
            "chase_ok": int(_to_float(prev.get("chase_ok"), 1.0)),
            "has_platform": int(_to_float(prev.get("has_platform"), 0.0)),
            "platform_low": _to_float(prev.get("platform_low"), 0.0),
            "breakout_candle_low": _to_float(prev.get("breakout_candle_low"), 0.0),
            "last_signal_date": str(prev.get("last_signal_date") or ""),
            "notes": str(prev.get("notes") or ""),
        }

    if not records:
        return

    rows = list(records.values())[:V2_WATCHLIST_LIMIT]
    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO ai_sim_watchlist (
                        portfolio_id, symbol, name, status, sector_name, score,
                        breakout_date, breakout_price, stop_price, ma10, ma20,
                        pullback_ready, chase_ok, has_platform, platform_low, breakout_candle_low,
                        last_signal_date, notes
                    ) VALUES (
                        :portfolio_id, :symbol, :name, :status, :sector_name, :score,
                        :breakout_date, :breakout_price, :stop_price, :ma10, :ma20,
                        :pullback_ready, :chase_ok, :has_platform, :platform_low, :breakout_candle_low,
                        :last_signal_date, :notes
                    )
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        status = VALUES(status),
                        sector_name = VALUES(sector_name),
                        score = VALUES(score),
                        breakout_date = VALUES(breakout_date),
                        breakout_price = VALUES(breakout_price),
                        stop_price = VALUES(stop_price),
                        ma10 = VALUES(ma10),
                        ma20 = VALUES(ma20),
                        pullback_ready = VALUES(pullback_ready),
                        chase_ok = VALUES(chase_ok),
                        has_platform = VALUES(has_platform),
                        platform_low = VALUES(platform_low),
                        breakout_candle_low = VALUES(breakout_candle_low),
                        last_signal_date = VALUES(last_signal_date),
                        notes = VALUES(notes),
                        updated_at = CURRENT_TIMESTAMP
                    """
                ),
                row,
            )

def _classify_style_hint(symbol: str, name: str, amount: float, pct_chg: float) -> str:
    s = _normalize_symbol(symbol)
    if _is_etf_symbol(s, name):
        return "steady"

    amt = _to_float(amount, 0.0)
    pct_abs = abs(_to_float(pct_chg, 0.0))

    if amt >= 2e9 and pct_abs <= 3.0:
        return "steady"
    if amt <= 8e8 or pct_abs >= 6.0:
        return "aggressive"
    return "balanced"


def _build_style_map(candidates_df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if candidates_df.empty:
        return out
    for _, row in candidates_df.iterrows():
        symbol = _normalize_symbol(row.get("symbol", ""))
        if not symbol:
            continue
        out[symbol] = _classify_style_hint(
            symbol=symbol,
            name=str(row.get("name") or ""),
            amount=_to_float(row.get("amount"), 0.0),
            pct_chg=_to_float(row.get("pct_chg"), 0.0),
        )
    return out


def _get_csi500_regime(trade_date: str) -> Dict[str, Any]:
    sql = text(
        """
        SELECT trade_date, close_price
        FROM index_price
        WHERE ts_code = '000905.SH' AND trade_date <= :td
        ORDER BY trade_date DESC
        LIMIT 80
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"td": trade_date})

    if df.empty:
        return {
            "regime": "neutral",
            "score": 0,
            "close": None,
            "ma20": None,
            "ma60": None,
            "day_ret": 0.0,
            "ret20": 0.0,
            "summary": "中证500技术面数据不足，按中性处理。",
        }

    d = df.sort_values("trade_date").reset_index(drop=True)
    close = _to_float(d["close_price"].iloc[-1], 0.0)
    prev_close = _to_float(d["close_price"].iloc[-2], 0.0) if len(d) >= 2 else 0.0
    ma20 = _to_float(d["close_price"].tail(20).mean(), close)
    ma60 = _to_float(d["close_price"].tail(60).mean(), ma20)
    day_ret = (close / prev_close - 1.0) if prev_close > 0 else 0.0
    if len(d) >= 21 and _to_float(d["close_price"].iloc[-21], 0.0) > 0:
        ret20 = close / _to_float(d["close_price"].iloc[-21], close) - 1.0
    else:
        ret20 = 0.0

    score = 0
    score += 1 if close >= ma20 else -1
    score += 1 if ma20 >= ma60 else -1
    if day_ret >= 0.03:
        score += 2
    elif day_ret <= -0.03:
        score -= 2

    if score >= 2:
        regime = "bull"
        summary = "中证500偏多，可适度积极配置。"
    elif score <= -2:
        regime = "bear"
        summary = "中证500偏空，建议防守或明显降仓。"
    else:
        regime = "neutral"
        summary = "中证500中性震荡，维持均衡仓位。"

    return {
        "regime": regime,
        "score": int(score),
        "close": close,
        "ma20": ma20,
        "ma60": ma60,
        "day_ret": day_ret,
        "ret20": ret20,
        "summary": summary,
    }


def _regime_stock_exposure_cap(csi500_regime: Dict[str, Any]) -> float:
    regime = str(csi500_regime.get("regime") or "neutral").lower()
    score = int(csi500_regime.get("score") or 0)
    if regime == "bear":
        # 鏋佸急鐜鍏佽鍏ㄧ幇閲戣鏈涖€?
        return 0.0 if score <= -3 else 0.35
    if regime == "bull":
        return 0.95
    return 0.75


def _extract_json_from_text(raw_text: str) -> Optional[Dict[str, Any]]:
    if not raw_text:
        return None

    raw_text = str(raw_text).strip()
    try:
        obj = json.loads(raw_text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", raw_text)
    if not match:
        return None
    try:
        obj = json.loads(match.group(0))
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _current_weight_map(current_positions: Dict[str, Dict[str, Any]], price_map: Dict[str, Dict[str, Any]], nav_prev: float) -> Dict[str, float]:
    if nav_prev <= 0:
        return {}
    weight_map: Dict[str, float] = {}
    for symbol, pos in current_positions.items():
        p = price_map.get(symbol)
        if not p:
            continue
        mv = _to_float(pos.get("quantity")) * _to_float(p.get("close"))
        if mv <= 0:
            continue
        weight_map[symbol] = mv / nav_prev
    return weight_map


def _fallback_ai_actions(
    candidates_df: pd.DataFrame,
    current_weights: Dict[str, float],
    config: Dict[str, Any],
    csi500_regime: Dict[str, Any],
    style_map: Dict[str, str],
) -> Dict[str, Any]:
    max_positions = int(config.get("max_positions", 10))
    stock_cap = _regime_stock_exposure_cap(csi500_regime)
    regime = str(csi500_regime.get("regime") or "neutral")
    picks = candidates_df.sort_values(["score", "amount"], ascending=[False, False]).head(max_positions * 3)

    actions: List[Dict[str, Any]] = []
    if picks.empty or stock_cap <= 1e-8:
        for symbol, w in current_weights.items():
            if w > 0.01:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "sell",
                        "target_weight": 0.0,
                        "reason": f"中证500偏空({regime})，回退策略先回收仓位。",
                        "confidence": 0.6,
                    }
                )
        return {
            "summary": "回退策略：当前市场环境不利，优先防守并保留现金。",
            "risk_notes": f"{csi500_regime.get('summary', '中证500偏弱')}（回退模式）",
            "actions": actions,
            "source": "fallback_rule",
        }

    picks = picks.copy()
    picks["style_hint"] = picks["symbol"].map(style_map).fillna("balanced")
    steady_pool = picks[picks["style_hint"].isin(["steady", "balanced"])]
    aggr_pool = picks[picks["style_hint"] == "aggressive"]

    selected_rows: List[Dict[str, Any]] = []
    selected_symbols: set[str] = set()
    if regime != "bear":
        if not steady_pool.empty:
            r = steady_pool.iloc[0]
            selected_rows.append(dict(r))
            selected_symbols.add(str(r["symbol"]))
        if not aggr_pool.empty:
            r = aggr_pool.iloc[0]
            if str(r["symbol"]) not in selected_symbols:
                selected_rows.append(dict(r))
                selected_symbols.add(str(r["symbol"]))

    for _, row in picks.iterrows():
        sym = str(row.get("symbol") or "")
        if sym in selected_symbols:
            continue
        selected_rows.append(dict(row))
        selected_symbols.add(sym)
        if len(selected_rows) >= max_positions:
            break

    selected_df = pd.DataFrame(selected_rows).head(max_positions)
    if selected_df.empty:
        return {
            "summary": "候选池为空，保持当前持仓不变。",
            "risk_notes": "当日无有效候选股，策略进入防守模式。",
            "actions": actions,
            "source": "fallback_rule",
        }

    target_weight = min(stock_cap / max(1, len(selected_df)), float(config.get("max_single_weight_soft", 0.20)))

    pick_symbols = set()
    for _, row in selected_df.iterrows():
        symbol = _normalize_symbol(row.get("symbol", ""))
        if not symbol:
            continue
        pick_symbols.add(symbol)
        style = str(style_map.get(symbol) or "balanced")
        style_cn = "稳健中线" if style in {"steady", "balanced"} else "积极短线"
        actions.append(
            {
                "symbol": symbol,
                "action": "buy" if current_weights.get(symbol, 0.0) <= 0 else "hold",
                "target_weight": round(target_weight, 4),
                "reason": f"{style_cn}配置，候选池评分靠前，score={_to_float(row.get('score')):.2f}",
                "confidence": 0.55,
            }
        )

    for symbol, w in current_weights.items():
        if symbol not in pick_symbols and w > 0.01:
            actions.append(
                {
                    "symbol": symbol,
                    "action": "sell",
                    "target_weight": 0.0,
                    "reason": "不在当日优选候选池或市场偏弱，执行减仓。",
                    "confidence": 0.5,
                }
            )

    return {
        "summary": f"使用规则回退策略：参考中证500({regime})并做风格混配。",
        "risk_notes": f"{csi500_regime.get('summary', '当前为回退模式，建议检查模型服务可用性。')}",
        "actions": actions,
        "source": "fallback_rule",
    }


def _format_recent_trade_memory(trades_df: pd.DataFrame, max_days: int = 5, max_items_per_day: int = 4) -> str:
    if trades_df.empty:
        return "近几个交易日无历史成交记录。"

    df = trades_df.copy()
    df["trade_date"] = df["trade_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    df = df[df["trade_date"].str.len() == 8]
    if df.empty:
        return "近几个交易日无历史成交记录。"

    unique_days = sorted(df["trade_date"].unique(), reverse=True)[:max_days]
    day_lines: List[str] = []
    for td in unique_days:
        ddf = df[df["trade_date"] == td].copy()
        if ddf.empty:
            continue

        buy_df = ddf[ddf["side"].astype(str).str.lower() == "buy"]
        sell_df = ddf[ddf["side"].astype(str).str.lower() == "sell"]
        buy_amt = _to_float(buy_df["amount"].sum() if not buy_df.empty else 0.0)
        sell_amt = _to_float(sell_df["amount"].sum() if not sell_df.empty else 0.0)

        top_moves = ddf.sort_values("amount", ascending=False).head(max_items_per_day)
        move_parts: List[str] = []
        for _, row in top_moves.iterrows():
            symbol = str(row.get("symbol") or "")
            side = str(row.get("side") or "").lower()
            side_cn = "买入" if side == "buy" else ("卖出" if side == "sell" else side)
            amount_wan = _to_float(row.get("amount"), 0.0) / 1e4
            reason = str(row.get("reason_short") or "").strip()
            if reason:
                move_parts.append(f"{side_cn}{symbol}({amount_wan:.1f}万, {reason})")
            else:
                move_parts.append(f"{side_cn}{symbol}({amount_wan:.1f}万)")

        td_view = f"{td[:4]}-{td[4:6]}-{td[6:]}"
        line = (
            f"- {td_view}：{len(ddf)}笔，买{len(buy_df)}笔/{buy_amt/1e4:.1f}万，"
            f"卖{len(sell_df)}笔/{sell_amt/1e4:.1f}万；主要动作：{('、'.join(move_parts) if move_parts else '无')}"
        )
        day_lines.append(line)

    if not day_lines:
        return "近几个交易日无历史成交记录。"
    return "\n".join(day_lines)

def _load_recent_trade_memory(portfolio_id: str, trade_date: str, days: int = 5) -> str:
    ensure_ai_sim_tables()
    sql = text(
        """
        SELECT t.trade_date, t.symbol, t.side, t.quantity, t.price, t.amount,
               COALESCE(o.reason_short, '') AS reason_short
        FROM ai_sim_trades t
        LEFT JOIN (
            SELECT portfolio_id, trade_date, symbol, side, MAX(reason_short) AS reason_short
            FROM ai_sim_orders
            WHERE portfolio_id = :pid2 AND trade_date < :td2
            GROUP BY portfolio_id, trade_date, symbol, side
        ) o
          ON o.portfolio_id = t.portfolio_id
         AND o.trade_date = t.trade_date
         AND o.symbol = t.symbol
         AND o.side = t.side
        WHERE t.portfolio_id = :pid
          AND t.trade_date < :td
        ORDER BY t.trade_date DESC, t.created_at DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"pid": portfolio_id, "td": trade_date, "pid2": portfolio_id, "td2": trade_date, "lim": int(max(20, days * 40))})
    return _format_recent_trade_memory(df, max_days=days)


def _build_ai_prompt(
    trade_date: str,
    nav_prev: float,
    cash: float,
    positions: Dict[str, Dict[str, Any]],
    candidates_df: pd.DataFrame,
    config: Dict[str, Any],
    csi500_regime: Dict[str, Any],
    style_map: Dict[str, str],
    recent_trade_memory: str = "",
) -> Tuple[str, str]:
    positions_lines: List[str] = []
    for symbol, pos in sorted(positions.items()):
        positions_lines.append(
            f"- {symbol} {pos.get('name', '')} qty={_to_float(pos.get('quantity')):.0f} avg_cost={_to_float(pos.get('avg_cost')):.3f}"
        )
    if not positions_lines:
        positions_lines = ["- 当前空仓"]

    top_candidates = candidates_df.head(60)
    cand_lines: List[str] = []
    for _, row in top_candidates.iterrows():
        symbol = str(row.get("symbol") or "")
        style = str(style_map.get(symbol) or "balanced")
        style_cn = "稳健" if style in {"steady", "balanced"} else "积极"
        name = str(row.get("name") or "")
        score = _to_float(row.get("score"), 0.0)
        amount = _to_float(row.get("amount"), 0.0)
        close = _to_float(row.get("close"), 0.0)
        cand_lines.append(
            f"- {symbol} {name} style={style_cn} score={score:.2f} amount={amount/1e8:.2f}亿 close={close:.3f}"
        )

    csi_summary = str(csi500_regime.get("summary") or "中证500技术面中性。")
    csi_close = _to_float(csi500_regime.get("close"), 0.0)
    csi_ma20 = _to_float(csi500_regime.get("ma20"), 0.0)
    csi_ma60 = _to_float(csi500_regime.get("ma60"), 0.0)
    csi_day_ret = _to_float(csi500_regime.get("day_ret"), 0.0)

    system_prompt = f"""
你是官方AI模拟投资组合的交易决策引擎。

约束：
1. 只允许A股和ETF，只做多，不允许杠杆和做空。
2. 交易执行为当日收盘价理论成交（close_t0）。
3. 风控：软风控+硬门禁；单票硬上限 {config.get('max_single_weight_hard')}，持仓数不超过 {config.get('max_positions')}。
4. 当日调仓笔数不超过 {config.get('max_daily_trades')}。
5. 你可以使用可用工具进行辅助判断（工具权限全开），但最终输出必须是纯JSON。
6. 必须参考“近5日交易摘要”，保持交易连续性，避免无理由大幅反复换仓。
7. 风格要求：在非偏空市场中，组合尽量同时包含“积极短线”和“稳健中线”两类持仓；偏空时可显著降仓，必要时可全现金。
8. 市场择时：必须参考中证500技术面；偏空时降低总仓位，偏多时可更积极。

输出JSON格式（不要输出额外文本）：
{{
  "summary": "一句总体观点",
  "risk_notes": "一句风险提示",
  "actions": [
    {{"symbol":"600519.SH","action":"buy|sell|hold","target_weight":0.12,"reason":"简短理由","confidence":0.0}}
  ]
}}

要求：
- symbol必须是6位代码+后缀（.SH/.SZ/.BJ）。
- target_weight是组合目标权重（0~1）。
- action=buy/hold 时，target_weight > 0；action=sell 时，target_weight = 0。
- 优先高流动性和高质量候选，避免过度集中。
""".strip()

    user_prompt = f"""
交易日: {trade_date}
组合净值(昨收后): {nav_prev:.2f}
可用现金: {cash:.2f}

当前持仓:
{chr(10).join(positions_lines)}

候选池(已过滤停牌/ST/低流动性，以下为前60):
{chr(10).join(cand_lines)}

中证500技术面:
- 结论: {csi_summary}
- close={csi_close:.2f}, MA20={csi_ma20:.2f}, MA60={csi_ma60:.2f}, 当日涨跌={csi_day_ret:+.2%}

近5日交易摘要:
{recent_trade_memory or "近几个交易日无历史成交记录。"}
""".strip()

    return system_prompt, user_prompt


def _generate_ai_actions_with_tools(
    portfolio_id: str,
    trade_date: str,
    nav_prev: float,
    cash: float,
    positions: Dict[str, Dict[str, Any]],
    candidates_df: pd.DataFrame,
    config: Dict[str, Any],
    csi500_regime: Dict[str, Any],
    style_map: Dict[str, str],
    recent_trade_memory: str = "",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], str]:
    # 避免代理/追踪服务导致外部请求异常，和项目里其他抓取脚本保持一致。
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ]:
        if key in os.environ:
            del os.environ[key]
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ["LANGSMITH_TRACING"] = "false"

    current_prices = _fetch_price_snapshot(list(positions.keys()), trade_date)
    current_weights = _current_weight_map(positions, current_prices, nav_prev)

    if candidates_df.empty:
        return _fallback_ai_actions(candidates_df, current_weights, config, csi500_regime, style_map), [], "候选池为空，使用回退策略。"

    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        return _fallback_ai_actions(candidates_df, current_weights, config, csi500_regime, style_map), [], "缺少 DASHSCOPE_API_KEY，使用回退策略。"

    model_name = str(config.get("model_name") or DEFAULT_CONFIG["model_name"])
    system_prompt, user_prompt = _build_ai_prompt(
        trade_date=trade_date,
        nav_prev=nav_prev,
        cash=cash,
        positions=positions,
        candidates_df=candidates_df,
        config=config,
        csi500_regime=csi500_regime,
        style_map=style_map,
        recent_trade_memory=recent_trade_memory,
    )

    llm = ChatTongyiCompat(model=model_name, streaming=False, temperature=0.2, api_key=api_key)
    tool_calls: List[Dict[str, Any]] = []

    try:
        agent = create_react_agent(llm, TOOLS_FOR_SIMULATION, prompt=system_prompt)
        result = agent.invoke({"messages": [HumanMessage(content=user_prompt)]}, {"recursion_limit": 24})
        messages = result.get("messages", [])

        for msg in messages:
            msg_type = str(getattr(msg, "type", ""))
            if msg_type == "tool":
                tool_calls.append(
                    {
                        "name": str(getattr(msg, "name", "tool")),
                        "content": str(getattr(msg, "content", ""))[:800],
                    }
                )

        final_text = ""
        if messages:
            final_text = str(getattr(messages[-1], "content", ""))
        parsed = _extract_json_from_text(final_text)
        if parsed and isinstance(parsed.get("actions"), list):
            parsed["source"] = "llm"
            return parsed, tool_calls, ""

        fallback = _fallback_ai_actions(candidates_df, current_weights, config, csi500_regime, style_map)
        return fallback, tool_calls, "模型输出非JSON，使用回退策略。"
    except Exception as exc:
        fallback = _fallback_ai_actions(candidates_df, current_weights, config, csi500_regime, style_map)
        return fallback, tool_calls, f"模型决策异常({exc})，使用回退策略。"

def _sanitize_actions(raw_actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    for item in raw_actions:
        symbol = _normalize_symbol(item.get("symbol", ""))
        if not symbol:
            continue
        action = str(item.get("action", "hold")).lower().strip()
        if action not in {"buy", "sell", "hold"}:
            action = "hold"
        target_weight = max(0.0, min(1.0, _to_float(item.get("target_weight"), 0.0)))
        confidence = max(0.0, min(1.0, _to_float(item.get("confidence"), 0.5)))

        actions.append(
            {
                "symbol": symbol,
                "action": action,
                "target_weight": target_weight,
                "reason": str(item.get("reason") or ""),
                "confidence": confidence,
            }
        )
    return actions


def _apply_risk_gates(
    raw_actions: List[Dict[str, Any]],
    current_weights: Dict[str, float],
    candidate_symbols: set,
    config: Dict[str, Any],
    csi500_regime: Dict[str, Any],
    style_map: Dict[str, str],
    candidate_score_map: Dict[str, float],
) -> Tuple[List[Dict[str, Any]], Dict[str, float], List[str]]:
    hard_cap = _to_float(config.get("max_single_weight_hard"), 0.30)
    soft_cap = _to_float(config.get("max_single_weight_soft"), 0.20)
    max_positions = int(config.get("max_positions", 10))

    audited_actions: List[Dict[str, Any]] = []
    risk_notes: List[str] = []

    target_weights = dict(current_weights)

    for action in raw_actions:
        symbol = action["symbol"]
        notes: List[str] = []
        status = "passed"

        if symbol not in candidate_symbols and symbol not in current_weights:
            status = "rejected"
            notes.append("symbol not in candidate/current universe")

        if status != "rejected" and action["target_weight"] > hard_cap:
            action["target_weight"] = hard_cap
            status = "adjusted"
            notes.append(f"hard cap clipped to {hard_cap:.2f}")

        if status != "rejected" and action["action"] in {"buy", "hold"} and action["target_weight"] > soft_cap:
            action["target_weight"] = soft_cap
            status = "adjusted"
            notes.append(f"soft cap clipped to {soft_cap:.2f}")

        if status != "rejected":
            if action["action"] == "sell":
                target_weights[symbol] = 0.0
            else:
                target_weights[symbol] = action["target_weight"]

        audited_actions.append(
            {
                **action,
                "gate_status": status,
                "gate_notes": "; ".join(notes),
            }
        )

    # 娓呯悊璐熸潈閲?
    target_weights = {k: max(0.0, _to_float(v)) for k, v in target_weights.items()}

    # 鎸佷粨鏁伴噺纭檺鍒?
    non_zero = [(k, v) for k, v in target_weights.items() if v > 1e-6]
    non_zero.sort(key=lambda x: x[1], reverse=True)
    keep = dict(non_zero[:max_positions])
    dropped = [k for k, _ in non_zero[max_positions:]]
    for k in dropped:
        keep[k] = 0.0
    if dropped:
        risk_notes.append(f"鎸佷粨鏁拌秴闄愶紝绉婚櫎 {len(dropped)} 鍙綆鏉冮噸鏍囩殑")

    # 甯傚満鎷╂椂浠撲綅闂ㄧ锛堜腑璇?00鎶€鏈潰锛?
    stock_cap = _regime_stock_exposure_cap(csi500_regime)
    total_weight = sum(v for v in keep.values() if v > 0)
    if total_weight > stock_cap >= 0:
        scale = (stock_cap / total_weight) if total_weight > 0 else 1.0
        for k in list(keep.keys()):
            keep[k] = keep[k] * scale
        risk_notes.append(f"涓瘉500鎷╂椂绾︽潫瑙﹀彂锛岀洰鏍囦粨浣嶆寜 {scale:.3f} 绛夋瘮缂╂斁")

    # 鏈€缁堝綊涓€鍖栦繚鎶?
    total_weight = sum(v for v in keep.values() if v > 0)
    if total_weight > 1.0:
        scale = 1.0 / total_weight
        for k in list(keep.keys()):
            keep[k] = keep[k] * scale
        risk_notes.append("目标总权重>1，已归一化缩放")

    # 椋庢牸娣烽厤绾︽潫锛氶潪鍋忕┖甯傚満灏介噺鍏奸【绉瀬涓庣ǔ鍋ャ€?
    regime = str(csi500_regime.get("regime") or "neutral")
    active_symbols = [k for k, v in keep.items() if v > 1e-6]
    if regime != "bear" and len(active_symbols) >= 3:
        def _style_of(sym: str) -> str:
            return str(style_map.get(sym) or "balanced")

        def _has_steady(syms: List[str]) -> bool:
            return any(_style_of(s) in {"steady", "balanced"} for s in syms)

        def _has_aggr(syms: List[str]) -> bool:
            return any(_style_of(s) == "aggressive" for s in syms)

        def _pick_missing_style(target: str) -> str:
            if target == "steady":
                ok = {"steady", "balanced"}
            else:
                ok = {"aggressive"}
            pool = [(s, _to_float(sc, 0.0)) for s, sc in candidate_score_map.items() if s not in keep and _style_of(s) in ok]
            if not pool:
                return ""
            pool.sort(key=lambda x: x[1], reverse=True)
            return pool[0][0]

        def _pick_donor(prefer_styles: set[str]) -> str:
            cands = [s for s in active_symbols if _style_of(s) in prefer_styles and keep.get(s, 0.0) > 1e-6]
            if not cands:
                cands = [s for s in active_symbols if keep.get(s, 0.0) > 1e-6]
            if not cands:
                return ""
            cands.sort(key=lambda s: keep.get(s, 0.0), reverse=True)
            return cands[0]

        if not _has_steady(active_symbols):
            add_sym = _pick_missing_style("steady")
            donor = _pick_donor({"aggressive"})
            if add_sym and donor:
                shift = min(0.08, keep.get(donor, 0.0) * 0.35)
                if shift >= 0.02:
                    keep[donor] = max(0.0, keep.get(donor, 0.0) - shift)
                    keep[add_sym] = keep.get(add_sym, 0.0) + shift
                    risk_notes.append(f"椋庢牸娣烽厤绾︽潫锛氳ˉ鍏呯ǔ鍋ユ爣鐨?{add_sym}锛屼粠 {donor} 鍒掕浆 {shift:.2%} 鏉冮噸")
                    active_symbols = [k for k, v in keep.items() if v > 1e-6]

        if not _has_aggr(active_symbols):
            add_sym = _pick_missing_style("aggressive")
            donor = _pick_donor({"steady", "balanced"})
            if add_sym and donor:
                shift = min(0.08, keep.get(donor, 0.0) * 0.35)
                if shift >= 0.02:
                    keep[donor] = max(0.0, keep.get(donor, 0.0) - shift)
                    keep[add_sym] = keep.get(add_sym, 0.0) + shift
                    risk_notes.append(f"椋庢牸娣烽厤绾︽潫锛氳ˉ鍏呯Н鏋佹爣鐨?{add_sym}锛屼粠 {donor} 鍒掕浆 {shift:.2%} 鏉冮噸")
                    active_symbols = [k for k, v in keep.items() if v > 1e-6]

    final_target = {k: round(v, 6) for k, v in keep.items() if v > 1e-8}
    return audited_actions, final_target, risk_notes


def _plan_trades(
    target_weights: Dict[str, float],
    current_positions: Dict[str, Dict[str, Any]],
    price_map: Dict[str, Dict[str, Any]],
    nav_prev: float,
    max_daily_trades: int,
    max_turnover_hard: float,
    has_prev_day: bool,
) -> List[Dict[str, Any]]:
    drafts: List[Dict[str, Any]] = []

    symbols = sorted(set(target_weights.keys()) | set(current_positions.keys()))
    for symbol in symbols:
        price = _to_float(price_map.get(symbol, {}).get("close"), 0.0)
        if price <= 0:
            continue

        current_qty = _to_float(current_positions.get(symbol, {}).get("quantity"), 0.0)
        current_value = current_qty * price
        target_value = nav_prev * _to_float(target_weights.get(symbol, 0.0), 0.0)
        delta_value = target_value - current_value

        min_step_value = price * DEFAULT_LOT_SIZE
        if abs(delta_value) < min_step_value * 0.5:
            continue

        if delta_value > 0:
            qty = int(delta_value / min_step_value) * DEFAULT_LOT_SIZE
            side = "buy"
        else:
            qty = int(abs(delta_value) / min_step_value) * DEFAULT_LOT_SIZE
            qty = min(int(current_qty), qty)
            side = "sell"

        if qty <= 0:
            continue

        amount = qty * price
        drafts.append(
            {
                "symbol": symbol,
                "side": side,
                "quantity": float(qty),
                "price": float(price),
                "amount": float(amount),
            }
        )

    # 鍏堝崠鍚庝拱锛屼笖鎸夐噾棰濅紭鍏?
    sells = sorted([x for x in drafts if x["side"] == "sell"], key=lambda x: x["amount"], reverse=True)
    buys = sorted([x for x in drafts if x["side"] == "buy"], key=lambda x: x["amount"], reverse=True)
    ordered = sells + buys

    if len(ordered) > max_daily_trades:
        ordered = ordered[:max_daily_trades]

    # 鎹㈡墜鐜囧彛寰勶細闈為鏃ユ墠缁熻锛屼笖鎸夋垚浜ら/2/鎬昏祫閲戯紝閬垮厤棣栨棩寤轰粨琚璇讳负楂樻崲鎵嬨€?
    turnover = 0.0
    if has_prev_day and nav_prev > 0:
        turnover = sum(x["amount"] for x in ordered) / max(1e-9, nav_prev * 2.0)
    if turnover > max_turnover_hard and turnover > 0:
        scale = max_turnover_hard / turnover
        adjusted: List[Dict[str, Any]] = []
        for item in ordered:
            qty = int((item["quantity"] * scale) / DEFAULT_LOT_SIZE) * DEFAULT_LOT_SIZE
            if qty <= 0:
                continue
            amount = qty * item["price"]
            adjusted.append({**item, "quantity": float(qty), "amount": float(amount)})
        ordered = adjusted

    return ordered


def _execute_trades(
    planned_trades: List[Dict[str, Any]],
    current_positions: Dict[str, Dict[str, Any]],
    price_map: Dict[str, Dict[str, Any]],
    cash_start: float,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], float]:
    positions = {k: dict(v) for k, v in current_positions.items()}
    cash = float(cash_start)
    executed: List[Dict[str, Any]] = []

    for t in planned_trades:
        symbol = t["symbol"]
        side = t["side"]
        qty = _to_float(t["quantity"], 0.0)
        price = _to_float(t["price"], 0.0)
        if qty <= 0 or price <= 0:
            continue

        if symbol not in positions:
            positions[symbol] = {
                "symbol": symbol,
                "name": str(price_map.get(symbol, {}).get("name") or ""),
                "quantity": 0.0,
                "avg_cost": 0.0,
            }

        pos = positions[symbol]
        cur_qty = _to_float(pos.get("quantity"), 0.0)
        cur_avg = _to_float(pos.get("avg_cost"), 0.0)
        realized_pnl = 0.0

        if side == "sell":
            qty = min(qty, cur_qty)
            if qty <= 0:
                continue
            realized_pnl = (price - cur_avg) * qty
            cash += qty * price
            new_qty = cur_qty - qty
            pos["quantity"] = new_qty
            if new_qty <= 0:
                pos["quantity"] = 0.0
                pos["avg_cost"] = 0.0
        else:
            required = qty * price
            if required > cash:
                affordable = int(cash / (price * DEFAULT_LOT_SIZE)) * DEFAULT_LOT_SIZE
                if affordable <= 0:
                    continue
                qty = float(affordable)
                required = qty * price

            old_value = cur_qty * cur_avg
            new_qty = cur_qty + qty
            new_avg = (old_value + required) / max(1e-9, new_qty)
            pos["quantity"] = new_qty
            pos["avg_cost"] = new_avg
            cash -= required

        executed.append(
            {
                **t,
                "quantity": float(qty),
                "amount": float(qty * price),
                "realized_pnl": float(realized_pnl),
            }
        )

    positions = {k: v for k, v in positions.items() if _to_float(v.get("quantity")) > 0}
    return positions, executed, float(cash)


def _compute_max_drawdown(portfolio_id: str, nav_today: float) -> float:
    sql = text(
        """
        SELECT nav
        FROM ai_sim_nav_daily
        WHERE portfolio_id = :pid
        ORDER BY trade_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"pid": portfolio_id})

    nav_values = df["nav"].tolist() if not df.empty else []
    nav_values.append(nav_today)
    if not nav_values:
        return 0.0

    s = pd.Series(nav_values, dtype=float)
    peak = s.cummax()
    drawdown = s / peak - 1.0
    return float(drawdown.min())


def _get_index_close(ts_code: str, trade_date: str) -> Optional[float]:
    sql = text(
        """
        SELECT close_price
        FROM index_price
        WHERE ts_code = :code
          AND trade_date = (
            SELECT MAX(trade_date)
            FROM index_price
            WHERE ts_code = :code2 AND trade_date <= :td
          )
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"code": ts_code, "code2": ts_code, "td": trade_date}).fetchone()
    if not row or row[0] is None:
        return None
    return _to_float(row[0], None)


def _compute_benchmark_values(portfolio_id: str, trade_date: str, prev_nav_row: Optional[Dict[str, Any]]) -> Tuple[float, float]:
    if not prev_nav_row:
        return 1.0, 1.0

    prev_td = re.sub(r"[^0-9]", "", str(prev_nav_row.get("trade_date", "")))[:8]
    hs_prev_val = _to_float(prev_nav_row.get("bench_hs300"), 1.0)
    zz_prev_val = _to_float(prev_nav_row.get("bench_zz1000"), 1.0)

    hs_prev_close = _get_index_close("000300.SH", prev_td)
    hs_now_close = _get_index_close("000300.SH", trade_date)
    zz_prev_close = _get_index_close("000852.SH", prev_td)
    zz_now_close = _get_index_close("000852.SH", trade_date)

    hs_val = hs_prev_val
    zz_val = zz_prev_val

    if hs_prev_close and hs_now_close and hs_prev_close > 0:
        hs_val = hs_prev_val * (hs_now_close / hs_prev_close)
    if zz_prev_close and zz_now_close and zz_prev_close > 0:
        zz_val = zz_prev_val * (zz_now_close / zz_prev_close)

    return float(hs_val), float(zz_val)


def _should_inject_persona(
    trade_date: str,
    pnl_pct: float,
    executed_trades: List[Dict[str, Any]],
    risk_notes: List[str],
) -> Tuple[bool, str]:
    if abs(_to_float(pnl_pct, 0.0)) >= 0.012:
        return True, "褰撴棩鐩堜簭娉㈠姩杈冨ぇ"

    note_text = " ".join([str(x) for x in risk_notes if str(x).strip()])
    stop_keywords = ["姝㈡崯", "娓呬粨", "鍓茶倝", "澶у箙鍥炴挙", "椋庢帶鍛婅", "鍥炴挙"]
    if any(k in note_text for k in stop_keywords):
        return True, "出现明显情绪事件（止损或回撤）"

    sell_count = sum(1 for x in executed_trades if str(x.get("side") or "").lower() == "sell")
    if sell_count >= 3:
        return True, "褰撴棩鍗栧嚭鍔ㄤ綔杈冨"

    digits = re.sub(r"[^0-9]", "", str(trade_date or ""))[:8]
    if len(digits) == 8:
        try:
            dt = datetime.strptime(digits, "%Y%m%d")
            if dt.weekday() == 4:
                return True, "周五收官日"
            if dt.day == monthrange(dt.year, dt.month)[1]:
                return True, "鏈堟湯鑺傜偣"
        except Exception:
            pass
    return False, ""


def _load_recent_diary_snippets(portfolio_id: str, trade_date: str, days: int = 7) -> List[str]:
    ensure_ai_sim_tables()
    sql = text(
        """
        SELECT summary_md, buys_md, sells_md, risk_md
        FROM ai_sim_review_daily
        WHERE portfolio_id = :pid AND trade_date < :td
        ORDER BY trade_date DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"pid": portfolio_id, "td": trade_date, "lim": int(max(days, 1))})
    if df.empty:
        return []

    phrases: List[str] = []
    for _, row in df.iterrows():
        text_blob = "\n".join(
            [
                str(row.get("summary_md") or ""),
                str(row.get("buys_md") or ""),
                str(row.get("sells_md") or ""),
                str(row.get("risk_md") or ""),
            ]
        )
        text_blob = re.sub(r"#{1,6}\s*", "", text_blob)
        text_blob = re.sub(r"[`*_>\-]", "", text_blob)
        parts = re.split(r"[锛屻€傦紒锛燂紱\n]", text_blob)
        for p in parts:
            pp = str(p or "").strip()
            if 8 <= len(pp) <= 36:
                phrases.append(pp)

    if not phrases:
        return []

    freq = Counter(phrases)
    blocked = [p for p, c in freq.items() if c >= 2]
    blocked.sort(key=lambda x: (-freq[x], -len(x), x))

    strong_banned = [
        "鎴戣繕鏄偅涓崟韬氦鏄撳憳锛屽枩娆㈣繍鍔紝鏃╀笂璺戞銆佹櫄涓婂鐩橈紝蹇冮噷涓€鐩存儲璁扮潃閭ｅぉ鑳藉紑鐫€鑷繁鐨勬父鑹囧幓鐜父涓栫晫",
    ]
    out = strong_banned + blocked[:18]
    deduped: List[str] = []
    seen = set()
    for p in out:
        if p not in seen:
            deduped.append(p)
            seen.add(p)
    return deduped


def _dedupe_phrase(text: str, blocked_phrases: List[str]) -> str:
    out = str(text or "")
    if not out or not blocked_phrases:
        return out
    for p in blocked_phrases:
        phrase = str(p or "").strip()
        if len(phrase) < 6:
            continue
        if phrase in out:
            out = out.replace(phrase, "")
    out = re.sub(r"[ \t]+", " ", out)
    out = re.sub(r"([锛屻€傦紒锛燂紱]){2,}", r"\1", out)
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out


def _is_v2_diary_rule_text(text: str) -> bool:
    t = str(text or "").strip()
    if not t:
        return False
    tokens = [
        "策略",
        "仓位",
        "单票",
        "总仓",
        "行业top3",
        "行业前3",
        "回踩",
        "ma10",
        "ma20",
        "止损位",
        "候选池",
        "权重",
        "中证500状态",
        "风控拦截",
        "风控下调",
        "llm",
        "不追高",
    ]
    lower = t.lower()
    if any(tok in lower for tok in tokens):
        return True
    if re.search(r"(\d+(\.\d+)?%)|([<>]=?)|(\bma\d+\b)", lower):
        return True
    return False


def _soften_v2_diary_text(text: str, fallback: str) -> str:
    t = str(text or "").strip()
    if not t:
        return fallback
    if _is_v2_diary_rule_text(t):
        return fallback
    t = re.sub(r"\s+", " ", t).strip("；;。 ")
    if not t:
        return fallback
    return t[:120]


def _strip_v2_diary_rule_lines(text: str, fallback: str) -> str:
    raw = str(text or "")
    if not raw:
        return fallback
    kept: List[str] = []
    for ln in raw.splitlines():
        line = str(ln or "").strip()
        if not line:
            kept.append("")
            continue
        if _is_v2_diary_rule_text(line):
            continue
        kept.append(ln)
    out = "\n".join(kept).strip()
    return out or fallback


def _build_review_payload(
    trade_date: str,
    nav_prev: float,
    nav_now: float,
    executed_trades: List[Dict[str, Any]],
    orders_audited: List[Dict[str, Any]],
    risk_notes: List[str],
    ai_payload: Dict[str, Any],
    candidates_df: pd.DataFrame,
    final_positions: Dict[str, Dict[str, Any]],
    config: Optional[Dict[str, Any]] = None,
    tool_calls: Optional[List[Dict[str, Any]]] = None,
    portfolio_id: str = OFFICIAL_PORTFOLIO_ID,
) -> Dict[str, Any]:
    config = config or {}
    tool_calls = tool_calls or []
    is_v2 = str(portfolio_id or "").strip() == OFFICIAL_PORTFOLIO_2_ID

    pnl = nav_now - nav_prev
    pnl_pct = (pnl / nav_prev) if nav_prev > 0 else 0.0
    blocked_phrases = _load_recent_diary_snippets(portfolio_id=portfolio_id, trade_date=trade_date, days=7)
    rng = random.Random(f"{trade_date}|{nav_now:.2f}|{len(executed_trades)}|{portfolio_id}")

    held_symbols = set(final_positions.keys())
    traded_symbols = {
        _normalize_symbol(str(t.get("symbol") or ""))
        for t in executed_trades
        if _normalize_symbol(str(t.get("symbol") or ""))
    }

    watchlist: List[Dict[str, Any]] = []
    if not candidates_df.empty:
        cdf = candidates_df.copy()
        cdf["score"] = pd.to_numeric(cdf.get("score"), errors="coerce").fillna(0.0)
        cdf = cdf.sort_values(["score", "amount"], ascending=[False, False], na_position="last")
        for _, row in cdf.iterrows():
            symbol = _normalize_symbol(row.get("symbol", ""))
            if not symbol:
                continue
            if symbol in held_symbols or symbol in traded_symbols:
                continue
            if int(_to_float(row.get("from_holdings_fallback"), 0.0)) == 1:
                continue
            watchlist.append(
                {
                    "symbol": symbol,
                    "name": str(row.get("name") or ""),
                    "score": round(_to_float(row.get("score"), 0.0), 2),
                }
            )
            if len(watchlist) >= 5:
                break

    def _fmt_td(td: str) -> str:
        d = re.sub(r"[^0-9]", "", str(td or ""))[:8]
        if len(d) == 8:
            return f"{d[:4]}-{d[4:6]}-{d[6:]}"
        return str(td or "-")

    def _reason_for_trade(symbol: str, side: str) -> str:
        side = str(side or "").lower()
        primary = "buy" if side == "buy" else "sell"
        symbol = _normalize_symbol(symbol)
        for o in orders_audited:
            if _normalize_symbol(o.get("symbol", "")) == symbol and str(o.get("action") or "").lower() == primary:
                txt = str(o.get("reason") or "").strip()
                if txt:
                    if is_v2:
                        return _soften_v2_diary_text(txt, "看着顺眼就上了一点，先拿小仓位试错。")
                    return txt
        for o in orders_audited:
            if _normalize_symbol(o.get("symbol", "")) == symbol:
                txt = str(o.get("reason") or "").strip()
                if txt:
                    if is_v2:
                        return _soften_v2_diary_text(txt, "这笔主要是顺势调整，动作不大。")
                    return txt
        return "这笔是顺着盘面的临场处理。"

    buys = [x for x in executed_trades if str(x.get("side") or "").lower() == "buy"]
    sells = [x for x in executed_trades if str(x.get("side") or "").lower() == "sell"]

    ai_summary = str(ai_payload.get("summary") or "").strip() or "当日以结构性机会为主"
    ai_risk = str(ai_payload.get("risk_notes") or "").strip() or "风险集中在节奏与仓位控制"
    if is_v2:
        ai_summary = _soften_v2_diary_text(ai_summary, "今天盘面机会不算多，我更想把节奏放慢一点。")
        ai_risk = _soften_v2_diary_text(ai_risk, "明天继续看市场给不给更舒服的位置。")
    ai_source = str(ai_payload.get("source") or "").strip().lower()

    if is_v2:
        opener_pool = [
            f"{_fmt_td(trade_date)} 收盘后我先把成交回放了一遍，账户定格在 {nav_now:,.2f}，日内变化 {'+' if pnl >= 0 else ''}{pnl:,.2f}（{'+' if pnl_pct >= 0 else ''}{pnl_pct:.2%}）。",
            f"今天结算后净值是 {nav_now:,.2f}，和昨天相比 {'+' if pnl >= 0 else ''}{pnl:,.2f}。先把情绪放一边，按规则拆每一笔动作。",
        ]
        tone_pool = [
            "我不追高，今天依旧是等回踩、等确认，再出手。",
            "交易节奏宁可慢半拍，也不愿意在噪音里硬冲。",
            "盘面再热，也只做自己看得懂、拿得住的那一段。",
        ]
        life_pool = [
            "今天气温一上来就有点烦热，复盘时把空调开得很低，脑子反而更清醒。",
            "晚饭照旧偏辣，辛辣感会让我更快从盘面噪音里抽离出来。",
            "耳机里循环着五月天和阿信的歌，节拍刚好把复盘节奏稳住。",
        ]
    else:
        opener_pool = [
            f"{_fmt_td(trade_date)} 收盘后净值来到 {nav_now:,.2f}，较前一日 {'+' if pnl >= 0 else ''}{pnl:,.2f}（{'+' if pnl_pct >= 0 else ''}{pnl_pct:.2%}）。",
            f"盘后先做结算：账户收在 {nav_now:,.2f}，日变动 {'+' if pnl >= 0 else ''}{pnl:,.2f}。",
        ]
        tone_pool = [
            "先讲事实，再谈判断，情绪放到最后处理。",
            "今天以纪律执行为主，节奏上保持克制。",
        ]
        life_pool = [""]

    summary_blocks = [
        rng.choice(opener_pool),
        rng.choice(tone_pool),
        f"盘后再回看一遍，今天市场没有给出特别清晰的增量线索，我更愿意把动作放在确定性上。{ai_summary}。",
        f"风险这一侧，我给自己的备注是：{ai_risk}。",
    ]
    life_line = rng.choice(life_pool).strip()
    if life_line:
        summary_blocks.append(life_line)

    summary_md = "\n\n".join([f"### 复盘日记（{_fmt_td(trade_date)}）"] + summary_blocks)
    summary_md = _dedupe_phrase(summary_md, blocked_phrases)
    if is_v2:
        summary_md = _strip_v2_diary_rule_lines(
            summary_md,
            f"### 复盘日记（{_fmt_td(trade_date)}）\n\n今天盘面没有让我特别兴奋的点，我更愿意慢一点，先把状态留给明天。",
        )

    def _v2_no_trade_reason() -> str:
        if candidates_df.empty:
            return "今天盯盘下来没看到特别顺手的机会，我宁愿空着等下一拍。"
        cols = set(candidates_df.columns.tolist())
        if {"signal_active", "pullback_ready", "chase_ok"}.issubset(cols):
            cdf = candidates_df.copy()
            cdf["signal_active"] = pd.to_numeric(cdf["signal_active"], errors="coerce").fillna(0.0)
            cdf["pullback_ready"] = pd.to_numeric(cdf["pullback_ready"], errors="coerce").fillna(0.0)
            cdf["chase_ok"] = pd.to_numeric(cdf["chase_ok"], errors="coerce").fillna(0.0)
            eligible = cdf[(cdf["signal_active"] == 1) & (cdf["pullback_ready"] == 1) & (cdf["chase_ok"] == 1)]
            if eligible.empty:
                return "看了几只票，但位置都不够舒服，今天就没有硬下单。"
        return "盘面有点乱，我今天主要做观察，不急着把手伸出去。"

    if buys:
        lines = []
        for t in buys:
            symbol = _normalize_symbol(t.get("symbol", ""))
            qty = int(_to_float(t.get("quantity"), 0.0))
            px = _to_float(t.get("price"), 0.0)
            reason = _reason_for_trade(symbol, "buy")
            lines.append(f"{symbol} 买入 {qty:,} 股，成交价 {px:.3f}，原因：{reason}。")
        buys_md = "### 今天为什么买\n\n" + "\n".join(lines)
    else:
        if is_v2:
            buys_md = f"### 今天为什么买\n\n今天没有买入。{_v2_no_trade_reason()}"
        else:
            buys_md = "### 今天为什么买\n\n今天没有新开仓，主要是等更干净的回踩与确认信号。"
    buys_md = _dedupe_phrase(buys_md, blocked_phrases)
    if is_v2:
        buys_md = _strip_v2_diary_rule_lines(buys_md, "### 今天为什么买\n\n今天没有买入，先把手收回来，等更顺的时机。")

    if sells:
        lines = []
        for t in sells:
            symbol = _normalize_symbol(t.get("symbol", ""))
            qty = int(_to_float(t.get("quantity"), 0.0))
            px = _to_float(t.get("price"), 0.0)
            reason = _reason_for_trade(symbol, "sell")
            lines.append(f"{symbol} 卖出 {qty:,} 股，成交价 {px:.3f}，原因：{reason}。")
        sells_md = "### 今天为什么卖\n\n" + "\n".join(lines)
    else:
        if is_v2:
            sells_md = "### 今天为什么卖\n\n今天没有卖出。手里的票还在观察节奏里，暂时不急着动。"
        else:
            sells_md = "### 今天为什么卖\n\n今天没有主动卖出，持仓结构与风险预算仍在可控区间。"
    sells_md = _dedupe_phrase(sells_md, blocked_phrases)
    if is_v2:
        sells_md = _strip_v2_diary_rule_lines(sells_md, "### 今天为什么卖\n\n今天没有卖出，先稳住再说。")

    rejected = [o for o in orders_audited if str(o.get("gate_status") or "").lower() == "rejected"]
    adjusted = [o for o in orders_audited if str(o.get("gate_status") or "").lower() == "adjusted"]
    watch_symbols = [str(x.get("symbol") or "") for x in watchlist if str(x.get("symbol") or "").strip()]

    def _clean_note_text(raw: Any) -> str:
        txt = str(raw or "").strip()
        if not txt:
            return ""
        bad_tokens = [
            "HTTPConnectionPool",
            "dashscope.aliyuncs.com",
            "SSLError",
            "UNEXPECTED_EOF",
            "Max retries exceeded",
            "EOF occurred",
            "_ssl.c",
            "Traceback",
            "port=443",
        ]
        if any(tok in txt for tok in bad_tokens):
            return ""
        if "锛" in txt or "銆" in txt or "€" in txt:
            return ""
        txt = re.sub(r"\s+", " ", txt).strip("；;。 ")
        return txt[:100]

    risk_parts: List[str] = []
    clean_ai_risk = _clean_note_text(ai_risk)
    if clean_ai_risk:
        risk_parts.append(clean_ai_risk)
    if ai_source == "fallback_rule":
        risk_parts.append("模型服务短暂波动，今晚采用规则回退执行，风控与仓位纪律照常生效")
    for n in risk_notes:
        txt = _clean_note_text(n)
        if is_v2 and _is_v2_diary_rule_text(txt):
            continue
        if txt and txt not in risk_parts:
            risk_parts.append(txt)
    if rejected:
        risk_parts.append(f"风控拦截 {len(rejected)} 笔指令")
    if adjusted:
        risk_parts.append(f"风控下调 {len(adjusted)} 笔指令")
    if not risk_parts:
        risk_parts.append("风控层面平稳，无额外警报")
    if watch_symbols:
        risk_parts.append(f"明日优先跟踪：{'、'.join(watch_symbols[:5])}")

    risk_md = "### 明天继续盯什么\n\n" + "。".join(risk_parts) + "。"
    risk_md = _dedupe_phrase(risk_md, blocked_phrases)
    if is_v2:
        risk_md = _strip_v2_diary_rule_lines(
            risk_md,
            "### 明天继续盯什么\n\n先看开盘情绪有没有回暖，盯住手里和观察名单里的几只票，耐心等信号更清楚一点。",
        )

    def _build_llm_diary() -> Optional[Dict[str, str]]:
        if int(config.get("review_use_llm", 1)) != 1:
            return None
        api_key = os.getenv("DASHSCOPE_API_KEY")
        if not api_key:
            return None

        model_name = str(config.get("model_name") or DEFAULT_CONFIG["model_name"])
        llm = ChatTongyiCompat(model=model_name, streaming=False, temperature=0.55, api_key=api_key)

        facts = {
            "trade_date": trade_date,
            "nav_prev": round(nav_prev, 4),
            "nav_now": round(nav_now, 4),
            "pnl": round(pnl, 4),
            "pnl_pct": round(pnl_pct, 6),
            "ai_summary": ai_summary,
            "ai_risk_notes": ai_risk,
            "risk_notes": [
                x
                for x in risk_notes
                if (not is_v2) or (not _is_v2_diary_rule_text(str(x or "")))
            ],
            "executed_trades": [
                {
                    "symbol": _normalize_symbol(t.get("symbol", "")),
                    "side": str(t.get("side") or ""),
                    "quantity": int(_to_float(t.get("quantity"), 0.0)),
                    "price": round(_to_float(t.get("price"), 0.0), 4),
                    "reason": _reason_for_trade(str(t.get("symbol") or ""), str(t.get("side") or "")),
                }
                for t in executed_trades
            ],
            "watchlist": watchlist,
            "recent_diary_snippets": blocked_phrases,
        }

        persona_clause = (
            "人设要求：女性操盘手，高冷、克制、专业；怕热、爱吃辣；喜欢五月天乐团，喜欢阿信和他们的歌。"
            "文风要像真实交易员的盘后手记，不要撒娇，不要口号，不要鸡汤。"
            if is_v2
            else "文风要求：克制、专业、自然，像真实交易员盘后记录。"
        )

        prompt = (
            "你要根据事实数据写当天复盘日记，并严格只输出JSON："
            '{"summary_md":"...","buys_md":"...","sells_md":"...","risk_md":"..."}。'
            "要求："
            "1) summary_md 必须像公众号文章一样的盘后手记，段落连贯、有情绪和节奏，不要写成报告腔；"
            "2) buys_md/sells_md 要点名股票代码并解释买卖理由；"
            "3) risk_md 写明次日重点观察与风控动作；"
            "4) 禁止复用 recent_diary_snippets 中高频句式；"
            "5) 禁止编造事实，只能依据给定 facts。"
            "6) 不要写策略参数、仓位比例、均线名词、规则条款，也不要写选股逻辑口号（如不追高）。"
            f"\n\n{persona_clause}"
            f"\n\nfacts={json.dumps(facts, ensure_ascii=False)}"
        )

        try:
            rsp = llm.invoke([HumanMessage(content=prompt)])
            raw_text = str(getattr(rsp, "content", "") or "")
            parsed = _extract_json_from_text(raw_text)
            if not parsed:
                return None
            out = {
                "summary_md": _dedupe_phrase(str(parsed.get("summary_md") or "").strip(), blocked_phrases),
                "buys_md": _dedupe_phrase(str(parsed.get("buys_md") or "").strip(), blocked_phrases),
                "sells_md": _dedupe_phrase(str(parsed.get("sells_md") or "").strip(), blocked_phrases),
                "risk_md": _dedupe_phrase(str(parsed.get("risk_md") or "").strip(), blocked_phrases),
            }
            if is_v2:
                out["summary_md"] = _strip_v2_diary_rule_lines(
                    out["summary_md"],
                    f"### 复盘日记（{_fmt_td(trade_date)}）\n\n今天更像是耐心的一天，我没有被盘中噪音带着走。",
                )
                out["buys_md"] = _strip_v2_diary_rule_lines(
                    out["buys_md"],
                    "### 今天为什么买\n\n今天没有买入，先等市场把位置走顺一点。",
                )
                out["sells_md"] = _strip_v2_diary_rule_lines(
                    out["sells_md"],
                    "### 今天为什么卖\n\n今天没有卖出，节奏还在观察区间里。",
                )
                out["risk_md"] = _strip_v2_diary_rule_lines(
                    out["risk_md"],
                    "### 明天继续盯什么\n\n先看开盘强弱，再决定要不要动手。",
                )
            if all(out.values()):
                return out
        except Exception:
            return None
        return None

    llm_diary = _build_llm_diary()
    if llm_diary:
        summary_md = llm_diary["summary_md"]
        buys_md = llm_diary["buys_md"]
        sells_md = llm_diary["sells_md"]
        risk_md = llm_diary["risk_md"]

    return {
        "summary_md": summary_md,
        "buys_md": buys_md,
        "sells_md": sells_md,
        "risk_md": risk_md,
        "next_watchlist": watchlist,
    }

def _delete_existing_day(portfolio_id: str, trade_date: str) -> None:
    with engine.begin() as conn:
        for table in [
            "ai_sim_orders",
            "ai_sim_trades",
            "ai_sim_positions",
            "ai_sim_review_daily",
            "ai_sim_nav_daily",
        ]:
            conn.execute(
                text(f"DELETE FROM {table} WHERE portfolio_id = :pid AND trade_date = :td"),
                {"pid": portfolio_id, "td": trade_date},
            )


def _delete_from_day(portfolio_id: str, trade_date: str) -> None:
    with engine.begin() as conn:
        for table in [
            "ai_sim_orders",
            "ai_sim_trades",
            "ai_sim_positions",
            "ai_sim_review_daily",
            "ai_sim_nav_daily",
        ]:
            conn.execute(
                text(f"DELETE FROM {table} WHERE portfolio_id = :pid AND trade_date >= :td"),
                {"pid": portfolio_id, "td": trade_date},
            )


def run_daily_simulation(
    trade_date: Optional[str] = None,
    portfolio_id: str = OFFICIAL_PORTFOLIO_ID,
    force: bool = False,
) -> Dict[str, Any]:
    if engine is None:
        return {"status": "error", "error": "鏁版嵁搴撹繛鎺ヤ笉鍙敤"}

    ensure_ai_sim_tables()
    td = _normalize_trade_date(trade_date)

    config = _load_config(portfolio_id)
    if int(config.get("is_active", 1)) != 1:
        return {"status": "skipped", "reason": "portfolio inactive", "trade_date": td}

    latest_sql = text("SELECT MAX(trade_date) FROM ai_sim_nav_daily WHERE portfolio_id = :pid")
    with engine.connect() as conn:
        latest_td_raw = conn.execute(latest_sql, {"pid": portfolio_id}).scalar()
    latest_td = _normalize_trade_date(latest_td_raw) if latest_td_raw else ""

    if latest_td and td < latest_td:
        if not force:
            return {
                "status": "error",
                "reason": "trade_date older than latest settled day; use force to rewind",
                "trade_date": td,
                "latest_trade_date": latest_td,
                "portfolio_id": portfolio_id,
            }
        # 鍥炲ご閲嶇畻鑰佹棩鏈熸椂锛屽繀椤绘竻鐞嗚鏃ュ強涔嬪悗蹇収锛岄伩鍏嶆椂闂寸嚎鏂銆?
        _delete_from_day(portfolio_id, td)

    # 骞傜瓑锛氬凡瀛樺湪鍑€鍊煎垯涓嶉噸澶嶆墽琛?
    exists_sql = text(
        "SELECT 1 FROM ai_sim_nav_daily WHERE portfolio_id = :pid AND trade_date = :td LIMIT 1"
    )
    with engine.connect() as conn:
        existed = conn.execute(exists_sql, {"pid": portfolio_id, "td": td}).fetchone()
    if existed and not force:
        return {"status": "skipped", "reason": "already settled", "trade_date": td, "portfolio_id": portfolio_id}
    if existed and force:
        # 寮哄埗閲嶇畻褰撴棩锛氬厛娓呯悊鏃у揩鐓э紝閬垮厤 max_drawdown/鎶ヨ〃璇诲彇琚棫鍊兼薄鏌撱€?
        _delete_existing_day(portfolio_id, td)

    prev_nav_row = _get_previous_nav_row(portfolio_id, td)
    nav_prev = _to_float(prev_nav_row.get("nav"), INITIAL_CAPITAL) if prev_nav_row else INITIAL_CAPITAL
    cash_start = _to_float(prev_nav_row.get("cash"), INITIAL_CAPITAL) if prev_nav_row else INITIAL_CAPITAL
    prev_trade_date = prev_nav_row.get("trade_date") if prev_nav_row else None

    current_positions = _load_previous_positions(portfolio_id, td, prev_trade_date)

    context = SimulationContext(
        portfolio_id=portfolio_id,
        trade_date=td,
        prev_trade_date=prev_trade_date,
        cash=cash_start,
        nav_prev=nav_prev,
        current_positions=current_positions,
        config=config,
    )

    csi500_regime = _get_csi500_regime(td)
    recent_trade_memory = _load_recent_trade_memory(portfolio_id=portfolio_id, trade_date=td, days=5)
    is_v2 = _is_v2_portfolio(portfolio_id)
    if is_v2:
        candidates_df, sector_notes = _build_candidate_pool_v2(td, current_positions)
    else:
        candidates_df = _build_candidate_pool(td, current_positions)
        sector_notes = []

    style_map = _build_style_map(candidates_df)
    candidate_score_map = {
        _normalize_symbol(str(r.get("symbol") or "")): _to_float(r.get("score"), 0.0)
        for _, r in candidates_df.iterrows()
        if _normalize_symbol(str(r.get("symbol") or ""))
    }

    candidate_symbols = set(candidates_df["symbol"].tolist())
    candidate_symbols.update(current_positions.keys())
    all_symbols_for_price = sorted(set(candidate_symbols) | set(current_positions.keys()))
    price_map = _fetch_price_snapshot(all_symbols_for_price, td)
    current_weights = _current_weight_map(current_positions, price_map, context.nav_prev)

    if not is_v2:
        ai_payload, tool_calls, ai_warning = _generate_ai_actions_with_tools(
            portfolio_id=portfolio_id,
            trade_date=td,
            nav_prev=context.nav_prev,
            cash=context.cash,
            positions=current_positions,
            candidates_df=candidates_df,
            config=config,
            csi500_regime=csi500_regime,
            style_map=style_map,
            recent_trade_memory=recent_trade_memory,
        )

        raw_actions = _sanitize_actions(ai_payload.get("actions", []))
        audited_actions, target_weights, gate_notes = _apply_risk_gates(
            raw_actions=raw_actions,
            current_weights=current_weights,
            candidate_symbols=candidate_symbols,
            config=config,
            csi500_regime=csi500_regime,
            style_map=style_map,
            candidate_score_map=candidate_score_map,
        )
        if ai_warning:
            gate_notes.append(ai_warning)
    else:
        rule_targets, reason_map, rule_notes, forced_stop_sell, buy_eligible = _v2_build_rule_targets(
            current_positions=current_positions,
            current_weights=current_weights,
            candidates_df=candidates_df,
            price_map=price_map,
            csi500_regime=csi500_regime,
            max_positions=int(config.get("max_positions", 10)),
        )
        gate_notes = list(rule_notes)
        if sector_notes:
            gate_notes.append(f"行业Top3: {'、'.join(sector_notes)}")

        llm_payload, llm_tool_calls, llm_warning = _generate_ai_actions_with_tools(
            portfolio_id=portfolio_id,
            trade_date=td,
            nav_prev=context.nav_prev,
            cash=context.cash,
            positions=current_positions,
            candidates_df=candidates_df,
            config=config,
            csi500_regime=csi500_regime,
            style_map=style_map,
            recent_trade_memory=recent_trade_memory,
        )
        llm_actions = _sanitize_actions(llm_payload.get("actions", []))
        target_weights, llm_notes, override_symbols = _v2_merge_llm_targets(
            rule_targets=rule_targets,
            llm_actions=llm_actions,
            current_positions=current_positions,
            allowed_symbols=set(candidates_df["symbol"].tolist()) | set(current_positions.keys()),
            buy_eligible=buy_eligible,
            forced_stop_sell=forced_stop_sell,
            csi500_regime=csi500_regime,
            max_positions=int(config.get("max_positions", 10)),
        )
        gate_notes.extend(llm_notes)
        if llm_warning:
            gate_notes.append(llm_warning)

        audited_actions = _v2_targets_to_actions(
            target_weights=target_weights,
            current_weights=current_weights,
            reasons_map=reason_map,
            override_symbols=override_symbols,
        )
        ai_payload = {
            "summary": "2号策略：行业前3中选择强势底部突破，回踩MA10/MA20再买入，不追高。",
            "risk_notes": "收盘跌破止损位卖出；仓位按中证500多空状态动态限制。",
            "actions": audited_actions,
            "source": "llm_v2_hybrid" if override_symbols else "rule_v2",
        }
        tool_calls = llm_tool_calls
        ai_warning = llm_warning

    gate_notes.append(f"中证500状态: {csi500_regime.get('summary', '中性')}")

    planned_trades = _plan_trades(
        target_weights=target_weights,
        current_positions=current_positions,
        price_map=price_map,
        nav_prev=context.nav_prev,
        max_daily_trades=int(config.get("max_daily_trades", 5)),
        max_turnover_hard=_to_float(config.get("max_turnover_hard"), 0.60),
        has_prev_day=bool(prev_nav_row),
    )

    final_positions, executed_trades, cash_end = _execute_trades(
        planned_trades=planned_trades,
        current_positions=current_positions,
        price_map=price_map,
        cash_start=context.cash,
    )

    if is_v2:
        try:
            _save_v2_watchlist(
                portfolio_id=portfolio_id,
                trade_date=td,
                candidates_df=candidates_df,
                current_positions=current_positions,
                final_positions=final_positions,
                executed_trades=executed_trades,
            )
        except Exception as exc:
            gate_notes.append(f"2号自选池更新失败: {exc}")

    # 璁＄畻鍑€鍊?
    position_value = 0.0
    positions_snapshot: List[Dict[str, Any]] = []
    for symbol, pos in final_positions.items():
        close = _to_float(price_map.get(symbol, {}).get("close"), 0.0)
        qty = _to_float(pos.get("quantity"), 0.0)
        if close <= 0 or qty <= 0:
            continue
        mv = qty * close
        avg_cost = _to_float(pos.get("avg_cost"), 0.0)
        unrealized = (close - avg_cost) * qty
        position_value += mv
        positions_snapshot.append(
            {
                "symbol": symbol,
                "name": str(pos.get("name") or price_map.get(symbol, {}).get("name") or ""),
                "quantity": qty,
                "avg_cost": avg_cost,
                "close_price": close,
                "market_value": mv,
                "unrealized_pnl": unrealized,
                "weight": 0.0,
            }
        )

    nav_now = cash_end + position_value
    daily_ret = (nav_now / context.nav_prev - 1.0) if context.nav_prev > 0 else 0.0
    cum_ret = (nav_now / INITIAL_CAPITAL - 1.0)
    turnover = 0.0
    if prev_nav_row and context.nav_prev > 0:
        turnover = sum(_to_float(t.get("amount")) for t in executed_trades) / max(context.nav_prev * 2.0, 1e-9)

    for p in positions_snapshot:
        p["weight"] = p["market_value"] / max(nav_now, 1e-9)

    max_dd = _compute_max_drawdown(portfolio_id, nav_now)
    bench_hs300, bench_zz1000 = _compute_benchmark_values(portfolio_id, td, prev_nav_row)
    alpha_hs300 = cum_ret - (bench_hs300 - 1.0)
    alpha_zz1000 = cum_ret - (bench_zz1000 - 1.0)

    review_payload = _build_review_payload(
        trade_date=td,
        nav_prev=context.nav_prev,
        nav_now=nav_now,
        executed_trades=executed_trades,
        orders_audited=audited_actions,
        risk_notes=gate_notes,
        ai_payload=ai_payload,
        candidates_df=candidates_df,
        final_positions=final_positions,
        config=config,
        tool_calls=tool_calls,
        portfolio_id=portfolio_id,
    )

    _delete_existing_day(portfolio_id, td)

    with engine.begin() as conn:
        for action in audited_actions:
            order_id = str(uuid.uuid4())
            conn.execute(
                text(
                    """
                    INSERT INTO ai_sim_orders (
                        portfolio_id, trade_date, order_id, symbol, side, target_weight,
                        reason_short, reason_detail, confidence, gate_status, gate_notes,
                        raw_model_output_json
                    ) VALUES (
                        :portfolio_id, :trade_date, :order_id, :symbol, :side, :target_weight,
                        :reason_short, :reason_detail, :confidence, :gate_status, :gate_notes,
                        :raw_model_output_json
                    )
                    """
                ),
                {
                    "portfolio_id": portfolio_id,
                    "trade_date": td,
                    "order_id": order_id,
                    "symbol": action["symbol"],
                    "side": action["action"],
                    "target_weight": _to_float(action.get("target_weight"), 0.0),
                    "reason_short": str(action.get("reason") or "")[:255],
                    "reason_detail": str(action.get("reason") or ""),
                    "confidence": _to_float(action.get("confidence"), 0.0),
                    "gate_status": str(action.get("gate_status") or "pending"),
                    "gate_notes": str(action.get("gate_notes") or ""),
                    "raw_model_output_json": json.dumps(ai_payload, ensure_ascii=False),
                },
            )

        for trade in executed_trades:
            conn.execute(
                text(
                    """
                    INSERT INTO ai_sim_trades (
                        portfolio_id, trade_date, trade_id, order_id, symbol, side,
                        quantity, price, amount, realized_pnl, cost, slippage, exec_mode
                    ) VALUES (
                        :portfolio_id, :trade_date, :trade_id, :order_id, :symbol, :side,
                        :quantity, :price, :amount, :realized_pnl, :cost, :slippage, :exec_mode
                    )
                    """
                ),
                {
                    "portfolio_id": portfolio_id,
                    "trade_date": td,
                    "trade_id": str(uuid.uuid4()),
                    "order_id": "",
                    "symbol": trade["symbol"],
                    "side": trade["side"],
                    "quantity": _to_float(trade.get("quantity"), 0.0),
                    "price": _to_float(trade.get("price"), 0.0),
                    "amount": _to_float(trade.get("amount"), 0.0),
                    "realized_pnl": _to_float(trade.get("realized_pnl"), 0.0),
                    "cost": 0.0,
                    "slippage": 0.0,
                    "exec_mode": str(config.get("execution_mode") or "close_t0"),
                },
            )

        for row in positions_snapshot:
            conn.execute(
                text(
                    """
                    INSERT INTO ai_sim_positions (
                        portfolio_id, trade_date, symbol, name, quantity, avg_cost,
                        close_price, market_value, unrealized_pnl, weight
                    ) VALUES (
                        :portfolio_id, :trade_date, :symbol, :name, :quantity, :avg_cost,
                        :close_price, :market_value, :unrealized_pnl, :weight
                    )
                    """
                ),
                {
                    "portfolio_id": portfolio_id,
                    "trade_date": td,
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "quantity": _to_float(row["quantity"]),
                    "avg_cost": _to_float(row["avg_cost"]),
                    "close_price": _to_float(row["close_price"]),
                    "market_value": _to_float(row["market_value"]),
                    "unrealized_pnl": _to_float(row["unrealized_pnl"]),
                    "weight": _to_float(row["weight"]),
                },
            )

        conn.execute(
            text(
                """
                INSERT INTO ai_sim_nav_daily (
                    portfolio_id, trade_date, cash, position_value, nav, daily_return,
                    cum_return, max_drawdown, turnover, bench_hs300, bench_zz1000,
                    alpha_vs_hs300, alpha_vs_zz1000
                ) VALUES (
                    :portfolio_id, :trade_date, :cash, :position_value, :nav, :daily_return,
                    :cum_return, :max_drawdown, :turnover, :bench_hs300, :bench_zz1000,
                    :alpha_vs_hs300, :alpha_vs_zz1000
                )
                """
            ),
            {
                "portfolio_id": portfolio_id,
                "trade_date": td,
                "cash": cash_end,
                "position_value": position_value,
                "nav": nav_now,
                "daily_return": daily_ret,
                "cum_return": cum_ret,
                "max_drawdown": max_dd,
                "turnover": turnover,
                "bench_hs300": bench_hs300,
                "bench_zz1000": bench_zz1000,
                "alpha_vs_hs300": alpha_hs300,
                "alpha_vs_zz1000": alpha_zz1000,
            },
        )

        conn.execute(
            text(
                """
                INSERT INTO ai_sim_review_daily (
                    portfolio_id, trade_date, summary_md, buys_md, sells_md, risk_md,
                    next_watchlist_json, model_name, tool_calls_json
                ) VALUES (
                    :portfolio_id, :trade_date, :summary_md, :buys_md, :sells_md, :risk_md,
                    :next_watchlist_json, :model_name, :tool_calls_json
                )
                """
            ),
            {
                "portfolio_id": portfolio_id,
                "trade_date": td,
                "summary_md": review_payload["summary_md"],
                "buys_md": review_payload["buys_md"],
                "sells_md": review_payload["sells_md"],
                "risk_md": review_payload["risk_md"],
                "next_watchlist_json": json.dumps(review_payload["next_watchlist"], ensure_ascii=False),
                "model_name": str(config.get("model_name") or ""),
                "tool_calls_json": json.dumps(tool_calls, ensure_ascii=False),
            },
        )

    return {
        "status": "success",
        "portfolio_id": portfolio_id,
        "trade_date": td,
        "nav": round(nav_now, 2),
        "cash": round(cash_end, 2),
        "position_count": len(positions_snapshot),
        "trade_count": len(executed_trades),
        "daily_return": daily_ret,
        "cum_return": cum_ret,
        "turnover": turnover,
        "ai_source": str(ai_payload.get("source") or "unknown"),
        "ai_warning": ai_warning,
    }


def get_latest_snapshot(portfolio_id: str = OFFICIAL_PORTFOLIO_ID) -> Dict[str, Any]:
    ensure_ai_sim_tables()

    sql = text(
        """
        SELECT *
        FROM ai_sim_nav_daily
        WHERE portfolio_id = :pid
        ORDER BY trade_date DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"pid": portfolio_id}).mappings().fetchone()

    if not row:
        return {
            "has_data": False,
            "portfolio_id": portfolio_id,
            "initial_capital": INITIAL_CAPITAL,
            "message": "暂无模拟投资数据。",
        }

    data = dict(row)
    return {
        "has_data": True,
        "portfolio_id": portfolio_id,
        "trade_date": str(data.get("trade_date") or ""),
        "nav": _to_float(data.get("nav")),
        "cash": _to_float(data.get("cash")),
        "position_value": _to_float(data.get("position_value")),
        "daily_return": _to_float(data.get("daily_return")),
        "cum_return": _to_float(data.get("cum_return")),
        "max_drawdown": _to_float(data.get("max_drawdown")),
        "turnover": _to_float(data.get("turnover")),
        "bench_hs300": _to_float(data.get("bench_hs300")),
        "bench_zz1000": _to_float(data.get("bench_zz1000")),
        "alpha_vs_hs300": _to_float(data.get("alpha_vs_hs300")),
        "alpha_vs_zz1000": _to_float(data.get("alpha_vs_zz1000")),
        "initial_capital": INITIAL_CAPITAL,
    }


def get_nav_series(portfolio_id: str = OFFICIAL_PORTFOLIO_ID, days: int = 120) -> pd.DataFrame:
    ensure_ai_sim_tables()

    sql = text(
        """
        SELECT trade_date, nav, daily_return, cum_return, max_drawdown,
               turnover, bench_hs300, bench_zz1000, alpha_vs_hs300, alpha_vs_zz1000
        FROM ai_sim_nav_daily
        WHERE portfolio_id = :pid
        ORDER BY trade_date DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"pid": portfolio_id, "lim": int(days)})
    if df.empty:
        return df
    return df.sort_values("trade_date").reset_index(drop=True)


def get_review_dates(portfolio_id: str = OFFICIAL_PORTFOLIO_ID, limit: int = 180) -> List[str]:
    ensure_ai_sim_tables()

    sql = text(
        """
        SELECT DISTINCT trade_date
        FROM ai_sim_review_daily
        WHERE portfolio_id = :pid
        ORDER BY trade_date DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"pid": portfolio_id, "lim": int(limit)}).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def get_positions(
    portfolio_id: str = OFFICIAL_PORTFOLIO_ID,
    as_of_date: Optional[str] = None,
    strict_as_of: bool = False,
) -> pd.DataFrame:
    ensure_ai_sim_tables()
    as_of = _normalize_trade_date(as_of_date) if as_of_date else None

    if as_of:
        if strict_as_of:
            sql = text(
                """
                SELECT *
                FROM ai_sim_positions
                WHERE portfolio_id = :pid
                  AND trade_date = :td
                ORDER BY market_value DESC
                """
            )
            params = {"pid": portfolio_id, "td": as_of}
        else:
            sql = text(
                """
                SELECT *
                FROM ai_sim_positions
                WHERE portfolio_id = :pid
                  AND trade_date = (
                    SELECT MAX(trade_date)
                    FROM ai_sim_positions
                    WHERE portfolio_id = :pid2 AND trade_date <= :td
                  )
                ORDER BY market_value DESC
                """
            )
            params = {"pid": portfolio_id, "pid2": portfolio_id, "td": as_of}
    else:
        sql = text(
            """
            SELECT *
            FROM ai_sim_positions
            WHERE portfolio_id = :pid
              AND trade_date = (
                SELECT MAX(trade_date)
                FROM ai_sim_positions
                WHERE portfolio_id = :pid2
              )
            ORDER BY market_value DESC
            """
        )
        params = {"pid": portfolio_id, "pid2": portfolio_id}

    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def _recompute_realized_pnl_for_trades(trades_df: pd.DataFrame) -> pd.DataFrame:
    if trades_df.empty:
        return trades_df

    df = trades_df.copy()
    if "realized_pnl" not in df.columns:
        df["realized_pnl"] = 0.0

    df["_trade_date_norm"] = (
        df["trade_date"]
        .astype(str)
        .str.replace(r"[^0-9]", "", regex=True)
        .str[:8]
    )
    df["_created_at_sort"] = pd.to_datetime(df.get("created_at"), errors="coerce")
    if "id" in df.columns:
        df["_id_sort"] = pd.to_numeric(df["id"], errors="coerce")
    else:
        df["_id_sort"] = range(len(df))
    df["_row_idx"] = range(len(df))

    df = df.sort_values(
        by=["_trade_date_norm", "_created_at_sort", "_id_sort", "_row_idx"],
        ascending=True,
        kind="stable",
    ).reset_index(drop=True)

    state: Dict[str, Dict[str, float]] = {}
    realized_values: List[float] = []

    for _, row in df.iterrows():
        symbol = str(row.get("symbol") or "").strip()
        side = str(row.get("side") or "").strip().lower()
        qty = max(_to_float(row.get("quantity"), 0.0), 0.0)
        price = max(_to_float(row.get("price"), 0.0), 0.0)

        if not symbol:
            realized_values.append(_to_float(row.get("realized_pnl"), 0.0))
            continue

        pos = state.setdefault(symbol, {"qty": 0.0, "avg_cost": 0.0})
        cur_qty = _to_float(pos.get("qty"), 0.0)
        cur_avg = _to_float(pos.get("avg_cost"), 0.0)

        realized = 0.0
        if side == "sell":
            exec_qty = min(qty, cur_qty)
            if exec_qty > 0 and price > 0:
                realized = (price - cur_avg) * exec_qty
                left_qty = cur_qty - exec_qty
                pos["qty"] = left_qty
                if left_qty <= 0:
                    pos["qty"] = 0.0
                    pos["avg_cost"] = 0.0
        elif side == "buy":
            if qty > 0 and price > 0:
                new_qty = cur_qty + qty
                pos["avg_cost"] = ((cur_qty * cur_avg) + (qty * price)) / max(1e-9, new_qty)
                pos["qty"] = new_qty
        else:
            realized = _to_float(row.get("realized_pnl"), 0.0)

        realized_values.append(float(realized))

    df["realized_pnl"] = realized_values
    return df.drop(columns=["_trade_date_norm", "_created_at_sort", "_id_sort", "_row_idx"], errors="ignore")


def get_trades(portfolio_id: str = OFFICIAL_PORTFOLIO_ID, days: int = 20) -> pd.DataFrame:
    ensure_ai_sim_tables()

    sql = text(
        """
        SELECT *
        FROM ai_sim_trades
        WHERE portfolio_id = :pid
        ORDER BY trade_date ASC, created_at ASC, id ASC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"pid": portfolio_id})
    if df.empty:
        return df

    df = _recompute_realized_pnl_for_trades(df)
    df["_trade_date_norm"] = (
        df["trade_date"]
        .astype(str)
        .str.replace(r"[^0-9]", "", regex=True)
        .str[:8]
    )
    df["_created_at_sort"] = pd.to_datetime(df.get("created_at"), errors="coerce")
    if "id" in df.columns:
        df["_id_sort"] = pd.to_numeric(df["id"], errors="coerce")
    else:
        df["_id_sort"] = range(len(df))
    df = df.sort_values(
        by=["_trade_date_norm", "_created_at_sort", "_id_sort"],
        ascending=[False, False, False],
        kind="stable",
    ).head(int(days) * 20)
    return df.drop(columns=["_trade_date_norm", "_created_at_sort", "_id_sort"], errors="ignore").reset_index(drop=True)


def get_daily_review(portfolio_id: str = OFFICIAL_PORTFOLIO_ID, trade_date: Optional[str] = None) -> Dict[str, Any]:
    ensure_ai_sim_tables()
    td = _normalize_trade_date(trade_date) if trade_date else None

    if td:
        sql = text(
            """
            SELECT *
            FROM ai_sim_review_daily
            WHERE portfolio_id = :pid AND trade_date = :td
            LIMIT 1
            """
        )
        params = {"pid": portfolio_id, "td": td}
    else:
        sql = text(
            """
            SELECT *
            FROM ai_sim_review_daily
            WHERE portfolio_id = :pid
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        params = {"pid": portfolio_id}

    with engine.connect() as conn:
        row = conn.execute(sql, params).mappings().fetchone()

    if not row:
        return {
            "has_data": False,
            "portfolio_id": portfolio_id,
            "summary_md": "暂无复盘数据。",
            "buys_md": "",
            "sells_md": "",
            "risk_md": "",
            "next_watchlist": [],
            "tool_calls": [],
        }

    data = dict(row)
    try:
        watchlist = json.loads(data.get("next_watchlist_json") or "[]")
    except Exception:
        watchlist = []
    try:
        tool_calls = json.loads(data.get("tool_calls_json") or "[]")
    except Exception:
        tool_calls = []

    return {
        "has_data": True,
        "portfolio_id": portfolio_id,
        "trade_date": str(data.get("trade_date") or ""),
        "summary_md": str(data.get("summary_md") or ""),
        "buys_md": str(data.get("buys_md") or ""),
        "sells_md": str(data.get("sells_md") or ""),
        "risk_md": str(data.get("risk_md") or ""),
        "next_watchlist": watchlist,
        "model_name": str(data.get("model_name") or ""),
        "tool_calls": tool_calls,
    }


def get_watchlist(
    portfolio_id: str = OFFICIAL_PORTFOLIO_2_ID,
    as_of_date: Optional[str] = None,
    limit: int = 40,
    statuses: Optional[List[str]] = None,
) -> pd.DataFrame:
    statuses = statuses or ["watching", "bought", "exited"]
    clean_statuses = [str(x).strip() for x in statuses if str(x).strip()]
    if not clean_statuses:
        clean_statuses = ["watching", "bought", "exited"]

    placeholders = ",".join([f":s{i}" for i in range(len(clean_statuses))])
    params: Dict[str, Any] = {"pid": portfolio_id, "lim": int(max(1, limit))}
    for i, stx in enumerate(clean_statuses):
        params[f"s{i}"] = stx

    date_cond = ""
    if as_of_date:
        date_cond = " AND (last_signal_date <= :td OR last_signal_date = '' OR last_signal_date IS NULL) "
        params["td"] = re.sub(r"[^0-9]", "", str(as_of_date))[:8]

    sql = text(
        f"""
        SELECT *
        FROM ai_sim_watchlist
        WHERE portfolio_id = :pid
          AND status IN ({placeholders})
          {date_cond}
        ORDER BY updated_at DESC
        LIMIT :lim
        """
    )
    try:
        with engine.connect() as conn:
            return pd.read_sql(sql, conn, params=params)
    except Exception:
        return pd.DataFrame(
            columns=[
                "symbol",
                "name",
                "status",
                "sector_name",
                "score",
                "breakout_date",
                "breakout_price",
                "stop_price",
                "ma10",
                "ma20",
                "pullback_ready",
                "chase_ok",
                "last_signal_date",
            ]
        )


if __name__ == "__main__":
    results: List[Dict[str, Any]] = []
    for pid in [OFFICIAL_PORTFOLIO_ID, OFFICIAL_PORTFOLIO_2_ID]:
        try:
            results.append(run_daily_simulation(portfolio_id=pid))
        except Exception as exc:
            results.append(
                {
                    "status": "error",
                    "portfolio_id": pid,
                    "error": str(exc),
                }
            )
    print(json.dumps({"results": results}, ensure_ascii=False, indent=2))
