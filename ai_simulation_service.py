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
INITIAL_CAPITAL = 1_000_000.0
DEFAULT_LOT_SIZE = 100


DEFAULT_CONFIG: Dict[str, Any] = {
    "portfolio_id": OFFICIAL_PORTFOLIO_ID,
    "model_name": "qwen3.5-plus",
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
        raise ValueError("数据库连接不可用")

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

        conn.execute(
            text(
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
            ),
            DEFAULT_CONFIG,
        )


def _normalize_trade_date(trade_date: Optional[str]) -> str:
    if not trade_date:
        trade_date = str(get_latest_data_date())
    normalized = re.sub(r"[^0-9]", "", str(trade_date))[:8]
    if len(normalized) != 8:
        raise ValueError(f"trade_date 非法: {trade_date}")
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
        if "ST" in name.upper() or "*ST" in name.upper() or "退" in name:
            continue
        if not _is_valid_universe_symbol(symbol, name):
            continue

        close = _to_float(price_info.get("close"), _to_float(r.get("close")))
        amount = _to_float(price_info.get("amount"), 0.0)
        if close <= 0:
            continue

        # 候选池过滤：流动性、极端低价保护
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

    # 候选池过严时，降级放宽流动性门槛，防止长期空仓
    if not rows:
        for _, r in base_df.sort_values(["score"], ascending=[False]).head(limit * 2).iterrows():
            symbol = r["symbol"]
            price_info = price_map.get(symbol)
            if not price_info:
                continue
            name = str(r.get("name") or price_info.get("name") or "")
            if "ST" in name.upper() or "*ST" in name.upper() or "退" in name:
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

    # 确保当前持仓不会被过滤掉（用于减仓/清仓）
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
        summary = "中证500偏空，建议防守或显著降仓。"
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
        # 极弱环境允许全现金观望。
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
            "summary": "回退策略：当前市场环境不利，优先防守与保留现金。",
            "risk_notes": f"{csi500_regime.get('summary','中证500偏弱')}（回退模式）",
            "actions": actions,
            "source": "fallback_rule",
        }

    # 尝试保持“积极+稳健”风格共存（可用样本不足时自动退化）。
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
                    "reason": "不在当日优选候选池或市场偏弱，执行腾仓。",
                    "confidence": 0.5,
                }
            )

    return {
        "summary": f"使用规则回退策略：参考中证500({regime})并做风格混配。",
        "risk_notes": f"{csi500_regime.get('summary','当前为回退模式，建议检查模型服务可用性。')}",
        "actions": actions,
        "source": "fallback_rule",
    }


def _format_recent_trade_memory(trades_df: pd.DataFrame, max_days: int = 5, max_items_per_day: int = 4) -> str:
    if trades_df.empty:
        return "近5个交易日无历史成交记录。"

    df = trades_df.copy()
    df["trade_date"] = df["trade_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]
    df = df[df["trade_date"].str.len() == 8]
    if df.empty:
        return "近5个交易日无历史成交记录。"

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
            f"卖{len(sell_df)}笔/{sell_amt/1e4:.1f}万；主要动作：{'、'.join(move_parts) if move_parts else '无'}"
        )
        day_lines.append(line)

    if not day_lines:
        return "近5个交易日无历史成交记录。"
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
            f"- {symbol} {pos.get('name','')} qty={_to_float(pos.get('quantity')):.0f} avg_cost={_to_float(pos.get('avg_cost')):.3f}"
        )
    if not positions_lines:
        positions_lines = ["- 当前空仓"]

    top_candidates = candidates_df.head(60)
    cand_lines: List[str] = []
    for _, row in top_candidates.iterrows():
        symbol = str(row["symbol"])
        style = str(style_map.get(symbol) or "balanced")
        style_cn = "稳健" if style in {"steady", "balanced"} else "积极"
        cand_lines.append(
            f"- {symbol} {row['name']} style={style_cn} score={_to_float(row['score']):.2f} amount={_to_float(row['amount'])/1e8:.2f}亿 close={_to_float(row['close']):.3f}"
        )

    csi_summary = str(csi500_regime.get("summary") or "中证500技术面中性")
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
    {{"symbol":"600519.SH","action":"buy|sell|hold","target_weight":0.12,"reason":"简短理由","confidence":0.0-1.0}}
  ]
}}

要求：
- symbol必须是6位代码+后缀（.SH/.SZ/.BJ）。
- target_weight是组合目标权重（0~1）。
- action=buy/hold 时 target_weight > 0；action=sell 时 target_weight = 0。
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
{recent_trade_memory or "近5个交易日无历史成交记录。"}
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

    # 清理负权重
    target_weights = {k: max(0.0, _to_float(v)) for k, v in target_weights.items()}

    # 持仓数量硬限制
    non_zero = [(k, v) for k, v in target_weights.items() if v > 1e-6]
    non_zero.sort(key=lambda x: x[1], reverse=True)
    keep = dict(non_zero[:max_positions])
    dropped = [k for k, _ in non_zero[max_positions:]]
    for k in dropped:
        keep[k] = 0.0
    if dropped:
        risk_notes.append(f"持仓数超限，移除 {len(dropped)} 只低权重标的")

    # 市场择时仓位门禁（中证500技术面）
    stock_cap = _regime_stock_exposure_cap(csi500_regime)
    total_weight = sum(v for v in keep.values() if v > 0)
    if total_weight > stock_cap >= 0:
        scale = (stock_cap / total_weight) if total_weight > 0 else 1.0
        for k in list(keep.keys()):
            keep[k] = keep[k] * scale
        risk_notes.append(f"中证500择时约束触发，目标仓位按 {scale:.3f} 等比缩放")

    # 最终归一化保护
    total_weight = sum(v for v in keep.values() if v > 0)
    if total_weight > 1.0:
        scale = 1.0 / total_weight
        for k in list(keep.keys()):
            keep[k] = keep[k] * scale
        risk_notes.append("目标总权重>1，已归一化缩放")

    # 风格混配约束：非偏空市场尽量兼顾积极与稳健。
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
                    risk_notes.append(f"风格混配约束：补充稳健标的 {add_sym}，从 {donor} 划转 {shift:.2%} 权重")
                    active_symbols = [k for k, v in keep.items() if v > 1e-6]

        if not _has_aggr(active_symbols):
            add_sym = _pick_missing_style("aggressive")
            donor = _pick_donor({"steady", "balanced"})
            if add_sym and donor:
                shift = min(0.08, keep.get(donor, 0.0) * 0.35)
                if shift >= 0.02:
                    keep[donor] = max(0.0, keep.get(donor, 0.0) - shift)
                    keep[add_sym] = keep.get(add_sym, 0.0) + shift
                    risk_notes.append(f"风格混配约束：补充积极标的 {add_sym}，从 {donor} 划转 {shift:.2%} 权重")
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

    # 先卖后买，且按金额优先
    sells = sorted([x for x in drafts if x["side"] == "sell"], key=lambda x: x["amount"], reverse=True)
    buys = sorted([x for x in drafts if x["side"] == "buy"], key=lambda x: x["amount"], reverse=True)
    ordered = sells + buys

    if len(ordered) > max_daily_trades:
        ordered = ordered[:max_daily_trades]

    # 换手率口径：非首日才统计，且按成交额/2/总资金，避免首日建仓被误读为高换手。
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
        return True, "当日盈亏波动较大"

    note_text = " ".join([str(x) for x in risk_notes if str(x).strip()])
    stop_keywords = ["止损", "清仓", "割肉", "大幅回撤", "风控告警", "回撤"]
    if any(k in note_text for k in stop_keywords):
        return True, "出现明显情绪事件（止损/回撤）"

    sell_count = sum(1 for x in executed_trades if str(x.get("side") or "").lower() == "sell")
    if sell_count >= 3:
        return True, "当日卖出动作较多"

    digits = re.sub(r"[^0-9]", "", str(trade_date or ""))[:8]
    if len(digits) == 8:
        try:
            dt = datetime.strptime(digits, "%Y%m%d")
            if dt.weekday() == 4:
                return True, "周五收官日"
            if dt.day == monthrange(dt.year, dt.month)[1]:
                return True, "月末节点"
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
        parts = re.split(r"[，。！？；\n]", text_blob)
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
        "我还是那个单身交易员，喜欢运动，早上跑步、晚上复盘，心里一直惦记着那天能开着自己的游艇去环游世界",
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
    out = re.sub(r"([，。！？；]){2,}", r"\1", out)
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out


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

    pnl = nav_now - nav_prev
    pnl_pct = (pnl / nav_prev) if nav_prev > 0 else 0.0
    is_profit_day = pnl >= 0
    persona_trigger, persona_reason = _should_inject_persona(
        trade_date=trade_date,
        pnl_pct=pnl_pct,
        executed_trades=executed_trades,
        risk_notes=risk_notes,
    )
    blocked_phrases = _load_recent_diary_snippets(portfolio_id=portfolio_id, trade_date=trade_date, days=7)
    rng = random.Random(f"{trade_date}|{nav_now:.2f}|{len(executed_trades)}")

    rejected = [o for o in orders_audited if o.get("gate_status") == "rejected"]
    adjusted = [o for o in orders_audited if o.get("gate_status") == "adjusted"]

    held_symbols = set(final_positions.keys())
    traded_symbols = {
        _normalize_symbol(str(t.get("symbol") or ""))
        for t in executed_trades
        if _normalize_symbol(str(t.get("symbol") or ""))
    }
    watchlist = []
    for _, row in candidates_df.iterrows():
        s = _normalize_symbol(row.get("symbol", ""))
        score = _to_float(row.get("score"), 0.0)
        from_holdings_fallback = int(_to_float(row.get("from_holdings_fallback"), 0.0)) == 1
        if (
            not s
            or s in held_symbols
            or s in traded_symbols
            or from_holdings_fallback
            or score <= 0
        ):
            continue
        watchlist.append({
            "symbol": s,
            "name": str(row.get("name") or ""),
            "score": round(score, 2),
        })
        if len(watchlist) >= 5:
            break

    # 候选池质量偏弱时，放宽分数阈值，但仍避免把昨日持仓兜底行塞进观察列表。
    if not watchlist:
        for _, row in candidates_df.iterrows():
            s = _normalize_symbol(row.get("symbol", ""))
            from_holdings_fallback = int(_to_float(row.get("from_holdings_fallback"), 0.0)) == 1
            if not s or s in held_symbols or s in traded_symbols or from_holdings_fallback:
                continue
            watchlist.append({
                "symbol": s,
                "name": str(row.get("name") or ""),
                "score": round(_to_float(row.get("score"), 0.0), 2),
            })
            if len(watchlist) >= 5:
                break
    def _reason_for_trade(symbol: str, side: str) -> str:
        side = str(side or "").lower()
        primary_side = "buy" if side == "buy" else "sell"
        for o in orders_audited:
            if o.get("symbol") == symbol and str(o.get("action") or "").lower() == primary_side and str(o.get("reason") or "").strip():
                return str(o.get("reason") or "").strip()
        for o in orders_audited:
            if o.get("symbol") == symbol and str(o.get("reason") or "").strip():
                return str(o.get("reason") or "").strip()
        return "仓位与风险平衡调整。"

    def _industry_brief() -> str:
        if candidates_df.empty:
            return "板块层面今天没有拿到完整候选池数据，先以仓位稳定为主。"
        df = candidates_df.copy()
        df["industry"] = df["industry"].astype(str).replace({"": "未分类"})
        gp = (
            df.groupby("industry", dropna=False)
            .agg(score_mean=("score", "mean"), amount_sum=("amount", "sum"), cnt=("symbol", "count"))
            .sort_values(["score_mean", "amount_sum"], ascending=[False, False])
            .head(3)
            .reset_index()
        )
        if gp.empty:
            return "板块分化比较明显，今天没有形成单一主线。"
        parts = []
        for _, row in gp.iterrows():
            parts.append(
                f"{row['industry']}（均分{_to_float(row['score_mean']):.1f}，成交额约{_to_float(row['amount_sum'])/1e8:.1f}亿）"
            )
        return f"板块潜力上，今天更有交易温度的是：{'、'.join(parts)}。"

    def _money_flow_brief() -> str:
        if candidates_df.empty:
            return "资金流信息偏少，倾向于先看防守和仓位控制。"
        top_amt = _to_float(candidates_df.head(20)["amount"].sum(), 0.0)
        avg_pct = _to_float(candidates_df.head(20)["pct_chg"].mean(), 0.0)
        flow_tone = "偏进攻" if avg_pct > 0.8 else ("偏谨慎" if avg_pct < -0.8 else "分歧震荡")
        return f"从资金流看，候选池前20只合计成交额约 {top_amt/1e8:.1f} 亿，短线情绪是 {flow_tone}。"

    def _macro_policy_brief() -> str:
        ai_summary = str(ai_payload.get("summary") or "").strip()
        ai_risk = str(ai_payload.get("risk_notes") or "").strip()
        merged = f"{ai_summary} {ai_risk}".strip()
        policy_keywords = ["政策", "财政", "货币", "降息", "加息", "会议", "地产", "消费", "科技"]
        tool_names = {str(x.get("name") or "") for x in tool_calls}
        if any(k in merged for k in policy_keywords):
            return f"宏观和政策面今天给我的体感是：{merged}。"
        if tool_names:
            return "宏观和政策面没有看到明确超预期增量，更像是存量资金在熟悉赛道里做轮动。"
        return "宏观和政策面暂时没有突发变量，组合继续按照既定节奏做筛选与调仓。"

    def _technical_brief() -> str:
        if not executed_trades:
            if candidates_df.empty:
                return "技术面上信号并不集中，所以今天更偏向观望。"
            avg_pct = _to_float(candidates_df.head(15)["pct_chg"].mean(), 0.0)
            if avg_pct > 0.8:
                return "技术面上短线动量还在，但性价比一般，所以今天没有强行动手。"
            if avg_pct < -0.8:
                return "技术面上波动偏大，信号不够干净，今天先保留子弹。"
            return "技术面上结构分化，趋势和估值没有在同一时点共振。"
        trade_pct: List[float] = []
        if not candidates_df.empty:
            pct_map = {str(r["symbol"]): _to_float(r.get("pct_chg"), 0.0) for _, r in candidates_df.iterrows()}
            for t in executed_trades:
                trade_pct.append(pct_map.get(str(t["symbol"]), 0.0))
        avg_trade_pct = sum(trade_pct) / len(trade_pct) if trade_pct else 0.0
        if avg_trade_pct > 0.8:
            return "技术面上今天更偏顺势，买点主要落在趋势延续而不是逆势抄底。"
        if avg_trade_pct < -0.8:
            return "技术面上我们更偏左侧交易，趁回撤分批建仓。"
        return "技术面上今天以结构性机会为主，主要做强弱切换。"

    def _hot_topic_reflection() -> str:
        tool_names = {str(x.get("name") or "") for x in tool_calls}
        ai_text = f"{str(ai_payload.get('summary') or '')} {str(ai_payload.get('risk_notes') or '')}"
        hot_keywords = [
            "政策", "关税", "降息", "加息", "监管", "并购", "地缘", "冲突", "战争",
            "科技", "AI", "消费", "地产", "就业", "通胀", "财政", "货币",
        ]
        has_hotspot = ("get_financial_news" in tool_names) or any(k in ai_text for k in hot_keywords)
        if not has_hotspot:
            return ""
        reflections = [
            "热点越热，越要提醒自己尊重规则。我一直讨厌不公平，也更愿意在信息对等、机会平等的框架里做判断。",
            "新闻里情绪很满，但我还是把交易当作长期游戏：钱只是筹码，真正重要的是一路看到的风景和人与人之间的真诚连接。",
            "面对热点分歧，我更在意价值是否站得住。市场会给价格，时间会给答案，公平和尊重永远比一时输赢更稀缺。",
        ]
        return rng.choice(reflections)

    buys = [x for x in executed_trades if x["side"] == "buy"]
    sells = [x for x in executed_trades if x["side"] == "sell"]

    openers = [
        f"今天收盘后先把账本过了一遍，组合净值来到 {nav_now:,.2f}，相比昨天 {'+' if pnl >= 0 else ''}{pnl:,.2f}（{'+' if pnl_pct >= 0 else ''}{pnl_pct:.2%}）。",
        f"盘后第一件事还是看净值，今天收在 {nav_now:,.2f}，日内变化 {'+' if pnl >= 0 else ''}{pnl:,.2f}（{'+' if pnl_pct >= 0 else ''}{pnl_pct:.2%}）。",
        f"结算完成后账户定格在 {nav_now:,.2f}，和昨天比 {'+' if pnl >= 0 else ''}{pnl:,.2f}（{'+' if pnl_pct >= 0 else ''}{pnl_pct:.2%}），先把节奏记下来。",
    ]
    if is_profit_day:
        mood_pool = [
            "今天盘感和执行都在线，信号与动作基本对得上。",
            "今天是偏顺手的一天，仓位节奏踩得还算稳。",
            "今天收益是正的，心态上更敢做确认后的动作。",
        ]
    else:
        mood_pool = [
            "今天又给市场交了点学费，先自嘲一句手还不够稳，但纪律不能丢。",
            "今天并不顺，脸上笑不出来，但复盘要比情绪更诚实。",
            "今天吃了点亏，先认，再拆动作，明天才有机会把节奏拿回来。",
        ]

    persona_pool = [
        "收盘后照例去慢跑了几公里，把噪音先甩掉，再回来拆每一笔交易。",
        "跑步时一直在复盘仓位切换，脑子里那条“总有一天开游艇远航”的线，反而让我更愿意把每一步走稳。",
        "今天这种盘面最考验心态，运动完再看持仓，很多判断会更干净。",
    ]

    narrative_blocks = [
        rng.choice(openers),
        rng.choice(mood_pool),
        _macro_policy_brief(),
        _money_flow_brief(),
        _industry_brief(),
        _technical_brief(),
        f"AI 今天给出的核心判断是：{str(ai_payload.get('summary') or '暂无明确主线判断')}。",
    ]
    hot_reflection = _hot_topic_reflection()
    if hot_reflection:
        narrative_blocks.append(hot_reflection)
    if persona_trigger:
        insert_at = min(len(narrative_blocks), 2 + rng.randint(0, 2))
        narrative_blocks.insert(insert_at, rng.choice(persona_pool))

    summary_md = "\n\n".join([f"### 复盘日记（{trade_date}）"] + narrative_blocks)
    summary_md = _dedupe_phrase(summary_md, blocked_phrases)

    if buys:
        buy_sentences = []
        for t in buys:
            reason = _reason_for_trade(t["symbol"], "buy")
            buy_sentences.append(
                f"{t['symbol']} 这笔买入大概 {int(_to_float(t['quantity'])):,} 股，成交在 { _to_float(t['price']):.3f}，主要是因为{reason}"
            )
        buys_md = "### 今天为什么买\n\n" + "；".join(buy_sentences) + "。"
    else:
        buy_idle_pool = [
            "今天没有新增买入，主要是想把节奏放慢，等更清晰的信号。",
            "今天没有新开仓，信号还没到“必须出手”的程度，先耐心一点。",
            "今天选择不加仓，先把仓位和波动压住，等更高确定性的窗口。",
        ]
        buys_md = "### 今天为什么买\n\n" + rng.choice(buy_idle_pool)
    buys_md = _dedupe_phrase(buys_md, blocked_phrases)

    if sells:
        sell_sentences = []
        for t in sells:
            reason = _reason_for_trade(t["symbol"], "sell")
            sell_sentences.append(
                f"{t['symbol']} 这笔卖出大概 {int(_to_float(t['quantity'])):,} 股，成交在 { _to_float(t['price']):.3f}，背后考虑是{reason}"
            )
        sells_md = "### 今天为什么卖\n\n" + "；".join(sell_sentences) + "。"
    else:
        sell_idle_pool = [
            "今天没有主动减仓，主要是现有仓位和风险预算还在可接受区间。",
            "今天没有卖出动作，持仓结构暂时还能承受当前波动。",
            "今天先不动减仓按钮，优先观察信号是否延续。",
        ]
        sells_md = "### 今天为什么卖\n\n" + rng.choice(sell_idle_pool)
    sells_md = _dedupe_phrase(sells_md, blocked_phrases)

    gate_notes_text = "；".join([str(x) for x in risk_notes if str(x).strip()])
    gate_tail = []
    if rejected:
        gate_tail.append(f"门禁拦截了 {len(rejected)} 条指令")
    if adjusted:
        gate_tail.append(f"风控下调了 {len(adjusted)} 条指令")
    gate_extra = "；".join(gate_tail)
    if gate_notes_text and gate_extra:
        risk_body = f"风控侧今天主要提醒是：{gate_notes_text}。另外，{gate_extra}。"
    elif gate_notes_text:
        risk_body = f"风控侧今天主要提醒是：{gate_notes_text}。"
    elif gate_extra:
        risk_body = f"风控执行上，{gate_extra}。"
    else:
        risk_body = "风控层面今天比较平稳，没有额外告警。"

    watch_sentence = "、".join([str(x.get("symbol") or "") for x in watchlist if x.get("symbol")])
    if watch_sentence:
        risk_body += f" 明天优先盯住 {watch_sentence}。"

    risk_md = "### 明天继续盯什么\n\n" + risk_body
    risk_md = _dedupe_phrase(risk_md, blocked_phrases)

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
            "ai_summary": str(ai_payload.get("summary") or ""),
            "ai_risk_notes": str(ai_payload.get("risk_notes") or ""),
            "risk_notes": risk_notes,
            "tool_calls": [str(x.get("name") or "") for x in tool_calls][:12],
            "persona_trigger": persona_trigger,
            "persona_reason": persona_reason,
            "recent_diary_snippets": blocked_phrases,
            "value_principles": [
                "讨厌不公平，追求平等",
                "金钱只是世界游戏里的筹码",
                "体验与人类情感比短期输赢更重要",
            ],
            "executed_trades": [
                {
                    "symbol": str(t.get("symbol") or ""),
                    "side": str(t.get("side") or ""),
                    "quantity": int(_to_float(t.get("quantity"), 0.0)),
                    "price": round(_to_float(t.get("price"), 0.0), 4),
                    "amount": round(_to_float(t.get("amount"), 0.0), 2),
                    "reason": _reason_for_trade(str(t.get("symbol") or ""), str(t.get("side") or "")),
                }
                for t in executed_trades
            ],
            "candidate_top": [
                {
                    "symbol": str(r.get("symbol") or ""),
                    "industry": str(r.get("industry") or ""),
                    "score": round(_to_float(r.get("score"), 0.0), 2),
                    "pct_chg": round(_to_float(r.get("pct_chg"), 0.0), 2),
                    "amount_e8": round(_to_float(r.get("amount"), 0.0) / 1e8, 2),
                }
                for _, r in candidates_df.head(15).iterrows()
            ],
            "watchlist": watchlist,
        }
        prompt = (
            "你是交易员本人，要写一篇当天收盘后的复盘日记，语气平实、自然、有温度，像公众号散文，不要写成报告。"
            "请你严格只输出 JSON，格式如下："
            '{"summary_md":"...","buys_md":"...","sells_md":"...","risk_md":"..."}。'
            "要求："
            "1) summary_md 采用自由日记体，不要求固定段落模板，但要自然覆盖宏观、政策、资金流、板块潜力、技术面这五个维度；"
            "1.1) 若 persona_trigger=true，才允许轻描淡写提一次“单身/运动/游艇梦想”；若 persona_trigger=false，严禁出现这些词；"
            "1.2) 避免复用 recent_diary_snippets 里的高频句式；"
            "1.3) 若当日存在新闻热点，可加入一小段感想，价值观立场保持：讨厌不公平、追求平等、金钱只是筹码、体验与情感更重要；但不要说教，不要每段都讲价值观；"
            f"1.4) 当天盈亏={pnl:+.2f}，若盈利请更自信；若亏损请带一点自嘲但保持专业克制；"
            "2) buys_md/sells_md 要写清楚为什么买这只、为什么卖那只，并出现股票代码；"
            "3) 不要使用列表符号开头，不要公文腔；"
            "4) 事实不足时明确写“未看到明确增量”，不要编造。"
            f"\n\n当日事实数据：{json.dumps(facts, ensure_ascii=False)}"
        )
        try:
            rsp = llm.invoke([HumanMessage(content=prompt)])
            raw_text = str(getattr(rsp, "content", "") or "")
            parsed = _extract_json_from_text(raw_text)
            if not parsed:
                return None
            summary = str(parsed.get("summary_md") or "").strip()
            buys_text = str(parsed.get("buys_md") or "").strip()
            sells_text = str(parsed.get("sells_md") or "").strip()
            risk_text = str(parsed.get("risk_md") or "").strip()
            summary = _dedupe_phrase(summary, blocked_phrases)
            buys_text = _dedupe_phrase(buys_text, blocked_phrases)
            sells_text = _dedupe_phrase(sells_text, blocked_phrases)
            risk_text = _dedupe_phrase(risk_text, blocked_phrases)
            if summary and buys_text and sells_text and risk_text:
                return {
                    "summary_md": summary,
                    "buys_md": buys_text,
                    "sells_md": sells_text,
                    "risk_md": risk_text,
                }
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
        return {"status": "error", "error": "数据库连接不可用"}

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
        # 回头重算老日期时，必须清理该日及之后快照，避免时间线断裂。
        _delete_from_day(portfolio_id, td)

    # 幂等：已存在净值则不重复执行
    exists_sql = text(
        "SELECT 1 FROM ai_sim_nav_daily WHERE portfolio_id = :pid AND trade_date = :td LIMIT 1"
    )
    with engine.connect() as conn:
        existed = conn.execute(exists_sql, {"pid": portfolio_id, "td": td}).fetchone()
    if existed and not force:
        return {"status": "skipped", "reason": "already settled", "trade_date": td, "portfolio_id": portfolio_id}
    if existed and force:
        # 强制重算当日：先清理旧快照，避免 max_drawdown/报表读取被旧值污染。
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

    candidates_df = _build_candidate_pool(td, current_positions)
    style_map = _build_style_map(candidates_df)
    candidate_score_map = {
        _normalize_symbol(str(r.get("symbol") or "")): _to_float(r.get("score"), 0.0)
        for _, r in candidates_df.iterrows()
        if _normalize_symbol(str(r.get("symbol") or ""))
    }
    csi500_regime = _get_csi500_regime(td)
    recent_trade_memory = _load_recent_trade_memory(portfolio_id=portfolio_id, trade_date=td, days=5)

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

    # 允许交易标的 = 候选池 + 当前持仓
    candidate_symbols = set(candidates_df["symbol"].tolist())
    candidate_symbols.update(current_positions.keys())

    all_symbols_for_price = sorted(set(candidate_symbols) | set(current_positions.keys()))
    price_map = _fetch_price_snapshot(all_symbols_for_price, td)

    current_weights = _current_weight_map(current_positions, price_map, context.nav_prev)

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

    # 计算净值
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


if __name__ == "__main__":
    out = run_daily_simulation()
    print(json.dumps(out, ensure_ascii=False, indent=2))
