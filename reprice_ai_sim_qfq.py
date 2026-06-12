from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import text

import ai_simulation_service as sim


SUPPORTED_PORTFOLIOS = (sim.OFFICIAL_PORTFOLIO_ID, sim.OFFICIAL_PORTFOLIO_2_ID)


@dataclass
class MissingQfq:
    trade_date: str
    symbol: str
    usage: str


@dataclass
class RepriceComputation:
    portfolio_id: str
    start_date: str
    end_date: str
    trade_updates: List[Dict[str, Any]] = field(default_factory=list)
    position_rows: List[Dict[str, Any]] = field(default_factory=list)
    nav_updates: List[Dict[str, Any]] = field(default_factory=list)
    missing_qfq: List[MissingQfq] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing_qfq


def clean_date(value: Any, label: str = "date") -> str:
    cleaned = re.sub(r"[^0-9]", "", str(value or ""))[:8]
    if len(cleaned) != 8:
        raise argparse.ArgumentTypeError(f"{label} must look like YYYYMMDD")
    datetime.strptime(cleaned, "%Y%m%d")
    return cleaned


def normalize_symbol(symbol: Any) -> str:
    return sim._normalize_symbol(str(symbol or ""))


def parse_portfolio_ids(value: str) -> List[str]:
    raw = [x.strip() for x in re.split(r"[,，\s]+", str(value or "")) if x.strip()]
    return raw or list(SUPPORTED_PORTFOLIOS)


def _norm_trade_date_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8]


def _load_nav_rows(portfolio_id: str, start_date: str = "", end_date: str = "") -> pd.DataFrame:
    filters = ["portfolio_id = :pid"]
    params: Dict[str, Any] = {"pid": portfolio_id}
    if start_date:
        filters.append("trade_date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        filters.append("trade_date <= :end_date")
        params["end_date"] = end_date
    sql = text(
        f"""
        SELECT *
        FROM ai_sim_nav_daily
        WHERE {' AND '.join(filters)}
        ORDER BY trade_date ASC
        """
    )
    with sim.engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def _load_trade_rows(portfolio_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT *
        FROM ai_sim_trades
        WHERE portfolio_id = :pid
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        ORDER BY trade_date ASC, created_at ASC, id ASC
        """
    )
    with sim.engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"pid": portfolio_id, "start_date": start_date, "end_date": end_date})


def _load_qfq_rows(symbols: Sequence[str], start_date: str, end_date: str) -> pd.DataFrame:
    cleaned = sorted({normalize_symbol(s) for s in symbols if normalize_symbol(s)})
    if not cleaned:
        return pd.DataFrame(columns=["trade_date", "ts_code", "name", "close_price"])
    placeholders = ",".join(f":s{i}" for i in range(len(cleaned)))
    params: Dict[str, Any] = {f"s{i}": s for i, s in enumerate(cleaned)}
    params.update({"start_date": start_date, "end_date": end_date})
    sql = text(
        f"""
        SELECT trade_date, ts_code, name, close_price
        FROM stock_price_qfq
        WHERE trade_date >= :start_date
          AND trade_date <= :end_date
          AND ts_code IN ({placeholders})
          AND close_price IS NOT NULL
          AND close_price > 0
        """
    )
    with sim.engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def _trade_sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["_trade_date_norm"] = _norm_trade_date_series(out["trade_date"])
    out["_created_at_sort"] = pd.to_datetime(out.get("created_at"), errors="coerce")
    out["_id_sort"] = pd.to_numeric(out.get("id"), errors="coerce")
    out["_row_idx"] = range(len(out))
    return out.sort_values(
        ["_trade_date_norm", "_created_at_sort", "_id_sort", "_row_idx"],
        kind="stable",
    ).drop(columns=["_trade_date_norm", "_created_at_sort", "_id_sort", "_row_idx"], errors="ignore")


def _qfq_price_map(qfq_df: pd.DataFrame) -> Dict[Tuple[str, str], Dict[str, Any]]:
    if qfq_df.empty:
        return {}
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    work = qfq_df.copy()
    work["trade_date"] = _norm_trade_date_series(work["trade_date"])
    work["ts_code"] = work["ts_code"].map(normalize_symbol)
    work["close_price"] = pd.to_numeric(work["close_price"], errors="coerce")
    for _, row in work.dropna(subset=["close_price"]).iterrows():
        symbol = normalize_symbol(row.get("ts_code"))
        trade_date = clean_date(row.get("trade_date"))
        close = sim._to_float(row.get("close_price"), 0.0)
        if symbol and close > 0:
            out[(trade_date, symbol)] = {
                "name": str(row.get("name") or ""),
                "close": close,
            }
    return out


def _get_qfq(
    prices: Dict[Tuple[str, str], Dict[str, Any]],
    trade_date: str,
    symbol: str,
    usage: str,
    missing: List[MissingQfq],
) -> Optional[Dict[str, Any]]:
    key = (clean_date(trade_date), normalize_symbol(symbol))
    price = prices.get(key)
    if price and sim._to_float(price.get("close"), 0.0) > 0:
        return price
    fallback: Optional[Tuple[str, Dict[str, Any]]] = None
    for (price_date, price_symbol), price_info in prices.items():
        if price_symbol != key[1] or price_date > key[0]:
            continue
        if fallback is None or price_date > fallback[0]:
            fallback = (price_date, price_info)
    if fallback and sim._to_float(fallback[1].get("close"), 0.0) > 0:
        return fallback[1]
    missing.append(MissingQfq(trade_date=key[0], symbol=key[1], usage=usage))
    return None


def compute_reprice(
    portfolio_id: str,
    nav_df: pd.DataFrame,
    trades_df: pd.DataFrame,
    qfq_df: pd.DataFrame,
    initial_capital: float = sim.INITIAL_CAPITAL,
) -> RepriceComputation:
    if nav_df.empty:
        raise ValueError(f"{portfolio_id} has no ai_sim_nav_daily rows")

    nav = nav_df.copy()
    nav["trade_date"] = _norm_trade_date_series(nav["trade_date"])
    nav = nav.sort_values("trade_date").reset_index(drop=True)
    start_date = clean_date(nav.iloc[0]["trade_date"], "start_date")
    end_date = clean_date(nav.iloc[-1]["trade_date"], "end_date")

    trades = _trade_sort_frame(trades_df.copy())
    if not trades.empty:
        trades["trade_date"] = _norm_trade_date_series(trades["trade_date"])
        trades["symbol"] = trades["symbol"].map(normalize_symbol)

    qfq = _qfq_price_map(qfq_df)
    missing: List[MissingQfq] = []
    trade_updates: List[Dict[str, Any]] = []
    position_rows: List[Dict[str, Any]] = []
    nav_updates: List[Dict[str, Any]] = []

    positions: Dict[str, Dict[str, Any]] = {}
    cash = float(initial_capital)
    nav_prev: Optional[float] = None
    nav_values: List[float] = []
    trades_by_date = {
        d: g.copy()
        for d, g in trades.groupby("trade_date", sort=False)
    } if not trades.empty else {}

    original_bench = {
        clean_date(row["trade_date"]): {
            "bench_hs300": sim._to_float(row.get("bench_hs300"), 1.0),
            "bench_zz1000": sim._to_float(row.get("bench_zz1000"), 1.0),
        }
        for _, row in nav.iterrows()
    }

    for _, nav_row in nav.iterrows():
        trade_date = clean_date(nav_row["trade_date"])
        day_trades = trades_by_date.get(trade_date, pd.DataFrame())
        day_turnover_amount = 0.0

        for _, trade in day_trades.iterrows():
            symbol = normalize_symbol(trade.get("symbol"))
            side = str(trade.get("side") or "").strip().lower()
            qty = max(sim._to_float(trade.get("quantity"), 0.0), 0.0)
            price_info = _get_qfq(qfq, trade_date, symbol, "trade", missing)
            if not price_info:
                continue
            price = sim._to_float(price_info.get("close"), 0.0)
            amount = qty * price
            realized_pnl = 0.0
            pos = positions.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "name": str(trade.get("name") or price_info.get("name") or symbol),
                    "quantity": 0.0,
                    "avg_cost": 0.0,
                },
            )
            cur_qty = sim._to_float(pos.get("quantity"), 0.0)
            cur_avg = sim._to_float(pos.get("avg_cost"), 0.0)

            if side == "buy":
                new_qty = cur_qty + qty
                pos["avg_cost"] = ((cur_qty * cur_avg) + amount) / max(new_qty, 1e-9)
                pos["quantity"] = new_qty
                pos["name"] = str(trade.get("name") or price_info.get("name") or pos.get("name") or symbol)
                cash -= amount
            elif side == "sell":
                exec_qty = min(qty, cur_qty)
                if exec_qty > 0:
                    realized_pnl = (price - cur_avg) * exec_qty
                pos["quantity"] = max(cur_qty - qty, 0.0)
                if sim._to_float(pos.get("quantity"), 0.0) <= 0:
                    pos["quantity"] = 0.0
                    pos["avg_cost"] = 0.0
                cash += amount
            else:
                realized_pnl = sim._to_float(trade.get("realized_pnl"), 0.0)

            day_turnover_amount += amount
            trade_updates.append(
                {
                    "id": int(trade["id"]),
                    "price": float(price),
                    "amount": float(amount),
                    "realized_pnl": float(realized_pnl),
                }
            )

        positions = {
            symbol: pos
            for symbol, pos in positions.items()
            if sim._to_float(pos.get("quantity"), 0.0) > 0
        }

        day_position_rows: List[Dict[str, Any]] = []
        position_value = 0.0
        for symbol, pos in sorted(positions.items()):
            price_info = _get_qfq(qfq, trade_date, symbol, "position", missing)
            if not price_info:
                continue
            close = sim._to_float(price_info.get("close"), 0.0)
            qty = sim._to_float(pos.get("quantity"), 0.0)
            avg_cost = sim._to_float(pos.get("avg_cost"), 0.0)
            market_value = qty * close
            unrealized = (close - avg_cost) * qty
            position_value += market_value
            day_position_rows.append(
                {
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "name": str(pos.get("name") or price_info.get("name") or symbol),
                    "quantity": float(qty),
                    "avg_cost": float(avg_cost),
                    "close_price": float(close),
                    "market_value": float(market_value),
                    "unrealized_pnl": float(unrealized),
                    "weight": 0.0,
                }
            )

        nav_now = cash + position_value
        daily_return = 0.0 if nav_prev is None or nav_prev <= 0 else (nav_now / nav_prev - 1.0)
        cum_return = nav_now / max(float(initial_capital), 1e-9) - 1.0
        turnover = 0.0 if nav_prev is None or nav_prev <= 0 else day_turnover_amount / max(nav_prev * 2.0, 1e-9)

        nav_values.append(nav_now)
        series = pd.Series(nav_values, dtype=float)
        max_drawdown = float((series / series.cummax() - 1.0).min()) if not series.empty else 0.0

        for row in day_position_rows:
            row["weight"] = row["market_value"] / max(nav_now, 1e-9)
        position_rows.extend(day_position_rows)

        bench_hs300 = original_bench.get(trade_date, {}).get("bench_hs300", 1.0)
        bench_zz1000 = original_bench.get(trade_date, {}).get("bench_zz1000", 1.0)
        nav_updates.append(
            {
                "portfolio_id": portfolio_id,
                "trade_date": trade_date,
                "cash": float(cash),
                "position_value": float(position_value),
                "nav": float(nav_now),
                "daily_return": float(daily_return),
                "cum_return": float(cum_return),
                "max_drawdown": float(max_drawdown),
                "turnover": float(turnover),
                "bench_hs300": float(bench_hs300),
                "bench_zz1000": float(bench_zz1000),
                "alpha_vs_hs300": float(cum_return - (bench_hs300 - 1.0)),
                "alpha_vs_zz1000": float(cum_return - (bench_zz1000 - 1.0)),
            }
        )
        nav_prev = nav_now

    unique_missing = sorted({(m.trade_date, m.symbol, m.usage) for m in missing})
    missing_objects = [MissingQfq(*x) for x in unique_missing]
    return RepriceComputation(
        portfolio_id=portfolio_id,
        start_date=start_date,
        end_date=end_date,
        trade_updates=trade_updates,
        position_rows=position_rows,
        nav_updates=nav_updates,
        missing_qfq=missing_objects,
    )


def _required_symbols_from_trades(trades_df: pd.DataFrame) -> List[str]:
    if trades_df.empty or "symbol" not in trades_df.columns:
        return []
    return sorted({normalize_symbol(x) for x in trades_df["symbol"].tolist() if normalize_symbol(x)})


def load_and_compute(portfolio_id: str, start_date: str = "", end_date: str = "") -> RepriceComputation:
    if portfolio_id not in SUPPORTED_PORTFOLIOS:
        raise ValueError(f"只支持 1号/2号组合: {', '.join(SUPPORTED_PORTFOLIOS)}")
    sim.ensure_ai_sim_tables()
    all_nav_df = _load_nav_rows(portfolio_id)
    if all_nav_df.empty:
        raise ValueError(f"{portfolio_id} 没有可重算的净值数据")
    all_nav_df["trade_date"] = _norm_trade_date_series(all_nav_df["trade_date"])
    first_nav_date = clean_date(all_nav_df["trade_date"].min(), "first_nav_date")
    latest_nav_date = clean_date(all_nav_df["trade_date"].max(), "latest_nav_date")
    if start_date and clean_date(start_date, "start_date") > first_nav_date:
        raise ValueError(f"会计重算必须从首个净值日 {first_nav_date} 开始，不能从中间日期开始")
    if end_date and clean_date(end_date, "end_date") < latest_nav_date:
        raise ValueError(f"会计重算必须覆盖到最新净值日 {latest_nav_date}，不能只重算半截历史")
    nav_df = _load_nav_rows(portfolio_id, start_date=first_nav_date, end_date=latest_nav_date)
    nav_df["trade_date"] = _norm_trade_date_series(nav_df["trade_date"])
    calc_start = clean_date(nav_df["trade_date"].min(), "start_date")
    calc_end = clean_date(nav_df["trade_date"].max(), "end_date")
    trades_df = _load_trade_rows(portfolio_id, calc_start, calc_end)
    symbols = _required_symbols_from_trades(trades_df)
    qfq_df = _load_qfq_rows(symbols, calc_start, calc_end)
    return compute_reprice(portfolio_id, nav_df, trades_df, qfq_df)


def apply_reprice(result: RepriceComputation, dry_run: bool = False) -> None:
    if result.missing_qfq:
        raise RuntimeError(f"{result.portfolio_id} 前复权缺口未补齐，拒绝写入")
    if dry_run:
        return
    with sim.engine.begin() as conn:
        for row in result.trade_updates:
            conn.execute(
                text(
                    """
                    UPDATE ai_sim_trades
                    SET price = :price,
                        amount = :amount,
                        realized_pnl = :realized_pnl,
                        cost = 0,
                        slippage = 0
                    WHERE id = :id
                    """
                ),
                row,
            )

        conn.execute(
            text(
                """
                DELETE FROM ai_sim_positions
                WHERE portfolio_id = :pid
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                """
            ),
            {"pid": result.portfolio_id, "start_date": result.start_date, "end_date": result.end_date},
        )
        for row in result.position_rows:
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
                row,
            )

        for row in result.nav_updates:
            conn.execute(
                text(
                    """
                    UPDATE ai_sim_nav_daily
                    SET cash = :cash,
                        position_value = :position_value,
                        nav = :nav,
                        daily_return = :daily_return,
                        cum_return = :cum_return,
                        max_drawdown = :max_drawdown,
                        turnover = :turnover,
                        bench_hs300 = :bench_hs300,
                        bench_zz1000 = :bench_zz1000,
                        alpha_vs_hs300 = :alpha_vs_hs300,
                        alpha_vs_zz1000 = :alpha_vs_zz1000
                    WHERE portfolio_id = :portfolio_id
                      AND trade_date = :trade_date
                    """
                ),
                row,
            )


def _print_result(result: RepriceComputation, dry_run: bool) -> None:
    print(
        f"{result.portfolio_id}: {result.start_date}->{result.end_date} "
        f"trades={len(result.trade_updates)} positions={len(result.position_rows)} "
        f"nav={len(result.nav_updates)} dry_run={dry_run}"
    )
    if result.missing_qfq:
        print(f"❌ 前复权缺口 {len(result.missing_qfq)} 个，未写入。样例：")
        for item in result.missing_qfq[:30]:
            print(f"  {item.trade_date} {item.symbol} usage={item.usage}")
    else:
        latest = result.nav_updates[-1] if result.nav_updates else {}
        print(
            "✅ 前复权会计重算通过"
            f" | latest_nav={sim._to_float(latest.get('nav'), 0.0):.2f}"
            f" | latest_cash={sim._to_float(latest.get('cash'), 0.0):.2f}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AI炒股1号/2号前复权会计重算，不重跑选股和大模型")
    parser.add_argument("--portfolio-id", default="", help="默认重算 1号和2号；也可传 official_cn_a_etf_v1 或 official_cn_a_etf_v2")
    parser.add_argument("--start-date", default=None, type=lambda v: clean_date(v, "start-date"))
    parser.add_argument("--end-date", default=None, type=lambda v: clean_date(v, "end-date"))
    parser.add_argument("--dry-run", action="store_true", help="只检查和打印结果，不写入数据库")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    portfolio_ids = parse_portfolio_ids(args.portfolio_id)
    exit_code = 0
    for portfolio_id in portfolio_ids:
        try:
            result = load_and_compute(portfolio_id, start_date=args.start_date, end_date=args.end_date)
            _print_result(result, dry_run=args.dry_run)
            if result.missing_qfq:
                exit_code = 2
                continue
            apply_reprice(result, dry_run=args.dry_run)
            if not args.dry_run:
                print(f"✅ {portfolio_id} 已写入前复权会计结果")
        except Exception as exc:
            exit_code = 1
            print(f"❌ {portfolio_id} 重算失败: {exc}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
