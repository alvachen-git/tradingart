import argparse
from datetime import datetime

import pandas as pd

from backtest_engine import (
    _fetch_option_data,
    _normalize_etf_code,
    _get_multiplier,
    _normalize_call_put,
    _fetch_underlying_prices,
)


def pick_deep_otm_put(df_day):
    df_put = df_day[df_day["call_put"] == "P"].copy()
    if df_put.empty:
        return None
    df_put = df_put.sort_values(["exercise_price", "oi"], ascending=[True, False])
    return df_put.iloc[0]


def reconcile_deep_otm_put(underlying, start_date, end_date, fee_per_lot=2.0):
    underlying = _normalize_etf_code(underlying)
    df = _fetch_option_data("etf", underlying, start_date, end_date)
    if df.empty:
        print("No data")
        return

    df = df.dropna(subset=["close"])
    df["call_put"] = df["call_put"].apply(_normalize_call_put)
    df["exercise_price"] = pd.to_numeric(df["exercise_price"], errors="coerce")
    df = df.dropna(subset=["call_put", "exercise_price", "delist_date"])
    df["trade_date"] = df["trade_date"].astype(str)
    df["delist_date"] = df["delist_date"].astype(str)

    multiplier = _get_multiplier("etf", underlying)
    price_map = {(r["ts_code"], r["trade_date"]): r["close"] for _, r in df.iterrows()}

    dates = sorted(df["trade_date"].unique().tolist())
    current = None
    expiry = None
    entry_price = None
    entry_date = None
    fee_total = 0.0
    cum_pnl = 0.0

    daily_rows = []
    trades = []

    prev_date = None
    for date in dates:
        df_day = df[df["trade_date"] == date]
        df_valid = df_day[df_day["delist_date"] >= date]
        if current is None:
            if df_valid.empty:
                prev_date = date
                continue
            expiry = df_valid["delist_date"].min()
            df_exp = df_valid[df_valid["delist_date"] == expiry]
            pick = pick_deep_otm_put(df_exp)
            if pick is None:
                prev_date = date
                continue
            current = pick["ts_code"]
            entry_price = pick["close"]
            entry_date = date
            fee_total += fee_per_lot
            cum_pnl -= fee_per_lot

        # 日度 PnL
        if prev_date:
            p0 = price_map.get((current, prev_date))
            p1 = price_map.get((current, date))
            day_pnl = 0.0
            if p0 is not None and p1 is not None:
                day_pnl = (p1 - p0) * multiplier
                cum_pnl += day_pnl
            daily_rows.append(
                {
                    "date": date,
                    "ts_code": current,
                    "p0": p0,
                    "p1": p1,
                    "day_pnl": day_pnl,
                    "cum_pnl": cum_pnl,
                    "expiry": expiry,
                }
            )

        # 到期日换月
        if expiry and date >= expiry:
            exit_price = price_map.get((current, date))
            fee_close = 0.0
            if exit_price is not None and exit_price > 0:
                fee_close = fee_per_lot
                fee_total += fee_close
                cum_pnl -= fee_close

            trade_ret = (exit_price - entry_price) * multiplier if exit_price is not None else 0.0
            trade_ret -= fee_per_lot  # 开仓费
            trade_ret -= fee_close     # 平仓费（若有）

            trades.append(
                {
                    "entry_date": entry_date,
                    "exit_date": date,
                    "ts_code": current,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "trade_pnl": trade_ret,
                    "fee_open": fee_per_lot,
                    "fee_close": fee_close,
                }
            )

            # 换下一合约（同日）
            df_valid = df_day[df_day["delist_date"] >= date]
            if not df_valid.empty:
                expiry = df_valid["delist_date"].min()
                df_exp = df_valid[df_valid["delist_date"] == expiry]
                pick = pick_deep_otm_put(df_exp)
                if pick is not None:
                    current = pick["ts_code"]
                    entry_price = pick["close"]
                    entry_date = date
                    fee_total += fee_per_lot
                    cum_pnl -= fee_per_lot
                else:
                    current = None
            else:
                current = None

        prev_date = date

    print(f"Underlying: {underlying}")
    print(f"Range: {start_date} ~ {end_date}")
    print(f"Total PnL: {cum_pnl:.2f}")
    print(f"Total Fees: {fee_total:.2f}")

    df_daily = pd.DataFrame(daily_rows)
    df_trades = pd.DataFrame(trades)

    out_daily = f"reconcile_{underlying}_{start_date}_{end_date}_daily.csv"
    out_trades = f"reconcile_{underlying}_{start_date}_{end_date}_trades.csv"
    df_daily.to_csv(out_daily, index=False)
    df_trades.to_csv(out_trades, index=False)

    print(f"Daily saved: {out_daily}")
    print(f"Trades saved: {out_trades}")


def _pick_strike_from_mode(S: float, cp: str, strike_mode: str, manual_strike: float | None):
    if manual_strike is not None:
        return manual_strike
    mode = (strike_mode or "ATM").upper()
    if mode == "OTM10":
        return S * 1.10 if cp == "C" else S * 0.90
    return S


def _select_contract_nearest(df_exp: pd.DataFrame, cp: str, target: float):
    df_cp = df_exp[df_exp["call_put"] == cp].copy()
    if df_cp.empty:
        return None
    df_cp["diff"] = (df_cp["exercise_price"] - target).abs()
    df_cp = df_cp.sort_values(["diff", "oi"], ascending=[True, False])
    return df_cp.iloc[0]


def reconcile_single_sell(
    underlying,
    start_date,
    end_date,
    cp="C",
    fee_per_lot=2.0,
    strike_mode="ATM",
    manual_strike=None,
    lots=1,
):
    underlying = _normalize_etf_code(underlying)
    df = _fetch_option_data("etf", underlying, start_date, end_date)
    if df.empty:
        print("No data")
        return

    df = df.dropna(subset=["close"])
    df["call_put"] = df["call_put"].apply(_normalize_call_put)
    df["exercise_price"] = pd.to_numeric(df["exercise_price"], errors="coerce")
    df = df.dropna(subset=["call_put", "exercise_price", "delist_date"])
    df["trade_date"] = df["trade_date"].astype(str)
    df["delist_date"] = df["delist_date"].astype(str)

    multiplier = _get_multiplier("etf", underlying)
    price_map = {(r["ts_code"], r["trade_date"]): r["close"] for _, r in df.iterrows()}
    underlying_prices = _fetch_underlying_prices(underlying, start_date, end_date)

    dates = sorted(df["trade_date"].unique().tolist())
    current = None
    expiry = None
    entry_price = None
    entry_date = None
    fee_total = 0.0
    cum_pnl = 0.0

    daily_rows = []
    trades = []
    missing_dates = []

    prev_date = None
    for date in dates:
        df_day = df[df["trade_date"] == date]
        df_valid = df_day[df_day["delist_date"] >= date]
        if current is None:
            if df_valid.empty:
                prev_date = date
                continue
            expiry = df_valid["delist_date"].min()
            df_exp = df_valid[df_valid["delist_date"] == expiry]
            S = underlying_prices.get(date)
            if S is None:
                missing_dates.append(date)
                prev_date = date
                continue
            strike = _pick_strike_from_mode(S, cp, strike_mode, manual_strike)
            pick = _select_contract_nearest(df_exp, cp, strike)
            if pick is None:
                prev_date = date
                continue
            current = pick["ts_code"]
            entry_price = pick["close"]
            entry_date = date
            fee_total += fee_per_lot * lots
            cum_pnl -= fee_per_lot * lots

        # 日度 PnL（卖方）
        if prev_date:
            p0 = price_map.get((current, prev_date))
            p1 = price_map.get((current, date))
            day_pnl = 0.0
            if p0 is not None and p1 is not None:
                day_pnl = (p0 - p1) * multiplier * lots
                cum_pnl += day_pnl
            else:
                missing_dates.append(date)
            daily_rows.append(
                {
                    "date": date,
                    "ts_code": current,
                    "p0": p0,
                    "p1": p1,
                    "day_pnl": day_pnl,
                    "cum_pnl": cum_pnl,
                    "expiry": expiry,
                }
            )

        # 到期日换月
        if expiry and date >= expiry:
            exit_price = price_map.get((current, date))
            fee_close = fee_per_lot * lots
            fee_total += fee_close
            cum_pnl -= fee_close

            trade_ret = (entry_price - exit_price) * multiplier * lots if exit_price is not None else 0.0
            trade_ret -= fee_per_lot * lots  # 开仓费
            trade_ret -= fee_close          # 平仓费

            trades.append(
                {
                    "entry_date": entry_date,
                    "exit_date": date,
                    "ts_code": current,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "trade_pnl": trade_ret,
                    "fee_open": fee_per_lot * lots,
                    "fee_close": fee_close,
                }
            )

            # 换下一合约（同日）
            df_valid = df_day[df_day["delist_date"] >= date]
            if not df_valid.empty:
                expiry = df_valid["delist_date"].min()
                df_exp = df_valid[df_valid["delist_date"] == expiry]
                S = underlying_prices.get(date)
                if S is None:
                    current = None
                else:
                    strike = _pick_strike_from_mode(S, cp, strike_mode, manual_strike)
                    pick = _select_contract_nearest(df_exp, cp, strike)
                    if pick is not None:
                        current = pick["ts_code"]
                        entry_price = pick["close"]
                        entry_date = date
                        fee_total += fee_per_lot * lots
                        cum_pnl -= fee_per_lot * lots
                    else:
                        current = None
            else:
                current = None

        prev_date = date

    print(f"Underlying: {underlying}")
    print(f"Strategy: single_sell_{'call' if cp == 'C' else 'put'}")
    print(f"Range: {start_date} ~ {end_date}")
    print(f"Total PnL: {cum_pnl:.2f}")
    print(f"Total Fees: {fee_total:.2f}")
    if missing_dates:
        print(f"Missing days: {len(set(missing_dates))}")

    df_daily = pd.DataFrame(daily_rows)
    df_trades = pd.DataFrame(trades)

    tag = f"single_sell_{'call' if cp == 'C' else 'put'}"
    out_daily = f"reconcile_{underlying}_{start_date}_{end_date}_{tag}_daily.csv"
    out_trades = f"reconcile_{underlying}_{start_date}_{end_date}_{tag}_trades.csv"
    df_daily.to_csv(out_daily, index=False)
    df_trades.to_csv(out_trades, index=False)

    print(f"Daily saved: {out_daily}")
    print(f"Trades saved: {out_trades}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--fee", type=float, default=2.0)
    ap.add_argument("--strategy", default="deep_otm_put", choices=["deep_otm_put", "single_sell_call", "single_sell_put"])
    ap.add_argument("--strike-mode", default="ATM", choices=["ATM", "OTM10"])
    ap.add_argument("--manual-strike", type=float, default=None)
    ap.add_argument("--lots", type=int, default=1)
    args = ap.parse_args()

    if args.strategy == "deep_otm_put":
        reconcile_deep_otm_put(
            underlying=args.symbol,
            start_date=args.start,
            end_date=args.end,
            fee_per_lot=args.fee,
        )
    else:
        cp = "C" if args.strategy == "single_sell_call" else "P"
        reconcile_single_sell(
            underlying=args.symbol,
            start_date=args.start,
            end_date=args.end,
            cp=cp,
            fee_per_lot=args.fee,
            strike_mode=args.strike_mode,
            manual_strike=args.manual_strike,
            lots=args.lots,
        )


if __name__ == "__main__":
    main()
