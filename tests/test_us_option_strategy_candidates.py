import unittest

import pandas as pd
from sqlalchemy import create_engine, text

import us_market_dashboard_data as dash
from us_options_ai_tools import (
    build_us_option_strategy_candidates,
    normalize_us_option_strategy,
)


class USOptionStrategyCandidatesTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)

    def _create_stock_prices(self, *, close=100.0):
        rows = []
        for idx, day in enumerate(pd.date_range("2025-11-01", periods=90, freq="D")):
            price = close + ((idx % 7) - 3) * 0.25
            if day.strftime("%Y-%m-%d") == "2026-01-15":
                price = close
            rows.append(
                {
                    "date": day.strftime("%Y-%m-%d"),
                    "symbol": "SPY",
                    "open": price - 0.5,
                    "high": price + 1.0,
                    "low": price - 1.0,
                    "close": price,
                    "volume": 1000000 + idx,
                    "adjClose": price,
                }
            )
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE stock_prices (
                        date TEXT,
                        symbol TEXT,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        adjClose REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO stock_prices
                    (date, symbol, open, high, low, close, volume, adjClose)
                    VALUES (:date, :symbol, :open, :high, :low, :close, :volume, :adjClose)
                    """
                ),
                rows,
            )

    def _create_strategy_option_tables(
        self,
        *,
        include_iv=True,
        iv_value=0.45,
        low_liquidity=False,
        include_metrics=True,
        current_atm_iv=45.0,
        iv_underlying_price=100.0,
    ):
        names = dash.option_table_names(True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {names['contracts']} (
                        option_ticker TEXT,
                        underlying TEXT,
                        call_put TEXT,
                        strike REAL,
                        expiration_date TEXT,
                        contract_root TEXT,
                        expiration_type TEXT,
                        settlement_type TEXT,
                        exercise_style TEXT,
                        shares_per_contract REAL,
                        source TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {names['daily']} (
                        trade_date TEXT,
                        option_ticker TEXT,
                        underlying TEXT,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        vwap REAL,
                        transactions REAL,
                        open_interest REAL,
                        source TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {names['iv']} (
                        trade_date TEXT,
                        option_ticker TEXT,
                        underlying TEXT,
                        provider_iv REAL,
                        computed_iv REAL,
                        iv_source TEXT,
                        open_interest REAL,
                        underlying_price REAL,
                        source TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            if include_metrics:
                conn.execute(
                    text(
                        f"""
                        CREATE TABLE {names['metrics']} (
                            trade_date TEXT,
                            underlying TEXT,
                            atm_iv_pct REAL,
                            iv_change_1d REAL,
                            rv20_pct REAL,
                            rv60_pct REAL,
                            iv_rv20_spread REAL,
                            iv_30d REAL,
                            iv_60d REAL,
                            term_slope_30_60 REAL,
                            term_state TEXT,
                            skew_expiration TEXT,
                            put_skew_5pct REAL,
                            call_skew_5pct REAL,
                            put_call_oi REAL,
                            put_call_volume REAL,
                            zero_dte_volume_share_pct REAL,
                            top_oi_strike REAL,
                            top_oi REAL,
                            top5_oi_share_pct REAL,
                            total_open_interest REAL,
                            total_volume REAL,
                            provider_iv_rows INTEGER,
                            computed_iv_rows INTEGER,
                            open_interest_rows INTEGER,
                            source TEXT,
                            updated_at TEXT
                        )
                        """
                    )
                )

            option_rows = []
            prices = {
                ("P", 80): 0.30,
                ("P", 85): 0.70,
                ("P", 90): 1.30,
                ("P", 95): 2.60,
                ("P", 100): 5.00,
                ("C", 100): 5.20,
                ("C", 105): 2.80,
                ("C", 110): 1.40,
                ("C", 115): 0.80,
                ("C", 120): 0.40,
            }
            for (side, strike), close in prices.items():
                ticker = f"O:SPY260220{side}{int(strike * 1000):08d}"
                option_rows.append(
                    {
                        "option_ticker": ticker,
                        "underlying": "SPY",
                        "call_put": side,
                        "strike": strike,
                        "expiration_date": "2026-02-20",
                        "contract_root": "SPY",
                        "expiration_type": "monthly",
                        "settlement_type": "physical",
                        "exercise_style": "",
                        "shares_per_contract": 100,
                        "source": "test",
                        "updated_at": "",
                        "trade_date": "20260115",
                        "open": close,
                        "high": close,
                        "low": close,
                        "close": close,
                        "volume": 0 if low_liquidity else 200 + strike,
                        "vwap": close,
                        "transactions": 1,
                        "open_interest": 0 if low_liquidity else 1000 + strike,
                    }
                )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['contracts']}
                    (option_ticker, underlying, call_put, strike, expiration_date, contract_root,
                     expiration_type, settlement_type, exercise_style, shares_per_contract, source, updated_at)
                    VALUES (:option_ticker, :underlying, :call_put, :strike, :expiration_date, :contract_root,
                            :expiration_type, :settlement_type, :exercise_style, :shares_per_contract, :source, :updated_at)
                    """
                ),
                option_rows,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['daily']}
                    (trade_date, option_ticker, underlying, open, high, low, close, volume,
                     vwap, transactions, open_interest, source, updated_at)
                    VALUES (:trade_date, :option_ticker, :underlying, :open, :high, :low, :close,
                            :volume, :vwap, :transactions, :open_interest, :source, :updated_at)
                    """
                ),
                option_rows,
            )
            iv_rows = []
            if include_iv:
                for row in option_rows:
                    iv_rows.append(
                        {
                            "trade_date": row["trade_date"],
                            "option_ticker": row["option_ticker"],
                            "underlying": row["underlying"],
                            "provider_iv": iv_value,
                            "computed_iv": None,
                            "iv_source": "provider",
                            "open_interest": row["open_interest"],
                            "underlying_price": iv_underlying_price,
                            "source": "test",
                            "updated_at": "",
                        }
                    )
            else:
                for row in option_rows:
                    iv_rows.append(
                        {
                            "trade_date": row["trade_date"],
                            "option_ticker": row["option_ticker"],
                            "underlying": row["underlying"],
                            "provider_iv": None,
                            "computed_iv": None,
                            "iv_source": "",
                            "open_interest": row["open_interest"],
                            "underlying_price": iv_underlying_price,
                            "source": "test",
                            "updated_at": "",
                        }
                    )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price, source, updated_at)
                    VALUES (:trade_date, :option_ticker, :underlying, :provider_iv, :computed_iv,
                            :iv_source, :open_interest, :underlying_price, :source, :updated_at)
                    """
                ),
                iv_rows,
            )

            if include_metrics:
                metric_rows = []
                start = pd.Timestamp("2025-11-07")
                for idx, day in enumerate(pd.date_range(start, periods=70, freq="D")):
                    metric_rows.append(
                        {
                            "trade_date": day.strftime("%Y%m%d"),
                            "underlying": "SPY",
                            "atm_iv_pct": 15.0 + idx * 0.1,
                            "iv_change_1d": 0.4,
                            "rv20_pct": 18.0,
                            "rv60_pct": 17.0,
                            "iv_rv20_spread": 3.0,
                            "iv_30d": current_atm_iv,
                            "iv_60d": current_atm_iv - 2.0,
                            "term_slope_30_60": 2.0,
                            "term_state": "Backwardation",
                            "skew_expiration": "2026-02-20",
                            "put_skew_5pct": 4.0,
                            "call_skew_5pct": 1.0,
                            "put_call_oi": 1.1,
                            "put_call_volume": 1.0,
                            "zero_dte_volume_share_pct": 12.0,
                            "top_oi_strike": 100.0,
                            "top_oi": 5000.0,
                            "top5_oi_share_pct": 30.0,
                            "total_open_interest": 12000.0,
                            "total_volume": 3500.0,
                            "provider_iv_rows": 10,
                            "computed_iv_rows": 0,
                            "open_interest_rows": 10,
                            "source": "test",
                            "updated_at": "",
                        }
                    )
                metric_rows[-1]["trade_date"] = "20260115"
                metric_rows[-1]["atm_iv_pct"] = current_atm_iv
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {names['metrics']}
                        (trade_date, underlying, atm_iv_pct, iv_change_1d, rv20_pct, rv60_pct,
                         iv_rv20_spread, iv_30d, iv_60d, term_slope_30_60, term_state,
                         skew_expiration, put_skew_5pct, call_skew_5pct, put_call_oi,
                         put_call_volume, zero_dte_volume_share_pct, top_oi_strike, top_oi,
                         top5_oi_share_pct, total_open_interest, total_volume, provider_iv_rows,
                         computed_iv_rows, open_interest_rows, source, updated_at)
                        VALUES
                        (:trade_date, :underlying, :atm_iv_pct, :iv_change_1d, :rv20_pct, :rv60_pct,
                         :iv_rv20_spread, :iv_30d, :iv_60d, :term_slope_30_60, :term_state,
                         :skew_expiration, :put_skew_5pct, :call_skew_5pct, :put_call_oi,
                         :put_call_volume, :zero_dte_volume_share_pct, :top_oi_strike, :top_oi,
                         :top5_oi_share_pct, :total_open_interest, :total_volume, :provider_iv_rows,
                         :computed_iv_rows, :open_interest_rows, :source, :updated_at)
                        """
                    ),
                    metric_rows,
                )

    def _build(self, strategy, **kwargs):
        return build_us_option_strategy_candidates(
            "SPY",
            strategy=strategy,
            trade_date="2026-01-15",
            use_test_tables=True,
            engine=self.engine,
            use_dashboard_engine=False,
            **kwargs,
        )

    def test_strategy_aliases_cover_chinese_seller_terms(self):
        self.assertEqual(normalize_us_option_strategy("卖put")[0], "short_put")
        self.assertEqual(normalize_us_option_strategy("卖call")[0], "short_call")
        self.assertEqual(normalize_us_option_strategy("双卖")[0], "short_strangle")
        self.assertEqual(normalize_us_option_strategy("卖跨")[0], "short_straddle")
        self.assertEqual(normalize_us_option_strategy("铁鹰")[0], "iron_condor")

    def test_cash_secured_put_and_covered_call_candidates(self):
        self._create_stock_prices()
        self._create_strategy_option_tables()

        csp = self._build("cash_secured_put")
        covered = self._build("covered_call")

        self.assertEqual(csp["status"], "ok")
        self.assertEqual(csp["candidates"][0]["legs"][0]["action"], "SELL_PUT")
        self.assertIn("cash_secured_notional", csp["candidates"][0]["metrics"])
        self.assertEqual(covered["candidates"][0]["legs"][0]["action"], "SELL_CALL")
        self.assertIn("covered_stock_notional", covered["candidates"][0]["metrics"])
        self.assertIn("【美股期权策略候选】SPY", csp["report"])

    def test_bull_put_and_bear_call_spreads_return_defined_risk_metrics(self):
        self._create_stock_prices()
        self._create_strategy_option_tables()

        bull = self._build("bull_put_spread")
        bear = self._build("bear_call_spread")

        self.assertEqual(bull["status"], "ok")
        self.assertEqual([leg["action"] for leg in bull["candidates"][0]["legs"]], ["SELL_PUT", "BUY_PUT"])
        self.assertGreater(bull["candidates"][0]["metrics"]["max_loss"], 0)
        self.assertEqual(bear["status"], "ok")
        self.assertEqual([leg["action"] for leg in bear["candidates"][0]["legs"]], ["SELL_CALL", "BUY_CALL"])
        self.assertGreater(bear["candidates"][0]["metrics"]["max_loss"], 0)

    def test_naked_and_volatility_seller_strategies_are_valid_candidates_for_aggressive_user(self):
        self._create_stock_prices()
        self._create_strategy_option_tables(current_atm_iv=45.0)

        short_put = self._build("short_put", risk_preference="激进")
        short_call = self._build("short_call", risk_preference="激进")
        strangle = self._build("short_strangle", risk_preference="激进")
        straddle = self._build("short_straddle", risk_preference="激进")
        condor = self._build("iron_condor", risk_preference="激进")

        self.assertEqual(short_put["status"], "ok")
        self.assertIn("卖方策略可作为正式候选", short_put["suitability"])
        self.assertEqual(short_call["candidates"][0]["legs"][0]["action"], "SELL_CALL")
        self.assertEqual([leg["action"] for leg in strangle["candidates"][0]["legs"]], ["SELL_PUT", "SELL_CALL"])
        self.assertIn("max_loss_note", straddle["candidates"][0]["metrics"])
        self.assertEqual(len(condor["candidates"][0]["legs"]), 4)

    def test_low_iv_and_low_liquidity_are_reported_as_degraded(self):
        self._create_stock_prices()
        self._create_strategy_option_tables(low_liquidity=True, current_atm_iv=11.0, iv_value=0.11)

        result = self._build("short_strangle")

        self.assertEqual(result["status"], "ok")
        self.assertIn("偏低", result["suitability"])
        self.assertTrue(any("低流动性" in gap for gap in result["gaps"]))

    def test_missing_iv_underlying_price_and_dte_gaps_are_explicit(self):
        self._create_strategy_option_tables(include_iv=False, iv_underlying_price=None, include_metrics=False)

        missing_price = self._build("short_put")
        no_dte = self._build("short_put", dte_min=55, dte_max=65)

        self.assertTrue(any("标的收盘价缺失" in gap for gap in missing_price["gaps"]))
        self.assertTrue(any("IV缺失" in gap for gap in missing_price["gaps"]))
        self.assertEqual(no_dte["status"], "partial")
        self.assertTrue(any("未找到DTE 55-65" in gap for gap in no_dte["gaps"]))

    def test_missing_chain_and_non_us_underlying_degrade_without_fabrication(self):
        missing = self._build("short_put")
        unsupported = build_us_option_strategy_candidates(
            "510500",
            strategy="卖put",
            use_test_tables=True,
            engine=self.engine,
            use_dashboard_engine=False,
        )

        self.assertEqual(missing["status"], "no_data")
        self.assertFalse(missing["candidates"])
        self.assertEqual(unsupported["status"], "unsupported")
        self.assertIn("仅支持美股", unsupported["gaps"][0])


if __name__ == "__main__":
    unittest.main()
