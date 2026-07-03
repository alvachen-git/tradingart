import unittest

import pandas as pd
from sqlalchemy import create_engine, text

import us_market_dashboard_data as dash
from us_options_ai_tools import build_us_option_market_profile, normalize_us_option_underlying


class USOptionsAIToolsTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)

    def _create_stock_prices(self, periods=80):
        start = pd.Timestamp("2025-10-28")
        rows = []
        for idx, day in enumerate(pd.date_range(start, periods=periods, freq="D")):
            close = 580 + idx * 0.35 + ((-1) ** idx) * 1.2
            rows.append(
                {
                    "date": day.strftime("%Y-%m-%d"),
                    "symbol": "SPY",
                    "open": close - 1,
                    "high": close + 2,
                    "low": close - 2,
                    "close": close,
                    "volume": 1000000 + idx,
                    "adjClose": close,
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

    def _create_option_tables(self, *, include_iv=True, include_metrics=True, metric_days=70, include_oi_cache=True):
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
            if include_iv:
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

            contract_rows = [
                ("O:SPY260220C00600000", "SPY", "C", 600, "2026-02-20", "SPY", "monthly", "physical"),
                ("O:SPY260220P00600000", "SPY", "P", 600, "2026-02-20", "SPY", "monthly", "physical"),
                ("O:SPY260115C00610000", "SPY", "C", 610, "2026-01-15", "SPY", "short_cycle", "physical"),
                ("O:SPY260115P00590000", "SPY", "P", 590, "2026-01-15", "SPY", "short_cycle", "physical"),
            ]
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['contracts']}
                    (option_ticker, underlying, call_put, strike, expiration_date, contract_root,
                     expiration_type, settlement_type, exercise_style, shares_per_contract, source, updated_at)
                    VALUES (:option_ticker, :underlying, :call_put, :strike, :expiration_date, :contract_root,
                            :expiration_type, :settlement_type, '', 100, 'test', '')
                    """
                ),
                [
                    {
                        "option_ticker": option_ticker,
                        "underlying": underlying,
                        "call_put": call_put,
                        "strike": strike,
                        "expiration_date": expiration_date,
                        "contract_root": contract_root,
                        "expiration_type": expiration_type,
                        "settlement_type": settlement_type,
                    }
                    for (
                        option_ticker,
                        underlying,
                        call_put,
                        strike,
                        expiration_date,
                        contract_root,
                        expiration_type,
                        settlement_type,
                    ) in contract_rows
                ],
            )
            daily_rows = [
                ("20260114", "O:SPY260220C00600000", 20, 100, 500),
                ("20260115", "O:SPY260220C00600000", 21, 100, 500),
                ("20260115", "O:SPY260220P00600000", 18, 90, 450),
                ("20260115", "O:SPY260115C00610000", 2, 250, 350),
                ("20260115", "O:SPY260115P00590000", 2.5, 300, 400),
            ]
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['daily']}
                    (trade_date, option_ticker, underlying, open, high, low, close, volume,
                     vwap, transactions, open_interest, source, updated_at)
                    VALUES (:trade_date, :option_ticker, 'SPY', :close, :close, :close, :close, :volume,
                            :close, 1, :open_interest, 'test', '')
                    """
                ),
                [
                    {
                        "trade_date": trade_date,
                        "option_ticker": option_ticker,
                        "close": close,
                        "volume": volume,
                        "open_interest": open_interest,
                    }
                    for trade_date, option_ticker, close, volume, open_interest in daily_rows
                ],
            )
            if include_iv:
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {names['iv']}
                        (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                         iv_source, open_interest, underlying_price, source, updated_at)
                        VALUES
                        ('20260115', 'O:SPY260220C00600000', 'SPY', .20, NULL, 'provider', 500, 600, 'test', ''),
                        ('20260115', 'O:SPY260220P00600000', 'SPY', NULL, .22, 'computed', 450, 600, 'test', ''),
                        ('20260115', 'O:SPY260115C00610000', 'SPY', .30, NULL, 'provider', 350, 600, 'test', ''),
                        ('20260115', 'O:SPY260115P00590000', 'SPY', .32, NULL, 'provider', 400, 600, 'test', '')
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
                metric_rows = []
                start = pd.Timestamp("2025-11-07") + pd.Timedelta(days=70 - metric_days)
                for idx, day in enumerate(pd.date_range(start, periods=metric_days, freq="D")):
                    metric_rows.append(
                        {
                            "trade_date": day.strftime("%Y%m%d"),
                            "underlying": "SPY",
                            "atm_iv_pct": 15.0 + idx * 0.1,
                            "iv_change_1d": 0.1,
                            "rv20_pct": 13.0,
                            "rv60_pct": 14.0,
                            "iv_rv20_spread": 2.0 + idx * 0.01,
                            "iv_30d": 20.0,
                            "iv_60d": 18.0,
                            "term_slope_30_60": -2.0,
                            "term_state": "Backwardation",
                            "skew_expiration": "2026-02-20",
                            "put_skew_5pct": 5.0,
                            "call_skew_5pct": -1.0,
                            "put_call_oi": 1.5,
                            "put_call_volume": 1.2,
                            "zero_dte_volume_share_pct": 35.0,
                            "top_oi_strike": 600.0,
                            "top_oi": 5000.0,
                            "top5_oi_share_pct": 22.0,
                            "total_open_interest": 100000.0,
                            "total_volume": 70000.0,
                            "provider_iv_rows": 1200,
                            "computed_iv_rows": 300,
                            "open_interest_rows": 8500,
                            "source": "local_metrics",
                            "updated_at": "",
                        }
                    )
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

            if include_oi_cache:
                conn.execute(
                    text(
                        """
                        CREATE TABLE us_option_oi_defense_daily (
                            trade_date TEXT,
                            date TEXT,
                            underlying TEXT,
                            underlying_close REAL,
                            call_strike REAL,
                            call_oi REAL,
                            call_distance_pct REAL,
                            call_expiration TEXT,
                            put_strike REAL,
                            put_oi REAL,
                            put_distance_pct REAL,
                            put_expiration TEXT,
                            total_call_oi REAL,
                            total_put_oi REAL,
                            put_call_oi REAL
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO us_option_oi_defense_daily
                        (trade_date, date, underlying, underlying_close, call_strike, call_oi,
                         call_distance_pct, call_expiration, put_strike, put_oi,
                         put_distance_pct, put_expiration, total_call_oi, total_put_oi, put_call_oi)
                        VALUES
                        ('20260115', '2026-01-15', 'SPY', 600, 610, 350, 1.67, '2026-01-15',
                         590, 400, -1.67, '2026-01-15', 850, 850, 1.0)
                        """
                    )
                )

    def test_normalize_rejects_non_us_underlying(self):
        symbol, reason = normalize_us_option_underlying("510500")

        self.assertEqual(symbol, "")
        self.assertIn("仅支持美股", reason)

    def test_profile_builds_fixed_report_from_local_metrics(self):
        self._create_stock_prices()
        self._create_option_tables()

        result = build_us_option_market_profile(
            "SPY",
            use_test_tables=True,
            engine=self.engine,
            use_dashboard_engine=False,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["underlying"], "SPY")
        self.assertEqual(result["trade_date"], "20260115")
        self.assertIn("【美股期权体检】SPY", result["report"])
        self.assertIn("ATM IV", result["report"])
        self.assertIn("Put/Call OI", result["report"])
        self.assertIn("OI防线", result["report"])
        self.assertIn("metrics", result)
        self.assertIn("chain_summary", result)
        self.assertAlmostEqual(result["metrics"]["put_call_oi"], 1.5)

    def test_profile_no_engine_degrades_without_connecting_real_db(self):
        result = build_us_option_market_profile(
            "SPY",
            engine=None,
            use_dashboard_engine=False,
        )

        self.assertEqual(result["status"], "no_data")
        self.assertIn("数据库未连接", result["report"])

    def test_profile_missing_tables_reports_no_data(self):
        result = build_us_option_market_profile(
            "SPY",
            use_test_tables=True,
            engine=self.engine,
            use_dashboard_engine=False,
        )

        self.assertEqual(result["status"], "no_data")
        self.assertIn("未找到该标的", result["report"])

    def test_profile_missing_iv_marks_gap(self):
        self._create_stock_prices()
        self._create_option_tables(include_iv=False, include_metrics=False, include_oi_cache=False)

        result = build_us_option_market_profile(
            "SPY",
            use_test_tables=True,
            engine=self.engine,
            use_dashboard_engine=False,
        )

        self.assertIn("IV数据不足", result["gaps"])
        self.assertIn("IV数据不足", result["report"])

    def test_profile_marks_insufficient_history(self):
        self._create_stock_prices()
        self._create_option_tables(metric_days=10)

        result = build_us_option_market_profile(
            "SPY",
            use_test_tables=True,
            engine=self.engine,
            use_dashboard_engine=False,
        )

        self.assertIn("历史IV样本不足（10/60）", result["gaps"])
        self.assertIn("历史IV样本不足", result["report"])

    def test_profile_falls_back_to_latest_available_date(self):
        self._create_stock_prices()
        self._create_option_tables()

        result = build_us_option_market_profile(
            "SPY",
            trade_date="2026-01-16",
            use_test_tables=True,
            engine=self.engine,
            use_dashboard_engine=False,
        )

        self.assertEqual(result["trade_date"], "20260115")
        self.assertIn("回退到 2026-01-15", result["date_note"])
        self.assertIn("日期说明", result["report"])


if __name__ == "__main__":
    unittest.main()
