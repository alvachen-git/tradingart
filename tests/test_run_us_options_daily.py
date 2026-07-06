import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine, text

import run_us_options_daily as job
import us_options_polygon as uop


NEW_OPTION_UNDERLYINGS = {
    "AVGO",
    "BABA",
    "BAC",
    "COIN",
    "DIS",
    "GOOGL",
    "HOOD",
    "INTC",
    "JPM",
    "MARA",
    "META",
    "MSFT",
    "MSTR",
    "MU",
    "NFLX",
    "PLTR",
    "RIVN",
    "SMCI",
    "SOFI",
    "TSM",
    "UBER",
    "WMT",
}

LATEST_BACKFILL_UNDERLYINGS = {
    "BABA",
    "BAC",
    "DIS",
    "JPM",
    "MARA",
    "MU",
    "RIVN",
    "SOFI",
    "UBER",
    "WMT",
}


class RunUSOptionsDailyTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)
        self._create_option_tables()

    def _create_option_tables(self):
        names = uop.table_names(use_test_tables=True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {names['contracts']} (
                        option_ticker TEXT NOT NULL PRIMARY KEY,
                        underlying TEXT NOT NULL,
                        call_put TEXT NOT NULL,
                        strike REAL NOT NULL,
                        expiration_date TEXT NOT NULL,
                        contract_root TEXT NOT NULL,
                        expiration_type TEXT NOT NULL,
                        settlement_type TEXT NOT NULL,
                        exercise_style TEXT,
                        shares_per_contract REAL,
                        source TEXT NOT NULL,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {names['daily']} (
                        trade_date TEXT NOT NULL,
                        option_ticker TEXT NOT NULL,
                        underlying TEXT NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        vwap REAL,
                        transactions REAL,
                        open_interest REAL,
                        source TEXT NOT NULL,
                        updated_at TEXT,
                        PRIMARY KEY (trade_date, option_ticker)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {names['iv']} (
                        trade_date TEXT NOT NULL,
                        option_ticker TEXT NOT NULL,
                        underlying TEXT NOT NULL,
                        provider_iv REAL,
                        computed_iv REAL,
                        iv_source TEXT,
                        open_interest REAL,
                        underlying_price REAL,
                        source TEXT NOT NULL,
                        updated_at TEXT,
                        PRIMARY KEY (trade_date, option_ticker)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {names['metrics']} (
                        trade_date TEXT NOT NULL,
                        underlying TEXT NOT NULL,
                        atm_iv_pct REAL,
                        put_call_oi REAL,
                        total_open_interest REAL,
                        provider_iv_rows INTEGER,
                        open_interest_rows INTEGER,
                        source TEXT NOT NULL,
                        updated_at TEXT,
                        PRIMARY KEY (trade_date, underlying)
                    )
                    """
                )
            )

    def test_daily_default_underlyings_follow_core_us_options_default(self):
        self.assertEqual(job.DEFAULT_UNDERLYINGS, uop.DEFAULT_UNDERLYINGS)
        self.assertIn("TSLA", job.DEFAULT_UNDERLYINGS)
        self.assertIn("GLD", job.DEFAULT_UNDERLYINGS)
        self.assertTrue(NEW_OPTION_UNDERLYINGS <= set(job.DEFAULT_UNDERLYINGS))
        self.assertNotIn("SPX", job.DEFAULT_UNDERLYINGS)

    def test_daily_shell_default_underlyings_include_new_symbols(self):
        script_text = Path("run_us_options_daily.sh").read_text(encoding="utf-8")

        self.assertIn("US_OPTIONS_UNDERLYINGS=", script_text)
        for symbol in NEW_OPTION_UNDERLYINGS:
            self.assertIn(symbol, script_text)
        self.assertNotIn("SPX", script_text)

    def test_backfill_script_defaults_to_new_batch(self):
        script_text = Path("scripts/backfill_us_options_new_underlyings_1y.sh").read_text(encoding="utf-8")
        expected = "BABA,BAC,DIS,JPM,MARA,MU,RIVN,SOFI,UBER,WMT"

        self.assertIn(f"US_OPTIONS_BACKFILL_UNDERLYINGS=\"${{US_OPTIONS_BACKFILL_UNDERLYINGS:-{expected}}}\"", script_text)
        self.assertEqual(set(expected.split(",")), LATEST_BACKFILL_UNDERLYINGS)

    def _insert_rows(self, *, open_interest=100, provider_iv=0.2, metrics=True):
        names = uop.table_names(use_test_tables=True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['contracts']}
                    (option_ticker, underlying, call_put, strike, expiration_date, contract_root,
                     expiration_type, settlement_type, exercise_style, shares_per_contract, source, updated_at)
                    VALUES
                    ('O:SPY260717C00600000', 'SPY', 'C', 600, '2026-07-17', 'SPY',
                     'monthly', 'physical', '', 100, 'massive', '')
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['daily']}
                    (trade_date, option_ticker, underlying, open, high, low, close, volume,
                     vwap, transactions, open_interest, source, updated_at)
                    VALUES
                    ('20260626', 'O:SPY260717C00600000', 'SPY', 2, 3, 1, 2.5, 100,
                     2.4, 20, :open_interest, 'massive', '')
                    """
                ),
                {"open_interest": open_interest},
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price, source, updated_at)
                    VALUES
                    ('20260626', 'O:SPY260717C00600000', 'SPY', :provider_iv, NULL,
                     'provider_snapshot', :open_interest, 600, 'massive', '')
                    """
                ),
                {"provider_iv": provider_iv, "open_interest": open_interest},
            )
            if metrics:
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {names['metrics']}
                        (trade_date, underlying, atm_iv_pct, put_call_oi, total_open_interest,
                         provider_iv_rows, open_interest_rows, source, updated_at)
                        VALUES
                        ('20260626', 'SPY', 20, 1.2, 100, 1, 1, 'massive', '')
                        """
                    )
                )

    def test_health_report_ok_with_oi_iv_and_metrics(self):
        self._insert_rows()

        report = job.build_health_report(
            engine=self.engine,
            update_result={},
            underlyings=["SPY"],
            trade_date="20260626",
            use_test_tables=True,
            dry_run=False,
        )

        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["checks"][0]["open_interest_rows"], 1)
        self.assertEqual(report["checks"][0]["provider_iv_rows"], 1)

    def test_health_report_flags_missing_open_interest(self):
        self._insert_rows(open_interest=None, metrics=False)

        report = job.build_health_report(
            engine=self.engine,
            update_result={},
            underlyings=["SPY"],
            trade_date="20260626",
            use_test_tables=True,
            dry_run=False,
        )

        self.assertEqual(report["status"], "health_failed")
        self.assertIn("missing_open_interest", {issue["code"] for issue in report["issues"]})

    def test_health_report_treats_all_empty_as_market_holiday(self):
        report = job.build_health_report(
            engine=self.engine,
            update_result={},
            underlyings=["SPY", "QQQ"],
            trade_date="20260626",
            use_test_tables=True,
            dry_run=False,
        )

        self.assertEqual(report["status"], "no_data_or_market_holiday")

    def test_dry_run_health_uses_snapshot_counts(self):
        report = job.build_health_report(
            engine=None,
            update_result={
                "per_underlying": {
                    "SPY": {
                        "daily": 10,
                        "iv": 10,
                        "open_interest_rows": 8,
                        "provider_iv_rows": 7,
                    }
                }
            },
            underlyings=["SPY"],
            trade_date="20260626",
            dry_run=True,
        )

        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["checks"][0]["open_interest_rows"], 8)

    def test_run_options_daily_calls_live_update_without_stock_update(self):
        fake_result = {
            "contracts": 1,
            "daily": 1,
            "iv": 1,
            "metrics": 0,
            "per_underlying": {"SPY": {"daily": 1, "iv": 1, "open_interest_rows": 1, "provider_iv_rows": 1}},
        }

        with patch("run_us_options_daily.MassiveOptionsClient", return_value=object()), \
             patch("run_us_options_daily.live_update", return_value=fake_result) as live_update:
            result = job.run_options_daily(date="20260626", underlyings=["SPY"], dry_run=True)

        self.assertEqual(result["status"], "ok")
        live_update.assert_called_once()
        self.assertEqual(live_update.call_args.kwargs["dry_run"], True)

    def test_live_update_reports_metrics_per_underlying(self):
        with patch.object(uop, "get_underlying_close", return_value=None), \
             patch.object(uop, "snapshot_rows_for_underlying", return_value=([], [], [], 600)), \
             patch.object(uop, "ensure_us_option_tables"), \
             patch.object(uop, "save_contracts"), \
             patch.object(uop, "save_daily"), \
             patch.object(uop, "save_iv"), \
             patch.object(uop, "compute_market_metrics_for_date", return_value=[SimpleNamespace(underlying="SPY")]), \
             patch.object(uop, "save_market_metrics", return_value=1):
            result = uop.live_update(object(), object(), ["SPY"], "20260626", dry_run=False)

        self.assertEqual(result["metrics"], 1)
        self.assertEqual(result["per_underlying"]["SPY"]["metrics"], 1)


if __name__ == "__main__":
    unittest.main()
