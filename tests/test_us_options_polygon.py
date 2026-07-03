import datetime as dt
import types
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

import us_options_polygon as uop


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class TestUSOptionsPolygon(unittest.TestCase):
    def test_default_underlyings_cover_dashboard_symbols_without_index_options(self):
        symbols = set(uop.DEFAULT_UNDERLYINGS)
        new_symbols = {
            "AVGO",
            "COIN",
            "GOOGL",
            "HOOD",
            "INTC",
            "META",
            "MSFT",
            "MSTR",
            "NFLX",
            "PLTR",
            "SMCI",
            "TSM",
        }

        self.assertFalse({"SPX", "NDX", "RUT", "VIX"} & symbols)
        for symbol in ("SPY", "QQQ", "IWM", "GLD", "TLT", "TSLA", "NVDA", "AMD", "AAPL", "AMZN"):
            self.assertIn(symbol, symbols)
        self.assertTrue(new_symbols <= symbols)
        self.assertEqual(len(uop.DEFAULT_UNDERLYINGS), 27)

    def test_parse_option_ticker_extracts_root_expiration_type_and_strike(self):
        parsed = uop.parse_option_ticker("O:SPY260619C00600000")

        self.assertEqual(parsed.root, "SPY")
        self.assertEqual(parsed.expiration_date, "2026-06-19")
        self.assertEqual(parsed.call_put, "C")
        self.assertEqual(parsed.strike, 600.0)

    def test_classify_spx_monthly_and_weekly(self):
        self.assertEqual(
            uop.classify_contract("O:SPX260619C06000000", "SPX", "2026-06-19")[:2],
            ("monthly", "AM"),
        )
        self.assertEqual(
            uop.classify_contract("O:SPXW260618P05500000", "SPX", "2026-06-18")[:2],
            ("short_cycle", "PM"),
        )

    def test_classify_etf_monthly_by_third_friday(self):
        monthly = uop.classify_contract("O:QQQ260619C00500000", "QQQ", "2026-06-19")
        weekly = uop.classify_contract("O:QQQ260612C00500000", "QQQ", "2026-06-12")

        self.assertEqual(monthly[0], "monthly")
        self.assertEqual(weekly[0], "short_cycle")

    def test_classify_equity_and_etf_underlyings_by_third_friday(self):
        tsla_monthly = uop.classify_contract("O:TSLA260619C00400000", "TSLA", "2026-06-19")
        tsla_weekly = uop.classify_contract("O:TSLA260612C00400000", "TSLA", "2026-06-12")
        gld_monthly = uop.classify_contract("O:GLD260619C00250000", "GLD", "2026-06-19")
        googl_monthly = uop.classify_contract("O:GOOGL260619C00200000", "GOOGL", "2026-06-19")
        tsm_monthly = uop.classify_contract("O:TSM260619C00200000", "TSM", "2026-06-19")

        self.assertEqual(tsla_monthly[:2], ("monthly", "physical"))
        self.assertEqual(tsla_weekly[:2], ("short_cycle", "physical"))
        self.assertEqual(gld_monthly[:2], ("monthly", "physical"))
        self.assertEqual(googl_monthly[:2], ("monthly", "physical"))
        self.assertEqual(tsm_monthly[:2], ("monthly", "physical"))

    def test_storage_filter_keeps_monthly_full_chain_but_short_cycle_only_band(self):
        monthly = uop.OptionContract(
            option_ticker="O:SPY260619C00650000",
            underlying="SPY",
            call_put="C",
            strike=650.0,
            expiration_date="2026-06-19",
            contract_root="SPY",
            expiration_type="monthly",
            settlement_type="physical",
        )
        weekly_near = uop.OptionContract(
            option_ticker="O:SPY260612C00605000",
            underlying="SPY",
            call_put="C",
            strike=605.0,
            expiration_date="2026-06-12",
            contract_root="SPY",
            expiration_type="short_cycle",
            settlement_type="physical",
        )
        weekly_far = uop.OptionContract(
            option_ticker="O:SPY260612C00700000",
            underlying="SPY",
            call_put="C",
            strike=700.0,
            expiration_date="2026-06-12",
            contract_root="SPY",
            expiration_type="short_cycle",
            settlement_type="physical",
        )

        self.assertTrue(uop.should_keep_contract_for_storage(monthly, "20260610", 600.0, 5.0))
        self.assertTrue(uop.should_keep_contract_for_storage(weekly_near, "20260610", 600.0, 5.0))
        self.assertFalse(uop.should_keep_contract_for_storage(weekly_far, "20260610", 600.0, 5.0))

    def test_storage_filter_keeps_equity_monthly_full_chain_after_classification(self):
        exp_type, settlement, root = uop.classify_contract("O:TSLA260619C00600000", "TSLA", "2026-06-19")
        monthly = uop.OptionContract(
            option_ticker="O:TSLA260619C00600000",
            underlying="TSLA",
            call_put="C",
            strike=600.0,
            expiration_date="2026-06-19",
            contract_root=root,
            expiration_type=exp_type,
            settlement_type=settlement,
        )

        self.assertEqual(monthly.expiration_type, "monthly")
        self.assertTrue(uop.should_keep_contract_for_storage(monthly, "20260610", 400.0, 5.0))

    def test_monthly_contract_becomes_short_cycle_for_storage_on_0dte(self):
        contract = uop.OptionContract(
            option_ticker="O:SPY260619C00650000",
            underlying="SPY",
            call_put="C",
            strike=650.0,
            expiration_date="2026-06-19",
            contract_root="SPY",
            expiration_type="monthly",
            settlement_type="physical",
        )

        self.assertFalse(uop.should_keep_contract_for_storage(contract, "20260619", 600.0, 5.0))

    def test_snapshot_normalization_does_not_keep_greeks(self):
        raw = {
            "details": {
                "ticker": "O:SPY260619C00600000",
                "underlying_ticker": "SPY",
                "contract_type": "call",
                "expiration_date": "2026-06-19",
                "strike_price": 600,
                "exercise_style": "american",
                "shares_per_contract": 100,
            },
            "day": {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10, "vwap": 1.4},
            "open_interest": 123,
            "implied_volatility": 0.22,
            "greeks": {"delta": 0.5},
        }
        contract = uop.contract_from_snapshot(raw, "SPY")
        daily = uop.daily_from_snapshot(raw, contract, "20260608")
        iv = uop.iv_from_snapshot(raw, contract, "20260608", 600.0)

        self.assertEqual(contract.call_put, "C")
        self.assertEqual(daily.close, 1.5)
        self.assertEqual(iv.provider_iv, 0.22)
        self.assertNotIn("delta", iv.__dict__)

    def test_snapshot_rows_uses_underlying_price_hint_for_short_cycle_filter(self):
        class FakeClient:
            def option_chain_snapshot(self, underlying):
                return [
                    {
                        "details": {
                            "ticker": "O:SPY260612C00605000",
                            "underlying_ticker": underlying,
                            "contract_type": "call",
                            "expiration_date": "2026-06-12",
                            "strike_price": 605,
                        },
                        "day": {"close": 1.5, "volume": 10},
                        "open_interest": 12,
                        "implied_volatility": 20,
                        "underlying_asset": {"ticker": underlying},
                    },
                    {
                        "details": {
                            "ticker": "O:SPY260612C00700000",
                            "underlying_ticker": underlying,
                            "contract_type": "call",
                            "expiration_date": "2026-06-12",
                            "strike_price": 700,
                        },
                        "day": {"close": 0.1, "volume": 2},
                        "open_interest": 1,
                        "implied_volatility": 80,
                        "underlying_asset": {"ticker": underlying},
                    },
                ]

        contracts, daily_rows, iv_rows, price = uop.snapshot_rows_for_underlying(
            FakeClient(),
            "SPY",
            "20260610",
            short_strike_band_pct=5,
            underlying_price_hint=600.0,
        )

        self.assertEqual(price, 600.0)
        self.assertEqual([contract.option_ticker for contract in contracts], ["O:SPY260612C00605000"])
        self.assertEqual(len(daily_rows), 1)
        self.assertEqual(iv_rows[0].provider_iv, 0.20)
        self.assertEqual(iv_rows[0].underlying_price, 600.0)

    def test_underlying_close_reads_stock_prices_latest_before_trade_date(self):
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE stock_prices (symbol TEXT, date TEXT, close REAL)"))
            conn.execute(
                text(
                    """
                    INSERT INTO stock_prices (symbol, date, close) VALUES
                    ('SPY', '2026-06-24', 606.5),
                    ('SPY', '2026-06-20', 600.0)
                    """
                )
            )

        self.assertEqual(uop.fetch_underlying_close_from_db(engine, "SPY", "20260625"), 606.5)

    def test_records_from_dataclasses_keep_none_values_for_bulk_upsert(self):
        row = uop.OptionDaily(
            trade_date="20260625",
            option_ticker="O:SPY260625C00700000",
            underlying="SPY",
            open_interest=12,
        )

        records = uop._records_from_dataclasses([row])

        self.assertIn("close", records[0])
        self.assertIsNone(records[0]["close"])
        self.assertIn("transactions", records[0])

    def test_provider_paginates_and_retries_429(self):
        client = uop.MassiveOptionsClient(api_key="k", sleep_seconds=0, max_retries=1)
        calls = []

        def fake_get(url, params=None, timeout=None):
            calls.append((url, params))
            if len(calls) == 1:
                return FakeResponse(429, text="rate limited")
            if len(calls) == 2:
                return FakeResponse(200, {"results": [{"a": 1}], "next_url": "https://next"})
            return FakeResponse(200, {"results": [{"a": 2}]})

        client.session = types.SimpleNamespace(get=fake_get)

        rows = list(client.iter_paginated("/v3/reference/options/contracts", {"limit": 1}))

        self.assertEqual(rows, [{"a": 1}, {"a": 2}])
        self.assertEqual(len(calls), 3)

    def test_backfill_rest_fallback_counts_daily_rows(self):
        contract = uop.OptionContract(
            option_ticker="O:SPY260619C00600000",
            underlying="SPY",
            call_put="C",
            strike=600.0,
            expiration_date="2026-06-19",
            contract_root="SPY",
            expiration_type="monthly",
            settlement_type="physical",
        )

        class FakeClient:
            def list_contracts(self, underlying, as_of=None, expired=True):
                return [contract]

            def aggregate_daily(self, option_ticker, start, end):
                return [{"o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 100, "vw": 1.4, "n": 7}]

        result = uop.backfill_daily_from_rest(
            None,
            FakeClient(),
            "20260605",
            underlyings=["SPY"],
            dry_run=True,
        )

        self.assertEqual(result["contracts"], 1)
        self.assertEqual(result["daily"], 1)
        self.assertEqual(result["source"], "rest_aggregates")

    def test_basic_probe_uses_one_contract_and_one_aggregate(self):
        class FakeClient:
            def underlying_daily_close(self, underlying, trade_date):
                return 600.0

            def contracts_page(
                self,
                underlying,
                as_of=None,
                expiration_gte=None,
                expiration_lte=None,
                strike_gte=None,
                strike_lte=None,
                limit=25,
            ):
                self.last_strike_range = (strike_gte, strike_lte)
                return [
                    {
                        "ticker": "O:SPY260717C00500000",
                        "underlying_ticker": "SPY",
                        "contract_type": "call",
                        "expiration_date": "2026-07-17",
                        "strike_price": 500,
                    },
                    {
                        "ticker": "O:SPY260717C00600000",
                        "underlying_ticker": "SPY",
                        "contract_type": "call",
                        "expiration_date": "2026-07-17",
                        "strike_price": 600,
                    }
                ]

            def aggregate_daily(self, option_ticker, start, end):
                return [{"o": 2.0, "h": 2.5, "l": 1.5, "c": 2.2, "v": 20, "vw": 2.1, "n": 3}]

        result = uop.basic_probe(
            None,
            FakeClient(),
            underlying="SPY",
            trade_date="20260623",
            dry_run=True,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["daily_rows"], 1)
        self.assertEqual(result["underlying_price"], 600.0)
        self.assertEqual(result["contract"]["option_ticker"], "O:SPY260717C00600000")

    def test_basic_probe_cli_requires_test_tables_for_writes(self):
        args = uop.build_arg_parser().parse_args(["--mode", "basic-probe", "--underlyings", "SPY"])

        with self.assertRaisesRegex(RuntimeError, "only writes with --use-test-tables"):
            uop.run_cli(args)

    def test_metrics_backfill_cli_does_not_instantiate_massive_client(self):
        args = uop.build_arg_parser().parse_args(
            [
                "--mode",
                "metrics-backfill",
                "--start",
                "20260622",
                "--end",
                "20260626",
                "--underlyings",
                "SPY,QQQ",
                "--dry-run",
            ]
        )
        with patch("us_options_polygon.get_db_engine", return_value=object()), \
             patch("us_options_polygon.metrics_backfill_range", return_value={"metrics": 2}) as backfill, \
             patch("us_options_polygon.MassiveOptionsClient", side_effect=AssertionError("API client should not be used")):
            result = uop.run_cli(args)

        self.assertEqual(result["metrics"], 2)
        backfill.assert_called_once()

    def test_flatfile_credentials_require_boto3(self):
        with patch.dict("os.environ", {"MASSIVE_FLATFILES_ACCESS_KEY": "a", "MASSIVE_FLATFILES_SECRET_KEY": "s"}):
            with patch("us_options_polygon.importlib.util.find_spec", return_value=None):
                self.assertFalse(uop.flatfile_credentials_available())

    def test_backfill_flatfile_only_skips_missing_flatfile_without_rest(self):
        with patch("us_options_polygon.flatfile_credentials_available", return_value=True), \
             patch("us_options_polygon.backfill_daily_from_flatfile", side_effect=uop.MassiveFlatFileMissingError("missing")), \
             patch("us_options_polygon.backfill_daily_from_rest") as rest:
            result = uop.backfill_range(
                None,
                object(),
                "20260703",
                "20260703",
                underlyings=["SPY"],
                dry_run=True,
                flatfile_only=True,
            )

        self.assertEqual(result["skipped_days"], 1)
        self.assertEqual(result["flatfile_days"], 0)
        self.assertEqual(result["rest_days"], 0)
        rest.assert_not_called()

    def test_backfill_flatfile_only_requires_flatfile_credentials(self):
        with patch("us_options_polygon.flatfile_credentials_available", return_value=False):
            with self.assertRaisesRegex(uop.MassiveAPIError, "Flat-file credentials"):
                uop.backfill_range(
                    None,
                    object(),
                    "20260626",
                    "20260626",
                    underlyings=["SPY"],
                    dry_run=True,
                    flatfile_only=True,
                )

    def test_iv_rank_uses_only_monthly_rows(self):
        if uop.pd is None:
            self.skipTest("pandas is not available in this test environment")

        def fake_read_sql(_sql, _engine, params=None):
            return uop.pd.DataFrame(
                [
                    {
                        "trade_date": "20260102",
                        "option_ticker": "O:SPY260220C00600000",
                        "underlying": "SPY",
                        "provider_iv": 0.20,
                        "computed_iv": None,
                        "open_interest": 100,
                        "underlying_price": 600.0,
                        "strike": 600.0,
                        "call_put": "C",
                        "expiration_date": "2026-02-20",
                        "expiration_type": "monthly",
                    },
                    {
                        "trade_date": "20260103",
                        "option_ticker": "O:SPY260220P00600000",
                        "underlying": "SPY",
                        "provider_iv": 0.30,
                        "computed_iv": None,
                        "open_interest": 100,
                        "underlying_price": 600.0,
                        "strike": 600.0,
                        "call_put": "P",
                        "expiration_date": "2026-02-20",
                        "expiration_type": "monthly",
                    },
                ]
            )

        with patch("us_options_polygon.pd.read_sql", fake_read_sql):
            result = uop.get_us_underlying_iv_rank("SPY", window=252, engine=object())

        self.assertIsNotNone(result)
        self.assertEqual(result["current_iv"], 30.0)
        self.assertEqual(result["iv_rank"], 100.0)

    def test_default_trade_date_before_us_close_uses_previous_weekday(self):
        now = dt.datetime(2026, 6, 8, 12, 0, tzinfo=dt.timezone.utc)

        self.assertEqual(uop.default_trade_date(now), "20260605")


if __name__ == "__main__":
    unittest.main()
