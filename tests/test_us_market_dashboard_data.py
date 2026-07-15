import json
import inspect
import math
import unittest
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text

import us_market_dashboard_data as dash


class UsMarketDashboardDataTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)

    def _create_stock_prices(self):
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
                    VALUES
                    ('2026-01-02', 'SPY', 600, 606, 598, 604, 1000, 604),
                    ('2026-01-03', 'SPY', 604, 608, 602, 606, 1100, 606),
                    ('2026-01-03', 'QQQ', 520, 522, 518, 521, 900, 521)
                    """
                )
            )

    def _create_option_tables(self, use_test_tables=True):
        names = dash.option_table_names(use_test_tables)
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
                        underlying_price REAL
                    )
                    """
                )
            )
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
                        monthly_contract_count INTEGER,
                        short_cycle_contract_count INTEGER,
                        provider_iv_rows INTEGER,
                        computed_iv_rows INTEGER,
                        open_interest_rows INTEGER,
                        source TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['contracts']}
                    (option_ticker, underlying, call_put, strike, expiration_date, contract_root,
                     expiration_type, settlement_type, exercise_style, shares_per_contract, source, updated_at)
                    VALUES
                    ('O:SPY260220C00600000', 'SPY', 'C', 600, '2026-02-20', 'SPY',
                     'monthly', 'physical', '', 100, 'massive', ''),
                    ('O:SPY260220P00600000', 'SPY', 'P', 600, '2026-02-20', 'SPY',
                     'monthly', 'physical', '', 100, 'massive', ''),
                    ('O:SPY260116C00630000', 'SPY', 'C', 630, '2026-01-16', 'SPY',
                     'short_cycle', 'physical', '', 100, 'massive', '')
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
                    ('20260115', 'O:SPY260220C00600000', 'SPY', 20, 22, 18, 21, 100,
                     20.5, 30, 500, 'massive', ''),
                    ('20260115', 'O:SPY260220P00600000', 'SPY', 19, 21, 17, 18, 90,
                     18.5, 25, 450, 'massive', ''),
                    ('20260115', 'O:SPY260116C00630000', 'SPY', 1, 1.2, .8, 1.1, 80,
                     1.0, 20, NULL, 'massive', '')
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price)
                    VALUES
                    ('20260115', 'O:SPY260220C00600000', 'SPY', .20, NULL,
                     'provider_snapshot', 500, 600),
                    ('20260115', 'O:SPY260220P00600000', 'SPY', NULL, .22,
                     'computed', 450, 600),
                    ('20260115', 'O:SPY260116C00630000', 'SPY', .80, NULL,
                     'provider_snapshot', NULL, 600)
                    """
                )
            )

    def _insert_option_anomaly_rows(self):
        self._create_option_tables(use_test_tables=True)
        names = dash.option_table_names(True)
        contracts = [
            {
                "option_ticker": "O:SPY260130C00630000",
                "underlying": "SPY",
                "call_put": "C",
                "strike": 630,
                "expiration_date": "2026-01-30",
            },
            {
                "option_ticker": "O:SPY260130P00570000",
                "underlying": "SPY",
                "call_put": "P",
                "strike": 570,
                "expiration_date": "2026-01-30",
            },
            {
                "option_ticker": "O:SPY260130C00650000",
                "underlying": "SPY",
                "call_put": "C",
                "strike": 650,
                "expiration_date": "2026-01-30",
            },
            {
                "option_ticker": "O:SPY260130P00550000",
                "underlying": "SPY",
                "call_put": "P",
                "strike": 550,
                "expiration_date": "2026-01-30",
            },
            {
                "option_ticker": "O:SPY260130C00610000",
                "underlying": "SPY",
                "call_put": "C",
                "strike": 610,
                "expiration_date": "2026-01-30",
            },
            {
                "option_ticker": "O:SPY260130P00530000",
                "underlying": "SPY",
                "call_put": "P",
                "strike": 530,
                "expiration_date": "2026-01-30",
            },
        ]
        history_dates = ["20260108", "20260109", "20260112", "20260113", "20260114"]
        history_oi = {
            "O:SPY260130C00630000": [90, 100, 110, 115, 120],
            "O:SPY260130P00570000": [400, 420, 440, 460, 500],
            "O:SPY260130C00650000": [0, 0, 0, 0, 0],
            "O:SPY260130C00610000": [110, 120, 130, 140, 150],
        }
        current_rows = [
            {
                "trade_date": "20260115",
                "option_ticker": "O:SPY260130C00630000",
                "underlying": "SPY",
                "open": 3.8,
                "high": 4.8,
                "low": 3.5,
                "close": 4.2,
                "volume": 700,
                "vwap": 4.5,
                "transactions": 100,
                "open_interest": 450,
                "source": "test",
                "updated_at": "",
            },
            {
                "trade_date": "20260115",
                "option_ticker": "O:SPY260130P00570000",
                "underlying": "SPY",
                "open": 2.8,
                "high": 3.4,
                "low": 2.5,
                "close": 3.1,
                "volume": 1500,
                "vwap": 3.0,
                "transactions": 80,
                "open_interest": 100,
                "source": "test",
                "updated_at": "",
            },
            {
                "trade_date": "20260115",
                "option_ticker": "O:SPY260130C00650000",
                "underlying": "SPY",
                "open": 4.0,
                "high": 5.2,
                "low": 3.9,
                "close": 5.0,
                "volume": 200,
                "vwap": None,
                "transactions": 20,
                "open_interest": 0,
                "source": "test",
                "updated_at": "",
            },
            {
                "trade_date": "20260115",
                "option_ticker": "O:SPY260130P00550000",
                "underlying": "SPY",
                "open": 1.8,
                "high": 2.4,
                "low": 1.6,
                "close": 2.1,
                "volume": 120,
                "vwap": 2.0,
                "transactions": 12,
                "open_interest": 80,
                "source": "test",
                "updated_at": "",
            },
            {
                "trade_date": "20260115",
                "option_ticker": "O:SPY260130C00610000",
                "underlying": "SPY",
                "open": 1.8,
                "high": 2.2,
                "low": 1.7,
                "close": 2.0,
                "volume": 20,
                "vwap": 2.0,
                "transactions": 5,
                "open_interest": 160,
                "source": "test",
                "updated_at": "",
            },
            {
                "trade_date": "20260115",
                "option_ticker": "O:SPY260130P00530000",
                "underlying": "SPY",
                "open": 1.4,
                "high": 1.7,
                "low": 1.3,
                "close": 1.5,
                "volume": 20,
                "vwap": 1.5,
                "transactions": 4,
                "open_interest": 180,
                "source": "test",
                "updated_at": "",
            },
        ]
        history_rows = []
        for ticker, oi_values in history_oi.items():
            for trade_date, open_interest in zip(history_dates, oi_values):
                history_rows.append(
                    {
                        "trade_date": trade_date,
                        "option_ticker": ticker,
                        "underlying": "SPY",
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                        "volume": 10,
                        "vwap": 1,
                        "transactions": 1,
                        "open_interest": open_interest,
                        "source": "test",
                        "updated_at": "",
                    }
                )
        history_rows.append(
            {
                "trade_date": "20260114",
                "option_ticker": "O:SPY260130P00550000",
                "underlying": "SPY",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 10,
                "vwap": 1,
                "transactions": 1,
                "open_interest": 10,
                "source": "test",
                "updated_at": "",
            }
        )
        history_rows.append(
            {
                "trade_date": "20260114",
                "option_ticker": "O:SPY260130P00530000",
                "underlying": "SPY",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 10,
                "vwap": 1,
                "transactions": 1,
                "open_interest": 0,
                "source": "test",
                "updated_at": "",
            }
        )
        iv_rows = []
        for row in [*history_rows, *current_rows]:
            if row["trade_date"] not in {"20260114", "20260115"}:
                continue
            iv_rows.append(
                {
                    "trade_date": row["trade_date"],
                    "option_ticker": row["option_ticker"],
                    "underlying": "SPY",
                    "provider_iv": 0.20 if row["trade_date"] == "20260114" else 0.25,
                    "computed_iv": None,
                    "iv_source": "provider_snapshot",
                    "open_interest": row["open_interest"],
                    "underlying_price": 600,
                }
            )

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['contracts']}
                    (option_ticker, underlying, call_put, strike, expiration_date, contract_root,
                     expiration_type, settlement_type, exercise_style, shares_per_contract, source, updated_at)
                    VALUES (:option_ticker, :underlying, :call_put, :strike, :expiration_date, 'SPY',
                            'monthly', 'physical', '', 100, 'test', '')
                    """
                ),
                contracts,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['daily']}
                    (trade_date, option_ticker, underlying, open, high, low, close, volume,
                     vwap, transactions, open_interest, source, updated_at)
                    VALUES (:trade_date, :option_ticker, :underlying, :open, :high, :low, :close, :volume,
                            :vwap, :transactions, :open_interest, :source, :updated_at)
                    """
                ),
                [*history_rows, *current_rows],
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price)
                    VALUES (:trade_date, :option_ticker, :underlying, :provider_iv, :computed_iv,
                            :iv_source, :open_interest, :underlying_price)
                    """
                ),
                iv_rows,
            )

    def _create_market_climate_tables(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE macro_daily (
                        trade_date TEXT,
                        indicator_code TEXT,
                        indicator_name TEXT,
                        category TEXT,
                        close_value REAL,
                        change_value REAL,
                        change_pct REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE market_climate_daily (
                        indicator_code TEXT,
                        as_of_date TEXT,
                        value REAL,
                        secondary_value REAL,
                        unit TEXT,
                        source TEXT,
                        payload_json TEXT,
                        updated_at TEXT
                    )
                    """
                )
            )
            macro_rows = [
                ("2026-05-30", "BAMLH0A0HYM2", "HY OAS", "credit", 3.20, 0, 0),
                ("2026-06-24", "DFII10", "10Y TIPS", "bond", 2.10, 0, 0),
                ("2026-06-29", "DGS10", "10Y", "bond", 4.38, 0.02, 0.46),
                ("2026-06-29", "DFII10", "10Y TIPS", "bond", 2.16, 0.02, 0.94),
                ("2026-06-29", "BAMLH0A0HYM2", "HY OAS", "credit", 2.80, -0.04, -1.4),
                ("2026-06-29", "SOFR", "SOFR", "policy", 3.62, -0.01, -0.28),
                ("2026-05-01", "FEDFUNDS", "Fed Funds", "policy", 3.63, 0, 0),
                ("2026-06-30", "T10Y3M", "10Y-3M", "bond", 0.57, 0.02, 3.6),
            ]
            conn.execute(
                text(
                    """
                    INSERT INTO macro_daily
                    (trade_date, indicator_code, indicator_name, category, close_value, change_value, change_pct)
                    VALUES (:trade_date, :indicator_code, :indicator_name, :category, :close_value, :change_value, :change_pct)
                    """
                ),
                [
                    {
                        "trade_date": trade_date,
                        "indicator_code": indicator_code,
                        "indicator_name": indicator_name,
                        "category": category,
                        "close_value": close_value,
                        "change_value": change_value,
                        "change_pct": change_pct,
                    }
                    for trade_date, indicator_code, indicator_name, category, close_value, change_value, change_pct in macro_rows
                ],
            )
            climate_rows = [
                (
                    "VIX_TERM",
                    "2026-06-30",
                    -1.4,
                    16.2,
                    "vol_points",
                    "cboe",
                    {"vix9d": 14.8, "vix": 16.2, "vix3m": 16.2},
                ),
                (
                    "FEDWATCH",
                    "2026-06-30",
                    62.4,
                    None,
                    "%",
                    "cme",
                    {"action_label": "维持", "meeting_date": "2026-07-29"},
                ),
                (
                    "AAII_BULL_BEAR",
                    "2026-06-25",
                    -0.7,
                    None,
                    "pp",
                    "aaii",
                    {"bullish_pct": 36.3, "bearish_pct": 37.0},
                ),
                (
                    "CFTC_VIX_LEV_NET",
                    "2026-06-24",
                    -8.2,
                    -12345,
                    "%_oi",
                    "cftc",
                    {"open_interest": 150000},
                ),
                (
                    "GSCPI",
                    "2026-05-01",
                    -0.12,
                    0.18,
                    "index",
                    "ny_fed",
                    {},
                ),
            ]
            conn.execute(
                text(
                    """
                    INSERT INTO market_climate_daily
                    (indicator_code, as_of_date, value, secondary_value, unit, source, payload_json, updated_at)
                    VALUES (:indicator_code, :as_of_date, :value, :secondary_value, :unit, :source, :payload_json, '')
                    """
                ),
                [
                    {
                        "indicator_code": indicator_code,
                        "as_of_date": as_of_date,
                        "value": value,
                        "secondary_value": secondary_value,
                        "unit": unit,
                        "source": source,
                        "payload_json": json.dumps(payload),
                    }
                    for indicator_code, as_of_date, value, secondary_value, unit, source, payload in climate_rows
                ],
            )

    def test_load_stock_daily_returns_sorted_symbol_rows(self):
        self._create_stock_prices()

        df = dash.load_stock_daily("spy", limit=10, engine=self.engine)

        self.assertEqual(list(df["symbol"].unique()), ["SPY"])
        self.assertEqual(df["date"].dt.strftime("%Y%m%d").tolist(), ["20260102", "20260103"])
        self.assertEqual(float(df["close"].iloc[-1]), 606.0)

    def test_load_stock_daily_missing_table_is_empty(self):
        df = dash.load_stock_daily("SPY", engine=self.engine)

        self.assertTrue(df.empty)
        self.assertIn("close", df.columns)

    def test_option_table_names_switch_to_test_tables(self):
        names = dash.option_table_names(True)

        self.assertEqual(names["daily"], "us_option_daily_test")
        self.assertEqual(names["metrics"], "us_option_market_metrics_daily_test")
        self.assertEqual(dash.option_table_names(False)["daily"], "us_option_daily")
        self.assertEqual(dash.option_table_names(False)["metrics"], "us_option_market_metrics_daily")

    def test_dashboard_underlyings_include_labels_and_exclude_index_symbols(self):
        symbols = set(dash.DEFAULT_DASHBOARD_UNDERLYINGS)
        new_symbols = {
            "ADBE",
            "APP",
            "ARM",
            "ASML",
            "AVGO",
            "BA",
            "BABA",
            "BAC",
            "C",
            "CAT",
            "COIN",
            "CRM",
            "CRWD",
            "CVNA",
            "DELL",
            "DIS",
            "DRAM",
            "EEM",
            "F",
            "FXI",
            "GME",
            "GOOGL",
            "HOOD",
            "IBM",
            "INTC",
            "JPM",
            "KRE",
            "LLY",
            "MARA",
            "META",
            "MRVL",
            "MSFT",
            "MSTR",
            "MU",
            "NFLX",
            "NKE",
            "ORCL",
            "PANW",
            "PDD",
            "PFE",
            "PLTR",
            "PYPL",
            "QCOM",
            "RIVN",
            "RKLB",
            "SHOP",
            "SMCI",
            "SMH",
            "SNOW",
            "SOFI",
            "SPCX",
            "TSM",
            "UBER",
            "UNH",
            "USO",
            "VRT",
            "WFC",
            "WMT",
            "XBI",
            "XLI",
            "XLK",
            "XLV",
            "XLY",
        }

        self.assertFalse({"SPX", "NDX", "RUT", "VIX"} & symbols)
        for symbol in (
            "SPY",
            "QQQ",
            "IWM",
            "GLD",
            "TLT",
            "TSLA",
            "NVDA",
            "AMD",
            "AAPL",
            "AMZN",
            *sorted(new_symbols),
        ):
            self.assertIn(symbol, symbols)
            self.assertTrue(dash.UNDERLYING_DISPLAY_NAMES.get(symbol))
        self.assertTrue(new_symbols <= symbols)
        self.assertEqual(len(dash.DEFAULT_DASHBOARD_UNDERLYINGS), 78)

    def test_dashboard_underlyings_prioritize_core_etfs_then_sort_symbols(self):
        symbols = list(dash.DEFAULT_DASHBOARD_UNDERLYINGS)

        self.assertEqual(symbols[:4], ["SPY", "QQQ", "DIA", "IWM"])
        self.assertEqual(symbols[4:], sorted(symbols[4:]))

    def test_underlying_profiles_cover_default_dashboard_symbols(self):
        for symbol in dash.DEFAULT_DASHBOARD_UNDERLYINGS:
            profile = dash.get_underlying_profile(symbol)

            self.assertEqual(profile["symbol"], symbol)
            self.assertTrue(profile["name"])
            self.assertIn(profile["asset_type"], {"stock", "etf"})
            self.assertTrue(profile["business"])
            self.assertTrue(profile["strength"])
            self.assertTrue(profile["risk"])
            self.assertTrue(profile["next_earnings_date"])

        self.assertEqual(dash.get_underlying_profile("SPY")["next_earnings_date"], dash.ETF_EARNINGS_NOTE)
        self.assertEqual(dash.get_underlying_profile("AAPL")["next_earnings_date"], dash.STOCK_EARNINGS_NOTE)

    def test_underlying_a_share_benchmarks_filter_by_asset_type(self):
        nvda = dash.get_underlying_a_share_benchmarks("NVDA")
        spy = dash.get_underlying_a_share_benchmarks("SPY")
        raw_items = [
            item
            for entries in dash.UNDERLYING_A_SHARE_BENCHMARKS.values()
            for item in entries
        ]

        self.assertTrue(nvda)
        self.assertTrue(all(item["type"] == "stock" for item in nvda))
        self.assertTrue(any(item["name"] == "寒武纪" for item in nvda))
        self.assertTrue(spy)
        self.assertTrue(all(item["type"] in {"etf", "index"} for item in spy))
        self.assertTrue(all(item.get("relation") for item in raw_items))
        self.assertTrue(all(item.get("note") for item in raw_items))
        self.assertEqual(dash.get_underlying_a_share_benchmarks("GME"), [])

    def test_a_share_benchmarks_follow_domestic_substitution_mapping_rules(self):
        asml = dash.get_underlying_a_share_benchmarks("ASML")
        mrvl = dash.get_underlying_a_share_benchmarks("MRVL")
        nvda = dash.get_underlying_a_share_benchmarks("NVDA")
        qcom = dash.get_underlying_a_share_benchmarks("QCOM")
        orcl = dash.get_underlying_a_share_benchmarks("ORCL")
        lly = dash.get_underlying_a_share_benchmarks("LLY")
        panw = dash.get_underlying_a_share_benchmarks("PANW")
        spcx = dash.get_underlying_a_share_benchmarks("SPCX")
        asml_names = {item["name"] for item in asml}
        mrvl_names = {item["name"] for item in mrvl}
        nvda_names = {item["name"] for item in nvda}
        qcom_names = {item["name"] for item in qcom}
        orcl_names = {item["name"] for item in orcl}
        lly_names = {item["name"] for item in lly}
        panw_names = {item["name"] for item in panw}
        spcx_names = {item["name"] for item in spcx}
        broadcom_names = {item["name"] for item in dash.get_underlying_a_share_benchmarks("AVGO")}

        self.assertEqual(asml_names, {"芯源微", "芯碁微装"})
        self.assertTrue(all(item["relation"] == "核心映射" for item in asml))
        self.assertTrue(any("不是光刻机整机" in item["note"] for item in asml))
        self.assertEqual(mrvl_names, {"盛科通信", "源杰科技", "中际旭创", "新易盛", "天孚通信"})
        self.assertIn("国产替代", {item["relation"] for item in mrvl})
        self.assertIn("核心映射", {item["relation"] for item in mrvl})
        self.assertTrue({"摩尔线程", "沐曦股份-U"} <= nvda_names)
        self.assertTrue(all(item["relation"] == "国产替代" for item in nvda))
        self.assertIn("翱捷科技-U", qcom_names)
        self.assertIn("达梦数据", orcl_names)
        self.assertTrue({"翰宇药业", "诺泰生物"} <= lly_names)
        self.assertIn("安恒信息", panw_names)
        self.assertIn("航天环宇", spcx_names)
        self.assertIn("芯原股份", broadcom_names)
        self.assertIn("盛科通信", broadcom_names)
        self.assertIn("裕太微-U", broadcom_names)
        self.assertEqual(dash.get_underlying_a_share_benchmarks("NKE"), [])
        self.assertEqual(dash.get_underlying_a_share_benchmarks("SOFI"), [])
        self.assertEqual(dash.get_underlying_a_share_benchmarks("UNH"), [])

    def test_build_underlying_profile_card_includes_a_share_benchmarks(self):
        nvda = dash.build_underlying_profile_card("NVDA", use_test_tables=True, engine=self.engine)
        gme = dash.build_underlying_profile_card("GME", use_test_tables=True, engine=self.engine)

        self.assertIn("a_share_benchmarks", nvda)
        self.assertTrue(any(item["name"] == "寒武纪" for item in nvda["a_share_benchmarks"]))
        self.assertEqual(gme["a_share_benchmarks"], [])

    def test_spcx_profile_is_spacex_stock_not_etf(self):
        profile = dash.get_underlying_profile("SPCX")

        self.assertEqual(profile["name"], "SpaceX")
        self.assertEqual(profile["asset_type"], "stock")
        self.assertIn("商业航天", profile["business"])
        self.assertEqual(profile["next_earnings_date"], dash.STOCK_EARNINGS_NOTE)

    def test_format_profile_updated_at_uses_beijing_time(self):
        self.assertEqual(dash.format_profile_updated_at_beijing("2026-07-07 15:05:00"), "07/07 23:05")
        self.assertEqual(dash.format_profile_updated_at_beijing("2026-07-07T15:05:00Z"), "07/07 23:05")
        self.assertEqual(dash.format_profile_updated_at_beijing("", "20260707"), "2026/07/07")

    def test_estimate_next_earnings_window_uses_next_quarter_window(self):
        self.assertEqual(
            dash.estimate_next_earnings_window(pd.Timestamp("2026-07-07").date()),
            "估算 2026/07/15-08/15",
        )
        self.assertEqual(
            dash.estimate_next_earnings_window(pd.Timestamp("2026-08-20").date()),
            "估算 2026/10/15-11/15",
        )

    def test_nasdaq_earnings_payload_formats_calendar_row(self):
        payload = dash._nasdaq_earnings_payload(
            {"time": "time-after-hours", "fiscalQuarterEnding": "Jun/2026", "epsForecast": "$1.35"},
            pd.Timestamp("2026-08-04").date(),
        )

        self.assertEqual(payload["date"], "2026/08/04")
        self.assertEqual(payload["source"], "Nasdaq")
        self.assertIn("盘后", payload["detail"])
        self.assertIn("EPS预期 $1.35", payload["detail"])
        self.assertEqual(payload["is_estimate"], "0")

    def test_profile_source_ref_classifies_analyst_catalyst_and_risk(self):
        catalyst = dash._classify_profile_source_ref(
            {"source": "Web Search", "title": "Analyst upgrades Disney and raises price target on streaming profit"}
        )
        risk = dash._classify_profile_source_ref(
            {"source": "Web Search", "title": "Analyst cuts target as regulatory probe pressures shares"}
        )

        self.assertEqual(catalyst["kind"], "analyst")
        self.assertEqual(catalyst["side"], "catalyst")
        self.assertEqual(risk["kind"], "analyst")
        self.assertEqual(risk["side"], "risk")

    def test_profile_dynamic_v2_fallback_uses_analyst_news_and_options_context(self):
        profile = dash.get_underlying_profile("DIS")
        refs = [
            dash._classify_profile_source_ref(
                {
                    "source": "Web Search",
                    "title": "Analyst upgrades Disney on streaming profit and theme park demand",
                    "summary": "Public report says analyst view improved after streaming margins recovered.",
                }
            )
        ]
        options_context = {
            "summary": "ATM IV 31.0%；Put/Call OI 1.35，保护需求偏高。",
            "refs": [{"source": "本地期权指标", "title": "ATM IV 31.0%"}],
        }

        out = dash._fallback_profile_dynamic_v2(
            profile=profile,
            earnings_date="2026/08/06",
            earnings_time="盘后",
            options_context=options_context,
            refs=refs,
            lookback_days=30,
        )

        self.assertIn("公开报道/分析师", out["recent_hotspot"])
        self.assertNotIn("Put/Call OI", out["recent_hotspot"])
        self.assertIn("Put/Call OI", out["option_data"])
        self.assertIn("偏空", out["option_data"])
        self.assertTrue(out["recent_risk"])
        self.assertEqual(out["confidence"], "medium")

    def test_summarize_option_market_bias_uses_requested_weights(self):
        result = dash.summarize_option_market_bias(
            {
                "put_call_oi_percentile": 90.0,
                "put_skew_5pct_percentile": 10.0,
                "call_skew_5pct_percentile": 90.0,
                "put_call_skew_5pct_percentile": 10.0,
                "term_slope_percentile": 90.0,
            }
        )

        self.assertEqual(result["direction"], "明显偏多")
        self.assertAlmostEqual(result["score"], 64.0)
        self.assertIn("Put保护溢价处于低位", result["basis"])
        self.assertIn("期限结构正常", result["basis"])
        weights = {item["field"]: item["weight"] for item in result["contributions"]}
        self.assertEqual(weights["put_call_oi"], 10.0)
        self.assertEqual(weights["put_skew_5pct"], 30.0)

    def test_summarize_option_market_bias_excludes_missing_oi_percentile(self):
        result = dash.summarize_option_market_bias(
            {
                "put_call_oi": 1.92,
                "put_call_oi_percentile": None,
                "put_skew_5pct_percentile": 15.0,
                "call_skew_5pct_percentile": 85.0,
                "put_call_skew_5pct_percentile": 20.0,
                "term_slope_percentile": 80.0,
            }
        )

        self.assertNotEqual(result["direction"], "参考性有限")
        self.assertEqual(result["eligible_indicators"], 4)
        self.assertEqual(result["available_weight"], 90.0)
        self.assertNotIn("Put侧持仓偏重", result["basis"])

    def test_summarize_option_market_bias_reports_insufficient_history(self):
        result = dash.summarize_option_market_bias(
            {
                "put_skew_5pct_percentile": 80.0,
                "call_skew_5pct_percentile": 20.0,
            }
        )

        self.assertEqual(result["direction"], "参考性有限")
        self.assertIsNone(result["score"])
        self.assertEqual(
            result["summary"],
            "期权判断：参考性有限；主要依据：历史样本不足，当前持仓与波动率信号尚未形成一致方向。",
        )

    def test_profile_options_context_keeps_concise_market_bias_summary(self):
        metrics = {
            "trade_date": "20260710",
            "put_call_oi_percentile": 10.0,
            "put_skew_5pct_percentile": 10.0,
            "call_skew_5pct_percentile": 90.0,
            "put_call_skew_5pct_percentile": 10.0,
            "term_slope_percentile": 90.0,
        }

        result = dash._profile_options_context(
            "SPY",
            metrics=metrics,
            as_of_date="20260710",
            engine=None,
        )

        self.assertTrue(result["summary"].startswith("期权判断："))
        self.assertIn("；主要依据：", result["summary"])
        self.assertNotIn("Put/Call OI", result["summary"])
        self.assertNotIn("ATM IV", result["summary"])

    def test_profile_dynamic_v2_uses_llm_json_when_valid(self):
        profile = dash.get_underlying_profile("AMD")
        with patch.object(
            dash,
            "_build_profile_llm_json",
            return_value={
                "recent_hotspot": "分析师关注AI GPU出货与财报指引。",
                "option_data": "期权数据看Call端追涨溢价和较高IV定价，信号偏多。",
                "confidence": "high",
            },
        ):
            out = dash._summarize_profile_dynamic_v2(
                profile=profile,
                earnings_date="2026/08/04",
                earnings_time="盘后",
                options_context={"summary": "ATM IV 78.0%；Call Skew +2.0。"},
                refs=[{"source": "Web Search", "title": "Analyst raises AMD target"}],
                lookback_days=30,
            )

        self.assertIn("AI GPU", out["recent_hotspot"])
        self.assertIn("Call", out["option_data"])
        self.assertIn("IV", out["recent_risk"])
        self.assertEqual(out["confidence"], "high")

    def test_underlying_profile_cache_table_creates_and_backfills_columns(self):
        table = dash.underlying_profile_cache_table(use_test_tables=True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {table} (
                        as_of_date TEXT NOT NULL,
                        underlying TEXT NOT NULL,
                        PRIMARY KEY (as_of_date, underlying)
                    )
                    """
                )
            )

        dash.ensure_underlying_profile_cache_table(self.engine, use_test_tables=True)

        columns = dash.table_columns(self.engine, table)
        for column in [*dash.UNDERLYING_PROFILE_CACHE_COLUMNS, "updated_at"]:
            self.assertIn(column, columns)

    def test_build_underlying_profile_card_prefers_cached_dynamic_payload(self):
        dash.replace_underlying_profile_cache(
            [
                {
                    "as_of_date": "20260707",
                    "underlying": "AAPL",
                    "earnings_date": "2026/08/04",
                    "earnings_time": "盘后",
                    "earnings_source": "Nasdaq",
                    "recent_catalyst": "测试催化",
                    "recent_risk": "期权数据测试：ATM IV 20.0%，期权信号中性。",
                    "dynamic_note": "测试动态",
                    "source_refs_json": json.dumps([{"source": "Nasdaq", "title": "calendar"}]),
                }
            ],
            as_of_date="20260707",
            underlyings=["AAPL"],
            use_test_tables=True,
            engine=self.engine,
        )

        with patch.object(dash, "fetch_nasdaq_next_earnings_dates", side_effect=AssertionError("no live fetch")):
            card = dash.build_underlying_profile_card("AAPL", use_test_tables=True, engine=self.engine)

        self.assertEqual(card["symbol"], "AAPL")
        self.assertEqual(card["earnings_date"], "2026/08/04")
        self.assertEqual(card["recent_hotspot"], "测试催化")
        self.assertEqual(card["option_data"], "期权数据测试：ATM IV 20.0%，期权信号中性。")
        self.assertEqual(card["dynamic_source_refs"][0]["source"], "Nasdaq")

    def test_build_underlying_profile_card_strips_legacy_static_text_from_dynamic_payload(self):
        profile = dash.get_underlying_profile("AAPL")
        dash.replace_underlying_profile_cache(
            [
                {
                    "as_of_date": "20260707",
                    "underlying": "AAPL",
                    "earnings_date": "2026/08/04",
                    "earnings_time": "盘后",
                    "earnings_source": "Nasdaq",
                    "recent_catalyst": f"近期看点围绕苹果财报窗口和期权定价。 核心业务：{profile['business']}",
                    "recent_risk": f"{profile['risk']} 财报前后留意IV事件后回落。",
                    "dynamic_note": "旧缓存",
                    "source_refs_json": json.dumps(
                        [
                            {"source": "本地期权指标", "title": "ATM IV 28.0%；Put/Call OI 0.78，看涨仓位更活跃。"},
                            {"source": "估算", "title": "legacy"},
                        ]
                    ),
                }
            ],
            as_of_date="20260707",
            underlyings=["AAPL"],
            use_test_tables=True,
            engine=self.engine,
        )

        card = dash.build_underlying_profile_card("AAPL", use_test_tables=True, engine=self.engine)

        self.assertNotIn("核心业务", card["recent_hotspot"])
        self.assertNotIn(profile["business"], card["recent_hotspot"])
        self.assertNotIn(profile["risk"], card["recent_risk"])
        self.assertIn("ATM IV", card["option_data"])
        self.assertEqual(card["option_data"], card["recent_risk"])

    def test_build_underlying_profile_card_ignores_stale_etf_cache_for_stock(self):
        dash.replace_underlying_profile_cache(
            [
                {
                    "as_of_date": "20260707",
                    "underlying": "SPCX",
                    "earnings_date": dash.ETF_EARNINGS_NOTE,
                    "earnings_time": "",
                    "earnings_source": "ETF",
                    "recent_catalyst": "近期关注SpaceX的成分板块轮动。",
                    "recent_risk": "ETF没有单一公司财报。",
                    "dynamic_note": "旧ETF缓存",
                    "source_refs_json": json.dumps([{"source": "ETF", "title": "legacy"}]),
                }
            ],
            as_of_date="20260707",
            underlyings=["SPCX"],
            use_test_tables=True,
            engine=self.engine,
        )

        card = dash.build_underlying_profile_card("SPCX", use_test_tables=True, engine=self.engine)

        self.assertEqual(card["asset_type"], "stock")
        self.assertNotEqual(card["earnings_date"], dash.ETF_EARNINGS_NOTE)
        self.assertNotEqual(card["earnings_source"], "ETF")
        self.assertNotIn("ETF没有单一公司财报", card["recent_risk"])

    def test_build_underlying_profile_card_has_readable_fallback_without_cache(self):
        card = dash.build_underlying_profile_card(
            "DIS",
            as_of_date="20260707",
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(card["symbol"], "DIS")
        self.assertIn("估算", card["earnings_date"])
        self.assertEqual(card["earnings_source"], "估算")
        self.assertIn("迪士尼", card["recent_hotspot"])
        self.assertIn("期权数据暂无最新样本", card["option_data"])
        self.assertEqual(card["dynamic_source_refs"][0]["source"], "估算")

    def test_rebuild_underlying_profile_cache_falls_back_without_network_or_llm(self):
        with patch.object(dash, "fetch_nasdaq_next_earnings_dates", return_value={}), patch.object(
            dash, "_fetch_recent_profile_news_refs", return_value=[]
        ), patch.object(
            dash, "_collect_profile_web_search_context", return_value=[]
        ), patch.dict("os.environ", {"DASHSCOPE_API_KEY": ""}, clear=False):
            result = dash.rebuild_underlying_profile_cache(
                underlyings=["AAPL"],
                as_of_date="20260707",
                lookback_days=30,
                apply=True,
                use_test_tables=True,
                engine=self.engine,
            )

        card = dash.build_underlying_profile_card("AAPL", use_test_tables=True, engine=self.engine)

        self.assertEqual(result["status"], "updated")
        self.assertEqual(result["written"], 1)
        self.assertIn("估算", card["earnings_date"])
        self.assertIn("V2", card["dynamic_note"])
        self.assertTrue(card["recent_hotspot"])
        self.assertTrue(card["option_data"])
        self.assertTrue(card["recent_risk"])

    def test_rebuild_underlying_profile_cache_marks_etf_without_company_earnings(self):
        with patch.object(dash, "fetch_nasdaq_next_earnings_dates", return_value={}), patch.object(
            dash, "_fetch_recent_profile_news_refs", return_value=[]
        ), patch.object(
            dash, "_collect_profile_web_search_context", return_value=[]
        ), patch.dict("os.environ", {"DASHSCOPE_API_KEY": ""}, clear=False):
            result = dash.rebuild_underlying_profile_cache(
                underlyings=["SPY"],
                as_of_date="20260707",
                apply=True,
                use_test_tables=True,
                engine=self.engine,
            )

        card = dash.build_underlying_profile_card("SPY", use_test_tables=True, engine=self.engine)

        self.assertEqual(result["written"], 1)
        self.assertEqual(card["earnings_date"], dash.ETF_EARNINGS_NOTE)
        self.assertIn("ETF", card["earnings_source"])
        self.assertIn("成分板块轮动", card["recent_hotspot"])
        self.assertIn("期权判断：", card["option_data"])
        self.assertIn("主要依据：", card["option_data"])

    def test_rebuild_underlying_profile_cache_combines_news_web_and_llm_summary(self):
        analyst_ref = dash._classify_profile_source_ref(
            {"source": "Web Search", "title": "Analyst upgrades Disney after streaming profit improves"}
        )
        news_ref = dash._classify_profile_source_ref(
            {"source": "Yahoo Finance News", "title": "Disney parks demand remains resilient"}
        )
        with patch.object(dash, "fetch_nasdaq_next_earnings_dates", return_value={}), patch.object(
            dash, "_collect_profile_news_context", return_value=[news_ref]
        ), patch.object(
            dash, "_collect_profile_web_search_context", return_value=[analyst_ref]
        ), patch.object(
            dash,
            "_build_profile_llm_json",
            return_value={
                "recent_hotspot": "分析师上调叠加乐园需求韧性，市场关注流媒体利润修复。",
                "option_data": "期权数据看财报前IV升温和Call端追涨溢价，信号偏多。",
                "confidence": "high",
            },
        ):
            result = dash.rebuild_underlying_profile_cache(
                underlyings=["DIS"],
                as_of_date="20260707",
                apply=True,
                use_test_tables=True,
                engine=self.engine,
            )

        card = dash.build_underlying_profile_card("DIS", use_test_tables=True, engine=self.engine)
        source_kinds = {ref.get("kind") for ref in card["dynamic_source_refs"]}

        self.assertEqual(result["written"], 1)
        self.assertEqual(result["news_refs"], 1)
        self.assertEqual(result["web_refs"], 1)
        self.assertIn("分析师上调", card["recent_hotspot"])
        self.assertIn("期权判断：", card["option_data"])
        self.assertIn("主要依据：", card["option_data"])
        self.assertNotIn("财报前IV升温", card["option_data"])
        self.assertIn("analyst", source_kinds)

    def test_load_market_climate_strip_missing_tables_returns_placeholders(self):
        cards = dash.load_market_climate_strip(engine=self.engine, today=pd.Timestamp("2026-07-01").date())

        self.assertEqual([card["label"] for card in cards], dash.MARKET_CLIMATE_CARD_ORDER)
        self.assertEqual(len(cards), 8)
        self.assertTrue(all(card["value"] == "--" for card in cards))
        self.assertTrue(all(card["freshness"] == "missing" for card in cards))

    def test_load_market_climate_strip_uses_cached_and_macro_rows(self):
        self._create_market_climate_tables()

        cards = dash.load_market_climate_strip(engine=self.engine, today=pd.Timestamp("2026-07-01").date())
        by_label = {card["label"]: card for card in cards}

        self.assertEqual([card["label"] for card in cards], dash.MARKET_CLIMATE_CARD_ORDER)
        self.assertEqual(by_label["VIX期限"]["value"], "-1.4点")
        self.assertIn("VIX 16.2", by_label["VIX期限"]["detail"])
        self.assertEqual(by_label["利率曲线"]["value"], "4.38%")
        self.assertIn("10Y-3M +0.57pp", by_label["利率曲线"]["detail"])
        self.assertEqual(by_label["实际利率"]["value"], "2.16%")
        self.assertIn("5日 +6bp", by_label["实际利率"]["detail"])
        self.assertEqual(by_label["政策预期"]["value"], "维持 62%")
        self.assertIn("会议 07/29", by_label["政策预期"]["detail"])
        self.assertEqual(by_label["AAII情绪"]["value"], "-0.7pp")
        self.assertIn("牛36 熊37", by_label["AAII情绪"]["detail"])
        self.assertEqual(by_label["VIX净仓"]["value"], "-8.2%")
        self.assertIn("净-12,345张", by_label["VIX净仓"]["detail"])
        self.assertEqual(by_label["供应链压力"]["value"], "-0.12")
        self.assertIn("3M +0.18", by_label["供应链压力"]["detail"])
        self.assertEqual(by_label["信用利差"]["value"], "2.80%")
        self.assertIn("1M -40bp", by_label["信用利差"]["detail"])
        self.assertTrue(all(card.get("hint") for card in cards))
        self.assertIn(">0", by_label["VIX期限"]["hint"])
        self.assertIn(">2%", by_label["实际利率"]["hint"])
        self.assertIn(">5%", by_label["信用利差"]["hint"])

    def test_load_market_climate_strip_uses_policy_rate_fallback_without_fedwatch(self):
        self._create_market_climate_tables()
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM market_climate_daily WHERE indicator_code = 'FEDWATCH'"))

        cards = dash.load_market_climate_strip(engine=self.engine, today=pd.Timestamp("2026-07-01").date())
        policy = {card["label"]: card for card in cards}["政策预期"]

        self.assertEqual(policy["value"], "3.62%")
        self.assertIn("SOFR-Fed -1bp", policy["detail"])
        self.assertEqual(policy["freshness"], "fresh")
        self.assertIn("美联储", policy["hint"])

    def test_load_market_climate_strip_marks_stale_cache(self):
        self._create_market_climate_tables()

        cards = dash.load_market_climate_strip(engine=self.engine, today=pd.Timestamp("2026-07-15").date())
        cftc = {card["label"]: card for card in cards}["VIX净仓"]

        self.assertEqual(cftc["freshness"], "stale")
        self.assertIn("旧21天", cftc["detail"])

    def test_load_latest_option_trade_date_prefers_latest_available_option_date(self):
        self._create_option_tables(use_test_tables=True)
        names = dash.option_table_names(True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price)
                    VALUES
                    ('20260116', 'O:SPY260220C00600000', 'SPY', .21, NULL,
                     'provider_snapshot', 500, 606)
                    """
                )
            )

        result = dash.load_latest_option_trade_date("spy", use_test_tables=True, engine=self.engine)

        self.assertEqual(result, "20260116")

    def test_load_available_option_trade_dates_returns_underlying_dates_desc(self):
        self._create_option_tables(use_test_tables=True)
        names = dash.option_table_names(True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['daily']}
                    (trade_date, option_ticker, underlying, open, high, low, close, volume,
                     vwap, transactions, open_interest, source, updated_at)
                    VALUES
                    ('20260116', 'O:SPY260220C00600000', 'SPY', 20, 22, 18, 21, 100,
                     20.5, 30, 500, 'massive', ''),
                    ('20260117', 'O:QQQ260220C00600000', 'QQQ', 20, 22, 18, 21, 100,
                     20.5, 30, 500, 'massive', '')
                    """
                )
            )

        result = dash.load_available_option_trade_dates("spy", use_test_tables=True, engine=self.engine)

        self.assertEqual(result, ["20260116", "20260115"])

    def test_load_oi_defense_history_tracks_call_put_max_oi_by_strike(self):
        self._create_option_tables(use_test_tables=True)
        self._create_stock_prices()
        names = dash.option_table_names(True)
        contracts = [
            {
                "option_ticker": "O:QQQ260320C00610000",
                "underlying": "QQQ",
                "call_put": "C",
                "strike": 610,
                "expiration_date": "2026-03-20",
            },
            {
                "option_ticker": "O:QQQ260417C00610000",
                "underlying": "QQQ",
                "call_put": "C",
                "strike": 610,
                "expiration_date": "2026-04-17",
            },
            {
                "option_ticker": "O:QQQ260320C00620000",
                "underlying": "QQQ",
                "call_put": "C",
                "strike": 620,
                "expiration_date": "2026-03-20",
            },
            {
                "option_ticker": "O:QQQ260320C00900000",
                "underlying": "QQQ",
                "call_put": "C",
                "strike": 900,
                "expiration_date": "2026-03-20",
            },
            {
                "option_ticker": "O:QQQ260918C00700000",
                "underlying": "QQQ",
                "call_put": "C",
                "strike": 700,
                "expiration_date": "2026-09-18",
            },
            {
                "option_ticker": "O:QQQ260320P00590000",
                "underlying": "QQQ",
                "call_put": "P",
                "strike": 590,
                "expiration_date": "2026-03-20",
            },
            {
                "option_ticker": "O:QQQ260417P00590000",
                "underlying": "QQQ",
                "call_put": "P",
                "strike": 590,
                "expiration_date": "2026-04-17",
            },
            {
                "option_ticker": "O:QQQ260320P00580000",
                "underlying": "QQQ",
                "call_put": "P",
                "strike": 580,
                "expiration_date": "2026-03-20",
            },
            {
                "option_ticker": "O:QQQ260320P00400000",
                "underlying": "QQQ",
                "call_put": "P",
                "strike": 400,
                "expiration_date": "2026-03-20",
            },
        ]
        daily_rows = []
        stock_rows = []
        oi_by_ticker = {
            "O:QQQ260320C00610000": 100,
            "O:QQQ260417C00610000": 80,
            "O:QQQ260320C00620000": 150,
            "O:QQQ260320C00900000": 9999,
            "O:QQQ260918C00700000": 9999,
            "O:QQQ260320P00590000": 100,
            "O:QQQ260417P00590000": 120,
            "O:QQQ260320P00580000": 200,
            "O:QQQ260320P00400000": 9999,
        }
        for idx in range(21):
            trade_dt = pd.Timestamp("2026-01-01") + pd.Timedelta(days=idx)
            trade_date = trade_dt.strftime("%Y%m%d")
            stock_rows.append(
                {
                    "date": trade_dt.strftime("%Y-%m-%d"),
                    "symbol": "QQQ",
                    "open": 600,
                    "high": 604,
                    "low": 596,
                    "close": 600,
                    "volume": 1000,
                    "adjClose": 600,
                }
            )
            for ticker, open_interest in oi_by_ticker.items():
                daily_rows.append(
                    {
                        "trade_date": trade_date,
                        "option_ticker": ticker,
                        "underlying": "QQQ",
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                        "volume": 10,
                        "vwap": 1,
                        "transactions": 1,
                        "open_interest": open_interest,
                        "source": "test",
                        "updated_at": "",
                    }
                )

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['contracts']}
                    (option_ticker, underlying, call_put, strike, expiration_date, contract_root,
                     expiration_type, settlement_type, exercise_style, shares_per_contract, source, updated_at)
                    VALUES
                    (:option_ticker, :underlying, :call_put, :strike, :expiration_date, 'QQQ',
                     'monthly', 'physical', '', 100, 'test', '')
                    """
                ),
                contracts,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['daily']}
                    (trade_date, option_ticker, underlying, open, high, low, close, volume,
                     vwap, transactions, open_interest, source, updated_at)
                    VALUES
                    (:trade_date, :option_ticker, :underlying, :open, :high, :low, :close, :volume,
                     :vwap, :transactions, :open_interest, :source, :updated_at)
                    """
                ),
                daily_rows,
            )
            conn.execute(
                text(
                    """
                    INSERT INTO stock_prices
                    (date, symbol, open, high, low, close, volume, adjClose)
                    VALUES
                    (:date, :symbol, :open, :high, :low, :close, :volume, :adjClose)
                    """
                ),
                stock_rows,
            )

        result = dash.load_oi_defense_history(
            "QQQ",
            "20260121",
            window=20,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(len(result), 20)
        self.assertEqual(result["trade_date"].iloc[0], "20260102")
        latest = result.iloc[-1]
        self.assertEqual(latest["trade_date"], "20260121")
        self.assertAlmostEqual(latest["call_strike"], 610.0)
        self.assertAlmostEqual(latest["call_oi"], 180.0)
        self.assertEqual(latest["call_expiration"], "2026-03-20")
        self.assertAlmostEqual(latest["put_strike"], 590.0)
        self.assertAlmostEqual(latest["put_oi"], 220.0)
        self.assertAlmostEqual(latest["put_call_oi"], 420 / 330)
        self.assertAlmostEqual(latest["call_distance_pct"], 10 / 600 * 100)
        self.assertAlmostEqual(latest["put_distance_pct"], -10 / 600 * 100)
        self.assertNotEqual(latest["call_strike"], 900.0)
        self.assertNotEqual(latest["call_strike"], 700.0)

    def test_load_oi_defense_history_prefers_preaggregated_cache(self):
        with self.engine.begin() as conn:
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
                    ('20260120', '2026-01-20', 'SPY', 600, 610, 1000, 1.67, '2026-02-20',
                     590, 900, -1.67, '2026-02-20', 5000, 4500, 0.9),
                    ('20260121', '2026-01-21', 'SPY', 604, 615, 1100, 1.82, '2026-02-20',
                     595, 950, -1.49, '2026-02-20', 5200, 4700, 0.903846)
                    """
                )
            )

        result = dash.load_oi_defense_history(
            "SPY",
            "20260121",
            window=20,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(result["trade_date"].tolist(), ["20260120", "20260121"])
        self.assertAlmostEqual(float(result["call_strike"].iloc[-1]), 615.0)
        self.assertAlmostEqual(float(result["put_call_oi"].iloc[-1]), 0.903846)

    def test_load_oi_defense_history_ignores_stale_preaggregated_cache(self):
        self._create_option_tables(use_test_tables=True)
        with self.engine.begin() as conn:
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
                    ('20260114', '2026-01-14', 'SPY', 590, 999, 9999, 69.32, '2026-02-20',
                     111, 8888, -81.19, '2026-02-20', 9999, 8888, 0.8888)
                    """
                )
            )

        result = dash.load_oi_defense_history(
            "SPY",
            "20260115",
            window=20,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(result["trade_date"].tolist(), ["20260115"])
        latest = result.iloc[-1]
        self.assertAlmostEqual(float(latest["call_strike"]), 600.0)
        self.assertAlmostEqual(float(latest["call_oi"]), 500.0)
        self.assertAlmostEqual(float(latest["put_strike"]), 600.0)
        self.assertAlmostEqual(float(latest["put_oi"]), 450.0)
        self.assertAlmostEqual(float(latest["put_call_oi"]), 450 / 500)

    def test_oi_defense_y_axis_range_pads_strike_extremes(self):
        df = pd.DataFrame(
            {
                "underlying_close": [540.0, 517.82],
                "call_strike": [500.0, 500.0],
                "put_strike": [590.0, 400.0],
            }
        )

        axis_range = dash.oi_defense_y_axis_range(df)

        self.assertIsNotNone(axis_range)
        self.assertLess(axis_range[0], 400.0)
        self.assertGreater(axis_range[1], 590.0)
        self.assertGreaterEqual(400.0 - axis_range[0], 20.0)
        self.assertGreaterEqual(axis_range[1] - 590.0, 20.0)

    def test_load_oi_defense_history_degrades_without_oi_rows(self):
        self._create_option_tables(use_test_tables=True)

        result = dash.load_oi_defense_history(
            "QQQ",
            "20260121",
            window=20,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertTrue(result.empty)
        self.assertIn("call_strike", result.columns)

    def test_compute_option_anomaly_scan_flags_oi_build_volume_premium_and_gaps(self):
        self._insert_option_anomaly_rows()

        scan = dash.compute_option_anomaly_scan(
            trade_date="20260115",
            underlyings=["SPY"],
            lookback_days=10,
            max_dte=30,
            min_volume=0,
            min_premium=250_000,
            min_oi_change=50,
            min_history_days=5,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertFalse(scan.empty)
        build_rows = scan[scan["option_ticker"] == "O:SPY260130C00630000"]
        self.assertEqual(set(build_rows["signal_family"]), {"oi_build", "volume_oi", "premium"})
        build = build_rows[build_rows["signal_family"] == "oi_build"].iloc[0]
        self.assertAlmostEqual(float(build["oi_change"]), 330.0)
        self.assertAlmostEqual(float(build["oi_change_pct"]), 330 / 120)
        self.assertAlmostEqual(float(build["premium_est"]), 4.5 * 700 * 100)
        self.assertAlmostEqual(float(build["volume_oi_ratio"]), 700 / 450)
        self.assertAlmostEqual(float(build["iv_change_1d"]), 5.0)
        self.assertEqual(int(build["history_days"]), 5)
        self.assertAlmostEqual(float(build["historical_avg_oi_change"]), 7.5)
        self.assertAlmostEqual(float(build["historical_max_oi_change"]), 10.0)
        self.assertAlmostEqual(float(build["oi_change_multiple"]), 44.0)
        tags = set(json.loads(build["tags_json"]))
        self.assertTrue(
            {
                "OI大幅净增",
                "OI增量异常",
                "高于历史均值",
                "突破历史增量",
                "Volume>OI",
                "大额权利金",
                "OTM埋伏",
                "近月增仓",
                "历史新高OI",
            }
            <= tags
        )
        self.assertNotIn("历史样本不足", tags)
        self.assertEqual(build["data_gap"], "")

    def test_compute_option_anomaly_scan_handles_decrease_zero_oi_and_short_history(self):
        self._insert_option_anomaly_rows()

        scan = dash.compute_option_anomaly_scan(
            trade_date="20260115",
            underlyings=["SPY"],
            lookback_days=10,
            max_dte=30,
            min_volume=0,
            min_premium=250_000,
            min_oi_change=50,
            min_history_days=5,
            use_test_tables=True,
            engine=self.engine,
        )

        decrease_rows = scan[scan["option_ticker"] == "O:SPY260130P00570000"]
        self.assertEqual(set(decrease_rows["signal_family"]), {"volume_oi", "premium"})
        self.assertNotIn("oi_build", set(decrease_rows["signal_family"]))
        self.assertLess(float(decrease_rows["oi_change"].iloc[0]), 0)

        zero_oi = scan[scan["option_ticker"] == "O:SPY260130C00650000"]
        self.assertEqual(set(zero_oi["signal_family"]), {"volume_oi"})
        self.assertAlmostEqual(float(zero_oi["open_interest"].iloc[0]), 0.0)
        self.assertAlmostEqual(float(zero_oi["volume_oi_ratio"].iloc[0]), 200.0)
        self.assertAlmostEqual(float(zero_oi["premium_est"].iloc[0]), 5.0 * 200 * 100)

        sparse_rows = scan[scan["option_ticker"] == "O:SPY260130P00550000"]
        self.assertEqual(set(sparse_rows["signal_family"]), {"volume_oi"})
        self.assertNotIn("oi_build", set(sparse_rows["signal_family"]))
        sparse = sparse_rows.iloc[0]
        sparse_tags = set(json.loads(sparse["tags_json"]))
        self.assertIn("历史样本不足", sparse_tags)
        self.assertIn("insufficient_oi_history", sparse["data_gap"])
        self.assertIn("OTM埋伏", sparse_tags)
        self.assertEqual(int(sparse["history_days"]), 1)

        normal_positive = scan[scan["option_ticker"] == "O:SPY260130C00610000"]
        self.assertTrue(normal_positive.empty)

        new_position = scan[
            (scan["option_ticker"] == "O:SPY260130P00530000")
            & (scan["signal_family"] == "oi_build")
        ].iloc[0]
        new_position_tags = set(json.loads(new_position["tags_json"]))
        self.assertAlmostEqual(float(new_position["oi_change"]), 180.0)
        self.assertIn("新仓突增", new_position_tags)
        self.assertIn("历史样本不足", new_position_tags)
        self.assertIn("OI增量异常", new_position_tags)

    def test_option_anomaly_cache_rebuild_is_idempotent_for_same_day(self):
        self._insert_option_anomaly_rows()

        first = dash.rebuild_option_anomaly_scan_cache(
            trade_date="20260115",
            underlyings=["SPY"],
            lookback_days=10,
            max_dte=30,
            min_volume=0,
            min_premium=250_000,
            min_oi_change=50,
            min_history_days=5,
            use_test_tables=True,
            engine=self.engine,
        )
        second = dash.rebuild_option_anomaly_scan_cache(
            trade_date="20260115",
            underlyings=["SPY"],
            lookback_days=10,
            max_dte=30,
            min_volume=0,
            min_premium=250_000,
            min_oi_change=50,
            min_history_days=5,
            use_test_tables=True,
            engine=self.engine,
        )
        table = dash.option_anomaly_scan_cache_table(use_test_tables=True)
        with self.engine.connect() as conn:
            row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            dupes = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM (
                        SELECT trade_date, option_ticker, signal_family, COUNT(*) AS n
                        FROM {table}
                        GROUP BY trade_date, option_ticker, signal_family
                        HAVING n > 1
                    ) d
                    """
                )
            ).scalar()

        cached = dash.load_option_anomaly_scan(
            trade_date="20260115",
            underlyings=["SPY"],
            prefer_cache=True,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(first["status"], "updated")
        self.assertEqual(first["rows"], second["rows"])
        self.assertEqual(row_count, first["rows"])
        self.assertEqual(dupes, 0)
        self.assertEqual(len(cached), first["rows"])

    def test_option_anomaly_source_query_has_no_raw_limit_before_filtering(self):
        source = inspect.getsource(dash._select_option_anomaly_source_rows)

        self.assertNotIn("LIMIT", source.upper())
        self.assertIn("WHERE d.trade_date = :trade_date", source)
        self.assertIn("d.underlying IN", source)

    def test_load_option_chain_daily_normalizes_iv_and_cycle(self):
        self._create_option_tables(use_test_tables=True)

        df = dash.load_option_chain_daily(
            "SPY",
            "20260115",
            use_test_tables=True,
            include_short_cycle=True,
            engine=self.engine,
        )

        self.assertEqual(len(df), 3)
        self.assertEqual(set(df["cycle_label"]), {"monthly", "1DTE"})
        self.assertEqual(float(df[df["call_put"] == "P"]["iv_pct"].iloc[0]), 22.0)
        self.assertIn("moneyness_pct", df.columns)

    def test_summarize_option_chain_counts_monthly_and_short_cycle(self):
        self._create_option_tables(use_test_tables=True)
        df = dash.load_option_chain_daily("SPY", "20260115", use_test_tables=True, engine=self.engine)

        summary = dash.summarize_option_chain(df)

        self.assertEqual(summary["rows"], 3)
        self.assertEqual(summary["monthly"], 2)
        self.assertEqual(summary["short_cycle"], 1)
        self.assertEqual(summary["one_dte"], 1)

    def test_load_option_chain_summary_counts_without_full_chain(self):
        self._create_option_tables(use_test_tables=True)

        summary = dash.load_option_chain_summary(
            "SPY",
            "20260115",
            include_short_cycle=True,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(summary["rows"], 3)
        self.assertEqual(summary["monthly"], 2)
        self.assertEqual(summary["short_cycle"], 1)
        self.assertEqual(summary["zero_dte"], 0)
        self.assertEqual(summary["one_dte"], 1)
        self.assertEqual(summary["expirations"], 2)
        self.assertEqual(summary["provider_iv_rows"], 2)
        self.assertEqual(summary["computed_iv_rows"], 1)
        self.assertEqual(summary["open_interest_rows"], 2)

    def test_load_option_chain_summary_can_exclude_short_cycle(self):
        self._create_option_tables(use_test_tables=True)

        summary = dash.load_option_chain_summary(
            "SPY",
            "20260115",
            include_short_cycle=False,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(summary["rows"], 2)
        self.assertEqual(summary["monthly"], 2)
        self.assertEqual(summary["short_cycle"], 0)
        self.assertEqual(summary["one_dte"], 0)
        self.assertEqual(summary["provider_iv_rows"], 1)
        self.assertEqual(summary["computed_iv_rows"], 1)
        self.assertEqual(summary["open_interest_rows"], 2)

    def test_load_option_chain_summary_can_skip_iv_counts(self):
        self._create_option_tables(use_test_tables=True)

        summary = dash.load_option_chain_summary(
            "SPY",
            "20260115",
            include_short_cycle=True,
            include_iv_counts=False,
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(summary["rows"], 3)
        self.assertEqual(summary["monthly"], 2)
        self.assertEqual(summary["short_cycle"], 1)
        self.assertEqual(summary["provider_iv_rows"], 0)
        self.assertEqual(summary["computed_iv_rows"], 0)
        self.assertEqual(summary["open_interest_rows"], 2)

    def test_load_iv_history_uses_monthly_contracts_only(self):
        self._create_option_tables(use_test_tables=True)

        df = dash.load_iv_history("SPY", window=252, use_test_tables=True, engine=self.engine)

        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(float(df["iv_pct"].iloc[0]), 20.947368421, places=6)

    def test_load_market_metrics_history_reads_test_table(self):
        self._create_option_tables(use_test_tables=True)
        names = dash.option_table_names(True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['metrics']}
                    (trade_date, underlying, atm_iv_pct, iv_change_1d, iv_rv20_spread,
                     term_slope_30_60, term_state, put_skew_5pct, put_call_oi, source)
                    VALUES
                    ('20260114', 'SPY', 19.5, -0.2, 1.5, -1.0, 'Backwardation', 3.0, 1.2, 'local_metrics'),
                    ('20260115', 'SPY', 20.0, 0.5, 2.0, -2.0, 'Backwardation', 5.0, 1.5, 'local_metrics')
                    """
                )
            )

        df = dash.load_market_metrics_history("SPY", window=252, use_test_tables=True, engine=self.engine)

        self.assertEqual(df["trade_date"].tolist(), ["20260114", "20260115"])
        self.assertAlmostEqual(float(df["iv_rv20_spread"].iloc[-1]), 2.0)
        self.assertEqual(df["term_state"].iloc[-1], "Backwardation")

    def test_load_volatility_cone_history_builds_dte_percentile_buckets(self):
        self._create_option_tables(use_test_tables=True)
        names = dash.option_table_names(True)
        contract_rows = []
        iv_rows = []
        for idx, trade_date in enumerate(["20260101", "20260102", "20260103"], start=1):
            base = pd.to_datetime(trade_date, format="%Y%m%d")
            for dte, iv_value in [(7, 0.10 + idx / 100), (30, 0.20 + idx / 100)]:
                expiration = (base + pd.Timedelta(days=dte)).strftime("%Y-%m-%d")
                ticker = f"O:SPY{trade_date}{dte:03d}C00100000"
                contract_rows.append(
                    {
                        "option_ticker": ticker,
                        "underlying": "SPY",
                        "call_put": "C",
                        "strike": 100,
                        "expiration_date": expiration,
                        "contract_root": "SPY",
                        "expiration_type": "monthly",
                        "settlement_type": "physical",
                        "exercise_style": "",
                        "shares_per_contract": 100,
                        "source": "test",
                        "updated_at": "",
                    }
                )
                iv_rows.append(
                    {
                        "trade_date": trade_date,
                        "option_ticker": ticker,
                        "underlying": "SPY",
                        "provider_iv": iv_value,
                        "computed_iv": None,
                        "iv_source": "provider_snapshot",
                        "open_interest": 100 + idx,
                        "underlying_price": 100,
                    }
                )
        with self.engine.begin() as conn:
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
                contract_rows,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price)
                    VALUES (:trade_date, :option_ticker, :underlying, :provider_iv, :computed_iv,
                            :iv_source, :open_interest, :underlying_price)
                    """
                ),
                iv_rows,
            )

        cone = dash.load_volatility_cone_history(
            "SPY",
            "20260103",
            window=3,
            dte_targets=(7, 30),
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(cone["dte_target"].tolist(), [7, 30])
        self.assertEqual(cone["sample_count"].tolist(), [3, 3])
        self.assertAlmostEqual(float(cone.loc[cone["dte_target"] == 7, "p50"].iloc[0]), 12.0)
        self.assertAlmostEqual(float(cone.loc[cone["dte_target"] == 30, "p50"].iloc[0]), 22.0)
        self.assertAlmostEqual(float(cone.loc[cone["dte_target"] == 7, "p10"].iloc[0]), 11.2)

    def test_load_volatility_cone_history_missing_tables_returns_empty(self):
        cone = dash.load_volatility_cone_history(
            "SPY",
            "20260103",
            window=3,
            dte_targets=(7, 30),
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertTrue(cone.empty)
        self.assertEqual(cone.columns.tolist(), dash.VOLATILITY_CONE_COLUMNS)

    def test_load_volatility_cone_history_uses_daily_cache_table(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {dash.VOLATILITY_CONE_DAILY_CACHE_TABLE} (
                        trade_date TEXT,
                        underlying TEXT,
                        dte_target INTEGER,
                        dte REAL,
                        expiration_date TEXT,
                        iv_pct REAL,
                        sample_count INTEGER
                    )
                    """
                )
            )
            rows = []
            for idx, trade_date in enumerate(["20260101", "20260102", "20260103"], start=1):
                rows.extend(
                    [
                        {
                            "trade_date": trade_date,
                            "underlying": "SPY",
                            "dte_target": 7,
                            "dte": 7,
                            "expiration_date": "2026-01-10",
                            "iv_pct": 10.0 + idx,
                            "sample_count": 2,
                        },
                        {
                            "trade_date": trade_date,
                            "underlying": "SPY",
                            "dte_target": 30,
                            "dte": 30,
                            "expiration_date": "2026-02-01",
                            "iv_pct": 20.0 + idx,
                            "sample_count": 2,
                        },
                    ]
                )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {dash.VOLATILITY_CONE_DAILY_CACHE_TABLE}
                    (trade_date, underlying, dte_target, dte, expiration_date, iv_pct, sample_count)
                    VALUES (:trade_date, :underlying, :dte_target, :dte, :expiration_date, :iv_pct, :sample_count)
                    """
                ),
                rows,
            )

        cone = dash.load_volatility_cone_history(
            "SPY",
            "20260103",
            window=3,
            dte_targets=(7, 30),
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(cone["dte_target"].tolist(), [7, 30])
        self.assertEqual(cone["sample_count"].tolist(), [3, 3])
        self.assertAlmostEqual(float(cone.loc[cone["dte_target"] == 7, "p50"].iloc[0]), 12.0)
        self.assertAlmostEqual(float(cone.loc[cone["dte_target"] == 30, "p90"].iloc[0]), 22.8)

    def test_load_volatility_cone_history_falls_back_when_cache_is_too_short(self):
        self._create_option_tables(use_test_tables=True)
        names = dash.option_table_names(True)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE {dash.VOLATILITY_CONE_DAILY_CACHE_TABLE} (
                        trade_date TEXT,
                        underlying TEXT,
                        dte_target INTEGER,
                        dte REAL,
                        expiration_date TEXT,
                        iv_pct REAL,
                        sample_count INTEGER
                    )
                    """
                )
            )
            cache_rows = []
            for idx in range(5):
                trade_date = (pd.Timestamp("2026-01-21") + pd.Timedelta(days=idx)).strftime("%Y%m%d")
                cache_rows.extend(
                    [
                        {"trade_date": trade_date, "underlying": "SPY", "dte_target": 7, "dte": 7, "expiration_date": "2026-02-01", "iv_pct": 99.0, "sample_count": 1},
                        {"trade_date": trade_date, "underlying": "SPY", "dte_target": 30, "dte": 30, "expiration_date": "2026-03-01", "iv_pct": 99.0, "sample_count": 1},
                    ]
                )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {dash.VOLATILITY_CONE_DAILY_CACHE_TABLE}
                    (trade_date, underlying, dte_target, dte, expiration_date, iv_pct, sample_count)
                    VALUES (:trade_date, :underlying, :dte_target, :dte, :expiration_date, :iv_pct, :sample_count)
                    """
                ),
                cache_rows,
            )

            contract_rows = []
            iv_rows = []
            for idx in range(25):
                trade_date = (pd.Timestamp("2026-01-01") + pd.Timedelta(days=idx)).strftime("%Y%m%d")
                base = pd.to_datetime(trade_date, format="%Y%m%d")
                for dte, iv_value in [(7, 0.10 + idx / 100), (30, 0.20 + idx / 100)]:
                    expiration = (base + pd.Timedelta(days=dte)).strftime("%Y-%m-%d")
                    ticker = f"O:SPYFALLBACK{idx:02d}{dte:03d}C00100000"
                    contract_rows.append(
                        {
                            "option_ticker": ticker,
                            "underlying": "SPY",
                            "call_put": "C",
                            "strike": 100,
                            "expiration_date": expiration,
                            "contract_root": "SPY",
                            "expiration_type": "monthly",
                            "settlement_type": "physical",
                            "exercise_style": "",
                            "shares_per_contract": 100,
                            "source": "test",
                            "updated_at": "",
                        }
                    )
                    iv_rows.append(
                        {
                            "trade_date": trade_date,
                            "option_ticker": ticker,
                            "underlying": "SPY",
                            "provider_iv": iv_value,
                            "computed_iv": None,
                            "iv_source": "provider_snapshot",
                            "open_interest": 100,
                            "underlying_price": 100,
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
                contract_rows,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price)
                    VALUES (:trade_date, :option_ticker, :underlying, :provider_iv, :computed_iv,
                            :iv_source, :open_interest, :underlying_price)
                    """
                ),
                iv_rows,
            )

        cone = dash.load_volatility_cone_history(
            "SPY",
            "20260125",
            window=252,
            dte_targets=(7, 30),
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(cone["dte_target"].tolist(), [7, 30])
        self.assertEqual(cone["sample_count"].tolist(), [25, 25])
        self.assertNotAlmostEqual(float(cone.loc[cone["dte_target"] == 7, "p50"].iloc[0]), 99.0)

    def test_build_otm_volatility_curve_uses_only_otm_put_and_call(self):
        chain = pd.DataFrame(
            [
                {"call_put": "P", "moneyness_pct": -5, "iv_pct": 30, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "P", "moneyness_pct": -0.2, "iv_pct": 55, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "P", "moneyness_pct": 5, "iv_pct": 80, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "C", "moneyness_pct": 5, "iv_pct": 20, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "C", "moneyness_pct": 0.2, "iv_pct": 45, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "C", "moneyness_pct": -5, "iv_pct": 70, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "C", "moneyness_pct": 0, "iv_pct": 60, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "C", "moneyness_pct": 4, "iv_pct": 99, "open_interest": 100, "dte": 60, "expiration_date": "2026-03-20"},
            ]
        )

        curve = dash.build_otm_volatility_curve(chain)

        self.assertEqual(curve["moneyness_pct"].tolist(), [-5.0, 5.0])
        self.assertEqual(curve["call_put"].tolist(), ["P", "C"])
        self.assertAlmostEqual(float(curve["iv_pct"].iloc[0]), 30.0)
        self.assertAlmostEqual(float(curve["iv_pct"].iloc[1]), 20.0)
        self.assertEqual(curve["quality"].tolist(), ["sparse", "sparse"])
        self.assertEqual(curve["point_count"].tolist(), [1, 1])

    def test_build_binned_otm_volatility_curve_uses_single_front_expiration(self):
        chain = pd.DataFrame(
            [
                {"call_put": "P", "moneyness_pct": -6.1, "iv_pct": 32, "open_interest": 100, "dte": 29, "expiration_date": "2026-02-19"},
                {"call_put": "P", "moneyness_pct": -4.1, "iv_pct": 30, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "P", "moneyness_pct": -2.1, "iv_pct": 31, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "C", "moneyness_pct": 2.1, "iv_pct": 21, "open_interest": 100, "dte": 29, "expiration_date": "2026-02-19"},
                {"call_put": "C", "moneyness_pct": 4.1, "iv_pct": 20, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "C", "moneyness_pct": 6.1, "iv_pct": 22, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
            ]
        )

        curve = dash.build_binned_otm_volatility_curve(chain)

        self.assertEqual(curve["moneyness_pct"].tolist(), [-6.1, 2.1])
        self.assertEqual(curve["expiration_date"].tolist(), ["2026-02-19", "2026-02-19"])
        self.assertEqual(curve["quality"].tolist(), ["sparse", "sparse"])
        self.assertEqual(curve["expiration_count"].tolist(), [1, 1])

    def test_build_binned_otm_volatility_curve_does_not_extrapolate_far_tails(self):
        chain = pd.DataFrame(
            [
                {"call_put": "P", "moneyness_pct": -9.6, "iv_pct": 50, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
                {"call_put": "C", "moneyness_pct": 9.6, "iv_pct": 40, "open_interest": 100, "dte": 30, "expiration_date": "2026-02-20"},
            ]
        )

        curve = dash.build_binned_otm_volatility_curve(chain)

        self.assertTrue(curve.empty)
        self.assertEqual(curve.columns.tolist(), dash.OTM_VOLATILITY_CURVE_COLUMNS)

    def test_load_volatility_cone_line_snapshot_aggregates_small_slice(self):
        self._create_option_tables(use_test_tables=True)

        line = dash.load_volatility_cone_line_snapshot(
            "SPY",
            "20260115",
            dte_targets=(1, 30),
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(line["dte_target"].tolist(), [1, 30])
        near_30 = line[line["dte_target"] == 30].iloc[0]
        self.assertEqual(float(near_30["dte"]), 36.0)
        self.assertAlmostEqual(float(near_30["iv_pct"]), 20.947368421, places=6)

    def test_load_otm_volatility_curve_snapshot_uses_only_otm_put_and_call(self):
        self._create_option_tables(use_test_tables=True)
        names = dash.option_table_names(True)
        contract_rows = [
            ("O:SPY260206P00570000", "P", 570, "2026-02-06", "weekly", 0.99),
            ("O:SPY260206C00630000", "C", 630, "2026-02-06", "weekly", 0.88),
            ("O:SPY260220P00570000", "P", 570, "2026-02-20", "monthly", 0.30),
            ("O:SPY260220P00630000", "P", 630, "2026-02-20", "monthly", 0.70),
            ("O:SPY260220C00570000", "C", 570, "2026-02-20", "monthly", 0.80),
            ("O:SPY260220C00630000", "C", 630, "2026-02-20", "monthly", 0.20),
        ]
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['contracts']}
                    (option_ticker, underlying, call_put, strike, expiration_date, contract_root,
                     expiration_type, settlement_type, exercise_style, shares_per_contract, source, updated_at)
                    VALUES (:option_ticker, 'SPY', :call_put, :strike, :expiration_date, 'SPY',
                            :expiration_type, 'physical', '', 100, 'test', '')
                    """
                ),
                [
                    {
                        "option_ticker": ticker,
                        "call_put": call_put,
                        "strike": strike,
                        "expiration_date": expiration_date,
                        "expiration_type": expiration_type,
                    }
                    for ticker, call_put, strike, expiration_date, expiration_type, _iv in contract_rows
                ],
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price)
                    VALUES ('20260115', :option_ticker, 'SPY', :provider_iv, NULL,
                            'provider_snapshot', 100, 600)
                    """
                ),
                [
                    {"option_ticker": ticker, "provider_iv": iv_value}
                    for ticker, _call_put, _strike, _expiration_date, _expiration_type, iv_value in contract_rows
                ],
            )

        curve = dash.load_otm_volatility_curve_snapshot(
            "SPY",
            "20260115",
            use_test_tables=True,
            underlying_price=600,
            engine=self.engine,
        )

        self.assertEqual(curve["moneyness_pct"].tolist(), [-5.0, 5.0])
        self.assertEqual(curve["call_put"].tolist(), ["P", "C"])
        self.assertEqual(curve["expiration_date"].tolist(), ["2026-02-20", "2026-02-20"])
        self.assertAlmostEqual(float(curve["iv_pct"].iloc[0]), 30.0)
        self.assertAlmostEqual(float(curve["iv_pct"].iloc[1]), 20.0)
        self.assertEqual(curve["quality"].tolist(), ["sparse", "sparse"])
        self.assertEqual(curve["expiration_count"].tolist(), [1, 1])

    def test_load_option_surface_snapshot_filters_to_needed_slice(self):
        self._create_option_tables(use_test_tables=True)
        names = dash.option_table_names(True)
        contract_rows = [
            {
                "option_ticker": "O:SPY260220C00750000",
                "underlying": "SPY",
                "call_put": "C",
                "strike": 750,
                "expiration_date": "2026-02-20",
                "contract_root": "SPY",
                "expiration_type": "monthly",
                "settlement_type": "physical",
                "exercise_style": "",
                "shares_per_contract": 100,
                "source": "test",
                "updated_at": "",
            },
            {
                "option_ticker": "O:SPY270115C00600000",
                "underlying": "SPY",
                "call_put": "C",
                "strike": 600,
                "expiration_date": "2027-01-15",
                "contract_root": "SPY",
                "expiration_type": "monthly",
                "settlement_type": "physical",
                "exercise_style": "",
                "shares_per_contract": 100,
                "source": "test",
                "updated_at": "",
            },
        ]
        daily_rows = [
            {
                "trade_date": "20260115",
                "option_ticker": row["option_ticker"],
                "underlying": "SPY",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
                "vwap": 1,
                "transactions": 1,
                "open_interest": 1,
                "source": "test",
                "updated_at": "",
            }
            for row in contract_rows
        ]
        iv_rows = [
            {
                "trade_date": "20260115",
                "option_ticker": row["option_ticker"],
                "underlying": "SPY",
                "provider_iv": 0.25,
                "computed_iv": None,
                "iv_source": "provider_snapshot",
                "open_interest": 1,
                "underlying_price": 600,
            }
            for row in contract_rows
        ]
        with self.engine.begin() as conn:
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
                contract_rows,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['daily']}
                    (trade_date, option_ticker, underlying, open, high, low, close, volume,
                     vwap, transactions, open_interest, source, updated_at)
                    VALUES (:trade_date, :option_ticker, :underlying, :open, :high, :low, :close, :volume,
                            :vwap, :transactions, :open_interest, :source, :updated_at)
                    """
                ),
                daily_rows,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['iv']}
                    (trade_date, option_ticker, underlying, provider_iv, computed_iv,
                     iv_source, open_interest, underlying_price)
                    VALUES (:trade_date, :option_ticker, :underlying, :provider_iv, :computed_iv,
                            :iv_source, :open_interest, :underlying_price)
                    """
                ),
                iv_rows,
            )

        surface = dash.load_option_surface_snapshot(
            "SPY",
            "20260115",
            moneyness_range=10,
            max_dte=60,
            use_test_tables=True,
            underlying_price=600,
            engine=self.engine,
        )

        self.assertFalse(surface.empty)
        self.assertNotIn("O:SPY260220C00750000", surface["option_ticker"].tolist())
        self.assertNotIn("O:SPY270115C00600000", surface["option_ticker"].tolist())
        self.assertLessEqual(float(surface["moneyness_pct"].abs().max()), 10.0)
        self.assertLessEqual(float(pd.to_numeric(surface["dte"], errors="coerce").max()), 60.0)

    def test_apply_historical_percentiles_uses_252_day_history(self):
        rows = []
        for idx in range(70):
            rows.append(
                {
                    "trade_date": (pd.Timestamp("2025-11-01") + pd.Timedelta(days=idx)).strftime("%Y%m%d"),
                    "underlying": "SPY",
                    "iv_change_1d": float(idx),
                    "iv_rv20_spread": float(idx - 30),
                    "term_slope_30_60": float(idx / 10),
                    "put_skew_5pct": float(idx / 5),
                    "call_skew_5pct": float(-idx / 10),
                    "put_call_oi": float(idx / 20),
                }
            )
        history = pd.DataFrame(rows)
        current = {
            "iv_change_1d": 69.0,
            "iv_rv20_spread": 39.0,
            "term_slope_30_60": 6.9,
            "put_skew_5pct": 13.8,
            "call_skew_5pct": -6.9,
            "put_call_oi": 69 / 20,
        }

        result = dash.apply_historical_percentiles(
            current,
            history,
            trade_date="20260109",
            window=252,
            min_samples=60,
        )

        self.assertAlmostEqual(result["iv_change_1d_percentile"], 100.0)
        self.assertAlmostEqual(result["iv_rv20_percentile"], 100.0)
        self.assertAlmostEqual(result["term_slope_percentile"], 100.0)
        self.assertAlmostEqual(result["put_skew_5pct_percentile"], 100.0)
        self.assertAlmostEqual(result["call_skew_5pct_percentile"], 1 / 70 * 100)
        self.assertAlmostEqual(result["put_call_skew_5pct"], 20.7)
        self.assertAlmostEqual(result["put_call_skew_5pct_percentile"], 100.0)
        self.assertAlmostEqual(result["put_call_oi_percentile"], 100.0)
        self.assertFalse(result["iv_change_1d_insufficient_history"])

    def test_apply_historical_percentiles_marks_insufficient_history(self):
        history = pd.DataFrame(
            {
                "trade_date": pd.date_range("2026-01-01", periods=10, freq="D").strftime("%Y%m%d"),
                "underlying": ["SPY"] * 10,
                "iv_rv20_spread": list(range(10)),
            }
        )

        result = dash.apply_historical_percentiles(
            {"iv_rv20_spread": 9.0},
            history,
            trade_date="20260110",
            window=252,
            min_samples=60,
        )

        self.assertIsNone(result["iv_rv20_percentile"])
        self.assertEqual(result["iv_rv20_spread_history_count"], 10)
        self.assertTrue(result["iv_rv20_spread_insufficient_history"])

    def test_apply_historical_percentiles_calculates_iv_up_directional_percentile(self):
        history = pd.DataFrame(
            {
                "trade_date": pd.date_range("2025-10-01", periods=70, freq="D").strftime("%Y%m%d"),
                "underlying": ["SPY"] * 70,
                "iv_change_1d": [float(idx + 1) for idx in range(70)],
            }
        )

        result = dash.apply_historical_percentiles(
            {"iv_change_1d": 68.0},
            history,
            trade_date="20251231",
            window=252,
            min_samples=60,
        )

        self.assertAlmostEqual(result["iv_change_1d_percentile"], 68 / 70 * 100)
        self.assertEqual(result["iv_change_1d_direction_label"], "升波分位")
        self.assertEqual(result["iv_change_1d_directional_history_count"], 70)
        self.assertAlmostEqual(result["iv_change_1d_directional_percentile"], 68 / 70 * 100)

    def test_apply_historical_percentiles_calculates_iv_down_directional_percentile(self):
        history = pd.DataFrame(
            {
                "trade_date": pd.date_range("2025-10-01", periods=70, freq="D").strftime("%Y%m%d"),
                "underlying": ["SPY"] * 70,
                "iv_change_1d": [-float(idx + 1) for idx in range(70)],
            }
        )

        result = dash.apply_historical_percentiles(
            {"iv_change_1d": -68.0},
            history,
            trade_date="20251231",
            window=252,
            min_samples=60,
        )

        self.assertAlmostEqual(result["iv_change_1d_percentile"], 3 / 70 * 100)
        self.assertEqual(result["iv_change_1d_direction_label"], "降波分位")
        self.assertEqual(result["iv_change_1d_directional_history_count"], 70)
        self.assertAlmostEqual(result["iv_change_1d_directional_percentile"], 68 / 70 * 100)

    def test_apply_historical_percentiles_requires_direction_samples_for_iv_change(self):
        history = pd.DataFrame(
            {
                "trade_date": pd.date_range("2025-10-01", periods=70, freq="D").strftime("%Y%m%d"),
                "underlying": ["SPY"] * 70,
                "iv_change_1d": [-float(idx + 1) if idx < 20 else float(idx + 1) for idx in range(70)],
            }
        )

        result = dash.apply_historical_percentiles(
            {"iv_change_1d": -18.0},
            history,
            trade_date="20251231",
            window=252,
            min_samples=60,
        )

        self.assertIsNone(result["iv_change_1d_directional_percentile"])
        self.assertEqual(result["iv_change_1d_direction_label"], "降波分位")
        self.assertEqual(result["iv_change_1d_directional_history_count"], 20)
        self.assertTrue(result["iv_change_1d_directional_insufficient_history"])

    def test_apply_historical_percentiles_uses_skew_specific_sample_floor(self):
        history = pd.DataFrame(
            {
                "trade_date": pd.date_range("2025-12-01", periods=25, freq="D").strftime("%Y%m%d"),
                "underlying": ["SPY"] * 25,
                "call_skew_5pct": [float(i) / 10 for i in range(25)],
                "iv_rv20_spread": [float(i) for i in range(25)],
            }
        )

        result = dash.apply_historical_percentiles(
            {"call_skew_5pct": 2.4, "iv_rv20_spread": 24.0},
            history,
            trade_date="20251225",
            window=252,
            min_samples=60,
        )

        self.assertEqual(result["call_skew_5pct_min_samples"], 20)
        self.assertFalse(result["call_skew_5pct_insufficient_history"])
        self.assertIsNotNone(result["call_skew_5pct_percentile"])
        self.assertEqual(result["iv_rv20_spread_min_samples"], 60)
        self.assertTrue(result["iv_rv20_spread_insufficient_history"])

    def test_calculate_realized_volatility_uses_selected_trade_date(self):
        returns = [0.01, -0.004, 0.006, -0.002, 0.008] * 14
        close = 100.0
        rows = [{"date": pd.Timestamp("2025-12-01"), "close": close}]
        for idx, ret in enumerate(returns, start=1):
            close *= 1 + ret
            rows.append({"date": pd.Timestamp("2025-12-01") + pd.Timedelta(days=idx), "close": close})
        stock_df = pd.DataFrame(rows)

        rv = dash.calculate_realized_volatility(stock_df, window=20, trade_date="20260109")
        scoped_returns = pd.Series(returns[:40]).tail(20)
        expected = float(scoped_returns.std() * math.sqrt(252) * 100)

        self.assertAlmostEqual(rv, expected, places=8)

    def test_calculate_overview_metrics_from_market_history_uses_preaggregated_rows(self):
        trade_dates = pd.date_range("2025-11-07", periods=70, freq="D").strftime("%Y%m%d")
        stock_df = pd.DataFrame(
            {
                "date": pd.date_range("2025-11-07", periods=70, freq="D"),
                "close": [100 + idx * 0.2 + ((-1) ** idx) * 0.4 for idx in range(70)],
            }
        )
        metrics_history = pd.DataFrame(
            {
                "trade_date": trade_dates,
                "underlying": ["SPY"] * 70,
                "atm_iv_pct": [15.0 + idx * 0.1 for idx in range(70)],
                "iv_change_1d": [0.1] * 70,
                "iv_rv20_spread": [2.0 + idx * 0.01 for idx in range(70)],
                "iv_30d": [20.0] * 70,
                "iv_60d": [18.0] * 70,
                "term_slope_30_60": [-2.0] * 70,
                "term_state": ["Backwardation"] * 70,
                "skew_expiration": ["2026-02-20"] * 70,
                "put_skew_5pct": [5.0] * 70,
                "call_skew_5pct": [-1.0] * 70,
                "put_call_oi": [1.5] * 70,
                "put_call_volume": [1.2] * 70,
                "zero_dte_volume_share_pct": [8.0] * 70,
                "top_oi_strike": [600.0] * 70,
                "top_oi": [5000.0] * 70,
                "top5_oi_share_pct": [22.0] * 70,
                "total_open_interest": [100000.0] * 70,
                "total_volume": [70000.0] * 70,
                "provider_iv_rows": [1200] * 70,
                "computed_iv_rows": [300] * 70,
                "open_interest_rows": [8500] * 70,
            }
        )

        metrics = dash.calculate_overview_metrics_from_market_history(
            stock_df=stock_df,
            market_metrics_history=metrics_history,
            trade_date=trade_dates[-1],
        )

        self.assertAlmostEqual(metrics["atm_iv_pct"], 21.9)
        self.assertAlmostEqual(metrics["iv_rank"], 100.0)
        self.assertAlmostEqual(metrics["iv_percentile"], 100.0)
        self.assertEqual(metrics["iv_history_days"], 70)
        self.assertAlmostEqual(metrics["iv_change_1d"], 0.1)
        self.assertAlmostEqual(metrics["iv_30d"], 20.0)
        self.assertAlmostEqual(metrics["term_slope_30_60"], -2.0)
        self.assertEqual(metrics["term_state"], "Backwardation")
        self.assertAlmostEqual(metrics["put_skew_5pct"], 5.0)
        self.assertAlmostEqual(metrics["call_skew_5pct"], -1.0)
        self.assertAlmostEqual(metrics["put_call_skew_5pct"], 6.0)
        self.assertAlmostEqual(metrics["put_call_oi"], 1.5)
        self.assertEqual(metrics["provider_iv_rows"], 1200)
        self.assertEqual(metrics["open_interest_rows"], 8500)

    def test_calculate_overview_metrics_backfills_sparse_put_call_oi_history(self):
        self._create_option_tables(use_test_tables=True)
        trade_dates = pd.date_range("2025-10-01", periods=70, freq="D").strftime("%Y%m%d")
        stock_df = pd.DataFrame(
            {
                "date": pd.date_range("2025-10-01", periods=70, freq="D"),
                "close": [600 + idx * 0.5 for idx in range(70)],
            }
        )
        metrics_history = pd.DataFrame(
            {
                "trade_date": trade_dates,
                "underlying": ["SPY"] * 70,
                "atm_iv_pct": [20.0] * 70,
                "put_call_oi": [None] * 70,
            }
        )
        names = dash.option_table_names(True)
        daily_rows = []
        for idx, trade_date in enumerate(trade_dates):
            daily_rows.extend(
                [
                    {
                        "trade_date": trade_date,
                        "option_ticker": "O:SPY260220C00600000",
                        "underlying": "SPY",
                        "open": 20,
                        "high": 22,
                        "low": 18,
                        "close": 21,
                        "volume": 100,
                        "vwap": 20.5,
                        "transactions": 30,
                        "open_interest": 1000,
                        "source": "test",
                        "updated_at": "",
                    },
                    {
                        "trade_date": trade_date,
                        "option_ticker": "O:SPY260220P00600000",
                        "underlying": "SPY",
                        "open": 19,
                        "high": 21,
                        "low": 17,
                        "close": 18,
                        "volume": 90,
                        "vwap": 18.5,
                        "transactions": 25,
                        "open_interest": 700 + idx,
                        "source": "test",
                        "updated_at": "",
                    },
                ]
            )
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {names['daily']}
                    (trade_date, option_ticker, underlying, open, high, low, close, volume,
                     vwap, transactions, open_interest, source, updated_at)
                    VALUES
                    (:trade_date, :option_ticker, :underlying, :open, :high, :low, :close, :volume,
                     :vwap, :transactions, :open_interest, :source, :updated_at)
                    """
                ),
                daily_rows,
            )

        metrics = dash.calculate_overview_metrics_from_market_history(
            stock_df=stock_df,
            market_metrics_history=metrics_history,
            trade_date=trade_dates[-1],
            underlying="SPY",
            use_test_tables=True,
            engine=self.engine,
        )

        self.assertEqual(metrics["put_call_oi_history_count"], 70)
        self.assertAlmostEqual(metrics["put_call_oi"], (700 + 69) / 1000)
        self.assertIsNotNone(metrics["put_call_oi_percentile"])

    def test_calculate_overview_metrics_from_market_history_degrades_without_rows(self):
        metrics = dash.calculate_overview_metrics_from_market_history(
            stock_df=pd.DataFrame(),
            market_metrics_history=pd.DataFrame(),
            trade_date="20260115",
        )

        self.assertIsNone(metrics["atm_iv_pct"])
        self.assertIsNone(metrics["iv_rank"])
        self.assertEqual(metrics["iv_history_days"], 0)
        self.assertEqual(metrics["term_state"], "样本不足")
        self.assertIsNone(metrics["put_call_oi"])

    def test_calculate_volatility_positioning_metrics_ignores_low_oi_skew_outlier(self):
        stock_df = pd.DataFrame(
            {
                "date": pd.date_range("2026-01-01", periods=40, freq="D"),
                "close": [80.0] * 40,
            }
        )
        chain_df = pd.DataFrame(
            [
                {"call_put": "C", "strike": 80, "expiration_date": "2026-02-20", "expiration_type": "monthly", "open_interest": 140000, "volume": 100, "iv_pct": 3.8, "moneyness_pct": 0.3, "dte": 30},
                {"call_put": "P", "strike": 80, "expiration_date": "2026-02-20", "expiration_type": "monthly", "open_interest": 90000, "volume": 100, "iv_pct": 2.2, "moneyness_pct": 0.3, "dte": 30},
                {"call_put": "P", "strike": 76, "expiration_date": "2026-02-20", "expiration_type": "monthly", "open_interest": 120000, "volume": 100, "iv_pct": 8.7, "moneyness_pct": -4.85, "dte": 30},
                {"call_put": "C", "strike": 83, "expiration_date": "2026-02-20", "expiration_type": "monthly", "open_interest": 24000, "volume": 100, "iv_pct": 8.6, "moneyness_pct": 3.92, "dte": 30},
                {"call_put": "C", "strike": 84, "expiration_date": "2026-02-20", "expiration_type": "monthly", "open_interest": 24, "volume": 100, "iv_pct": 101.4, "moneyness_pct": 5.17, "dte": 30},
            ]
        )

        metrics = dash.calculate_volatility_positioning_metrics(
            stock_df=stock_df,
            chain_df=chain_df,
            iv_history=pd.DataFrame(),
            trade_date="20260130",
            current_iv_pct=3.1,
        )

        self.assertIsNotNone(metrics["call_skew_5pct"])
        self.assertLess(metrics["call_skew_5pct"], 20)
        self.assertGreater(metrics["put_skew_5pct"], 3)

    def test_calculate_volatility_positioning_metrics_builds_investor_panel_values(self):
        stock_df = pd.DataFrame(
            {
                "date": pd.date_range("2025-11-10", periods=70, freq="D"),
                "close": [100 + idx * 0.2 + ((-1) ** idx) * 0.3 for idx in range(70)],
            }
        )
        chain_df = pd.DataFrame(
            [
                {
                    "call_put": "C",
                    "strike": 100,
                    "expiration_date": "2026-02-20",
                    "expiration_type": "monthly",
                    "open_interest": 100,
                    "volume": 50,
                    "iv_pct": 20,
                    "moneyness_pct": 0,
                    "dte": 36,
                },
                {
                    "call_put": "P",
                    "strike": 100,
                    "expiration_date": "2026-02-20",
                    "expiration_type": "monthly",
                    "open_interest": 120,
                    "volume": 60,
                    "iv_pct": 20,
                    "moneyness_pct": 0,
                    "dte": 36,
                },
                {
                    "call_put": "P",
                    "strike": 95,
                    "expiration_date": "2026-02-20",
                    "expiration_type": "monthly",
                    "open_interest": 80,
                    "volume": 40,
                    "iv_pct": 25,
                    "moneyness_pct": -5,
                    "dte": 36,
                },
                {
                    "call_put": "C",
                    "strike": 105,
                    "expiration_date": "2026-02-20",
                    "expiration_type": "monthly",
                    "open_interest": 70,
                    "volume": 20,
                    "iv_pct": 18,
                    "moneyness_pct": 5,
                    "dte": 36,
                },
                {
                    "call_put": "C",
                    "strike": 100,
                    "expiration_date": "2026-03-20",
                    "expiration_type": "monthly",
                    "open_interest": 90,
                    "volume": 30,
                    "iv_pct": 18,
                    "moneyness_pct": 0,
                    "dte": 64,
                },
                {
                    "call_put": "P",
                    "strike": 100,
                    "expiration_date": "2026-03-20",
                    "expiration_type": "monthly",
                    "open_interest": 90,
                    "volume": 30,
                    "iv_pct": 18,
                    "moneyness_pct": 0,
                    "dte": 64,
                },
                {
                    "call_put": "C",
                    "strike": 101,
                    "expiration_date": "2026-01-15",
                    "expiration_type": "short_cycle",
                    "open_interest": 10,
                    "volume": 100,
                    "iv_pct": 30,
                    "moneyness_pct": 1,
                    "dte": 0,
                },
                {
                    "call_put": "P",
                    "strike": 99,
                    "expiration_date": "2026-01-15",
                    "expiration_type": "short_cycle",
                    "open_interest": 10,
                    "volume": 300,
                    "iv_pct": 32,
                    "moneyness_pct": -1,
                    "dte": 0,
                },
            ]
        )
        iv_history = pd.DataFrame(
            {
                "trade_date": pd.date_range("2025-12-26", periods=21, freq="D").strftime("%Y%m%d"),
                "iv_pct": [10 + idx * 0.5 for idx in range(21)],
            }
        )

        metrics = dash.calculate_volatility_positioning_metrics(
            stock_df=stock_df,
            chain_df=chain_df,
            iv_history=iv_history,
            trade_date="20260115",
            current_iv_pct=20,
            iv_rank=None,
        )

        self.assertAlmostEqual(metrics["iv_rank"], 100.0)
        self.assertAlmostEqual(metrics["iv_percentile"], 100.0)
        self.assertAlmostEqual(metrics["iv_change_1d"], 0.5)
        self.assertAlmostEqual(metrics["iv_change_1d_percentile"], 100.0)
        self.assertAlmostEqual(metrics["iv_change_5d"], 2.5)
        self.assertAlmostEqual(metrics["iv_change_20d"], 10.0)
        self.assertIsNotNone(metrics["iv_rv20_percentile"])
        self.assertGreaterEqual(metrics["iv_rv20_percentile"], 0)
        self.assertLessEqual(metrics["iv_rv20_percentile"], 100)
        self.assertAlmostEqual(metrics["iv_30d"], 20.0)
        self.assertAlmostEqual(metrics["iv_60d"], 18.0)
        self.assertAlmostEqual(metrics["term_slope_30_60"], -2.0)
        self.assertAlmostEqual(metrics["term_slope_percentile"], 100.0)
        self.assertEqual(metrics["term_state"], "Backwardation")
        self.assertAlmostEqual(metrics["put_skew_5pct"], 5.0)
        self.assertAlmostEqual(metrics["call_skew_5pct"], -2.0)
        self.assertAlmostEqual(metrics["put_call_skew_5pct"], 7.0)
        self.assertAlmostEqual(metrics["put_skew_5pct_percentile"], 100.0)
        self.assertAlmostEqual(metrics["call_skew_5pct_percentile"], 100.0)
        self.assertAlmostEqual(metrics["put_call_skew_5pct_percentile"], 100.0)
        self.assertAlmostEqual(metrics["put_call_oi"], 300 / 270)
        self.assertAlmostEqual(metrics["put_call_volume"], 430 / 200)
        self.assertAlmostEqual(metrics["put_call_oi_percentile"], 2 / 3 * 100)
        self.assertAlmostEqual(metrics["put_call_volume_percentile"], 2 / 3 * 100)
        self.assertAlmostEqual(metrics["zero_dte_volume_share_pct"], 400 / 630 * 100)
        self.assertEqual(metrics["top_oi_strike"], 100.0)
        self.assertAlmostEqual(metrics["top5_oi_share_pct"], 100.0)
        self.assertAlmostEqual(metrics["total_open_interest"], 570)
        self.assertAlmostEqual(metrics["total_volume"], 630)

    def test_calculate_volatility_positioning_metrics_degrades_on_empty_samples(self):
        metrics = dash.calculate_volatility_positioning_metrics(
            stock_df=pd.DataFrame(),
            chain_df=pd.DataFrame(),
            iv_history=pd.DataFrame(),
            trade_date="20260115",
            current_iv_pct=None,
            iv_rank=None,
        )

        self.assertIsNone(metrics["iv_rv20_spread"])
        self.assertIsNone(metrics["iv_rv20_percentile"])
        self.assertIsNone(metrics["iv_change_1d"])
        self.assertEqual(metrics["term_state"], "样本不足")
        self.assertIsNone(metrics["term_slope_percentile"])
        self.assertIsNone(metrics["put_skew_5pct"])
        self.assertIsNone(metrics["put_skew_5pct_percentile"])
        self.assertIsNone(metrics["put_call_oi"])
        self.assertIsNone(metrics["put_call_oi_percentile"])

    def test_collect_option_table_diagnostics(self):
        self._create_option_tables(use_test_tables=True)

        diagnostics = dash.collect_option_table_diagnostics(
            "SPY",
            "20260115",
            use_test_tables=True,
            engine=self.engine,
        )

        tables = pd.DataFrame(diagnostics["tables"])
        self.assertEqual(int(tables.loc[tables["table"] == "us_option_daily_test", "underlying_rows"].iloc[0]), 3)
        self.assertEqual(int(tables["duplicate_keys"].sum()), 0)

    def test_validate_short_cycle_band_flags_out_of_band(self):
        chain = pd.DataFrame(
            [
                {
                    "expiration_type": "short_cycle",
                    "dte": 1,
                    "strike": 660,
                    "underlying_price": 600,
                }
            ]
        )

        result = dash.validate_short_cycle_band(chain, band_pct=5)

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["out_of_band_rows"], 1)


if __name__ == "__main__":
    unittest.main()
