import unittest
import importlib.util
import sys
import types
from unittest.mock import patch

import pandas as pd

if importlib.util.find_spec("tushare") is None:
    tushare_stub = types.ModuleType("tushare")
    tushare_stub.set_token = lambda *_args, **_kwargs: None
    tushare_stub.pro_api = lambda *_args, **_kwargs: None
    sys.modules["tushare"] = tushare_stub

if importlib.util.find_spec("sqlalchemy") is None:
    sqlalchemy_stub = types.ModuleType("sqlalchemy")
    sqlalchemy_stub.create_engine = lambda *_args, **_kwargs: None
    sqlalchemy_stub.text = lambda sql: sql
    sqlalchemy_stub.types = types.SimpleNamespace(
        VARCHAR=lambda *_args, **_kwargs: object(),
        Float=lambda *_args, **_kwargs: object(),
    )
    sys.modules["sqlalchemy"] = sqlalchemy_stub

if importlib.util.find_spec("dotenv") is None:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *_args, **_kwargs: None
    sys.modules["dotenv"] = dotenv_stub

import update_astock_daily as upd


class TestUpdateAstockDaily(unittest.TestCase):
    def test_normalize_akshare_code_preserves_leading_zeroes(self):
        self.assertEqual(upd._normalize_akshare_code(1), "000001.SZ")
        self.assertEqual(upd._normalize_akshare_code("600519"), "600519.SH")

    def test_standardize_akshare_df_converts_amount_to_tushare_unit(self):
        raw = pd.DataFrame(
            [
                {
                    "\u65e5\u671f": "2026-05-29",
                    "\u5f00\u76d8": 10.0,
                    "\u6700\u9ad8": 11.0,
                    "\u6700\u4f4e": 9.0,
                    "\u6536\u76d8": 10.5,
                    "\u6210\u4ea4\u91cf": 10000,
                    "\u6210\u4ea4\u989d": 123456000.0,
                    "\u6da8\u8dcc\u5e45": 1.2,
                }
            ]
        )

        out = upd._standardize_akshare_df(raw, "510300.SH", "CSI300 ETF")

        self.assertEqual(out.loc[0, "trade_date"], "20260529")
        self.assertEqual(out.loc[0, "ts_code"], "510300.SH")
        self.assertAlmostEqual(out.loc[0, "amount"], 123456.0)

    def test_standardize_akshare_spot_etf_converts_shares_and_yuan_to_tushare_units(self):
        raw = pd.DataFrame(
            [
                {
                    "代码": "510300",
                    "名称": "沪深300ETF",
                    "最新价": 4.862,
                    "今开": 4.828,
                    "最高": 4.868,
                    "最低": 4.807,
                    "成交量": 1_325_050_000,
                    "成交额": 6_421_618_436.0,
                    "涨跌幅": 1.12,
                }
            ]
        )

        out = upd._standardize_akshare_spot_df(raw, "20260522", "E")

        self.assertEqual(out.loc[0, "trade_date"], "20260522")
        self.assertEqual(out.loc[0, "ts_code"], "510300.SH")
        self.assertAlmostEqual(out.loc[0, "vol"], 13_250_500.0)
        self.assertAlmostEqual(out.loc[0, "amount"], 6_421_618.436)

    def test_standardize_akshare_spot_stock_keeps_volume_lot_unit(self):
        raw = pd.DataFrame(
            [
                {
                    "代码": "600519",
                    "名称": "贵州茅台",
                    "最新价": 1500.0,
                    "今开": 1490.0,
                    "最高": 1510.0,
                    "最低": 1480.0,
                    "成交量": 123_456,
                    "成交额": 18_518_400_000.0,
                    "涨跌幅": 0.5,
                }
            ]
        )

        out = upd._standardize_akshare_spot_df(raw, "20260522", "S")

        self.assertAlmostEqual(out.loc[0, "vol"], 123_456.0)
        self.assertAlmostEqual(out.loc[0, "amount"], 18_518_400.0)

    @patch("update_astock_daily._save_price_df", return_value=1)
    @patch("update_astock_daily._fetch_akshare_df")
    @patch("update_astock_daily._fetch_tushare_df")
    @patch("update_astock_daily.get_name_map", return_value={"600519.SH": "Moutai"})
    def test_fetch_and_save_data_keeps_tushare_when_target_date_present(
        self, _name_map, mock_tushare, mock_akshare, _save
    ):
        mock_tushare.return_value = pd.DataFrame(
            [
                {
                    "trade_date": "20260529",
                    "ts_code": "600519.SH",
                    "name": "Moutai",
                    "open_price": 10,
                    "high_price": 11,
                    "low_price": 9,
                    "close_price": 10.5,
                    "vol": 1,
                    "amount": 2,
                    "pct_chg": 3,
                }
            ]
        )

        result = upd.fetch_and_save_data("600519.SH", "20260528", "20260529", asset_type="S")

        self.assertEqual(result.source, "tushare")
        self.assertFalse(result.fallback_used)
        mock_akshare.assert_not_called()

    @patch("update_astock_daily._save_price_df", return_value=1)
    @patch("update_astock_daily._fetch_akshare_df")
    @patch("update_astock_daily._fetch_tushare_df")
    @patch("update_astock_daily.get_name_map", return_value={"510300.SH": "CSI300 ETF"})
    def test_fetch_and_save_data_falls_back_when_tushare_empty(
        self, _name_map, mock_tushare, mock_akshare, _save
    ):
        mock_tushare.return_value = pd.DataFrame(columns=upd.TARGET_COLS)
        mock_akshare.return_value = pd.DataFrame(
            [
                {
                    "trade_date": "20260529",
                    "ts_code": "510300.SH",
                    "name": "CSI300 ETF",
                    "open_price": 4,
                    "high_price": 4.1,
                    "low_price": 3.9,
                    "close_price": 4.0,
                    "vol": 1,
                    "amount": 2,
                    "pct_chg": 3,
                }
            ]
        )

        result = upd.fetch_and_save_data("510300.SH", "20260528", "20260529", asset_type="E")

        self.assertEqual(result.source, "akshare")
        self.assertTrue(result.fallback_used)
        self.assertEqual(result.rows_saved, 1)

    @patch("update_astock_daily.fetch_and_save_data")
    @patch("update_astock_daily._fetch_akshare_spot_df")
    @patch("update_astock_daily._save_price_rows", return_value=2)
    @patch("update_astock_daily._fetch_tushare_bulk_df")
    def test_bulk_update_uses_tushare_bulk_when_complete(
        self, mock_bulk, mock_save, mock_spot, mock_symbol
    ):
        mock_bulk.return_value = pd.DataFrame(
            [
                {
                    "trade_date": "20260604",
                    "ts_code": "600519.SH",
                    "name": "Moutai",
                    "open_price": 10,
                    "high_price": 11,
                    "low_price": 9,
                    "close_price": 10.5,
                    "vol": 1,
                    "amount": 2,
                    "pct_chg": 3,
                },
                {
                    "trade_date": "20260604",
                    "ts_code": "000001.SZ",
                    "name": "Ping An",
                    "open_price": 4,
                    "high_price": 4.1,
                    "low_price": 3.9,
                    "close_price": 4.0,
                    "vol": 1,
                    "amount": 2,
                    "pct_chg": 3,
                },
            ]
        )

        results, stats = upd.fetch_and_save_targets_bulk(
            ["600519.SH", "000001.SZ"], "20260604", "S", sleep_seconds=0
        )

        self.assertEqual({r.source for r in results}, {"tushare_bulk"})
        self.assertEqual(stats["tushare_bulk_saved"], 2)
        mock_save.assert_called_once()
        mock_spot.assert_not_called()
        mock_symbol.assert_not_called()

    @patch("update_astock_daily.fetch_and_save_data")
    @patch("update_astock_daily._fetch_akshare_spot_df")
    @patch("update_astock_daily._save_price_rows", return_value=1)
    @patch("update_astock_daily._fetch_tushare_bulk_df")
    def test_bulk_update_uses_akshare_spot_for_missing_symbols(
        self, mock_bulk, mock_save, mock_spot, mock_symbol
    ):
        mock_bulk.return_value = pd.DataFrame(
            [
                {
                    "trade_date": "20260604",
                    "ts_code": "600519.SH",
                    "name": "Moutai",
                    "open_price": 10,
                    "high_price": 11,
                    "low_price": 9,
                    "close_price": 10.5,
                    "vol": 1,
                    "amount": 2,
                    "pct_chg": 3,
                }
            ]
        )
        mock_spot.return_value = pd.DataFrame(
            [
                {
                    "trade_date": "20260604",
                    "ts_code": "000001.SZ",
                    "name": "Ping An",
                    "open_price": 4,
                    "high_price": 4.1,
                    "low_price": 3.9,
                    "close_price": 4.0,
                    "vol": 1,
                    "amount": 2,
                    "pct_chg": 3,
                }
            ]
        )

        results, stats = upd.fetch_and_save_targets_bulk(
            ["600519.SH", "000001.SZ"], "20260604", "S", sleep_seconds=0
        )

        by_symbol = {r.ts_code: r for r in results}
        self.assertEqual(by_symbol["600519.SH"].source, "tushare_bulk")
        self.assertEqual(by_symbol["000001.SZ"].source, "akshare_spot")
        self.assertTrue(by_symbol["000001.SZ"].fallback_used)
        self.assertEqual(stats["akshare_spot_saved"], 1)
        mock_symbol.assert_not_called()

    @patch("update_astock_daily.fetch_and_save_data")
    @patch("update_astock_daily._fetch_akshare_spot_df")
    @patch("update_astock_daily._fetch_tushare_bulk_df")
    def test_bulk_update_falls_back_to_symbol_fetch_after_source_errors(
        self, mock_bulk, mock_spot, mock_symbol
    ):
        mock_bulk.side_effect = RuntimeError("bulk timeout")
        mock_spot.side_effect = RuntimeError("spot disconnected")
        mock_symbol.return_value = upd.FetchResult(
            "600519.SH",
            "S",
            source="",
            rows_saved=0,
            error="Tushare error: symbol timeout; AkShare error: history disconnected",
        )

        results, stats = upd.fetch_and_save_targets_bulk(["600519.SH"], "20260604", "S", sleep_seconds=0)

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].ok)
        self.assertTrue(results[0].fallback_used)
        self.assertIn("tushare_bulk: bulk timeout", results[0].error)
        self.assertEqual(stats["symbol_fetch_count"], 1)
        self.assertEqual(stats["final_missing"], 1)

    def test_build_alert_html_contains_recovery_commands(self):
        report = {
            "trade_date": "20260529",
            "is_abnormal": True,
            "issues": ["ETF coverage 0.0% below 90%"],
            "target_rows": {"total": 100, "stock": 100, "etf": 0},
            "coverage": {
                "stock": 0.9,
                "etf": 0.0,
                "stock_present": 90,
                "stock_target": 100,
                "etf_present": 0,
                "etf_target": 10,
            },
            "failures": [upd.FetchResult("510300.SH", "E", error="empty")],
            "fallback_used": [],
            "update_stats": [
                {
                    "asset_type": "S",
                    "target_count": 100,
                    "tushare_bulk_ok": True,
                    "tushare_bulk_rows": 5000,
                    "tushare_bulk_saved": 90,
                    "akshare_spot_used": True,
                    "akshare_spot_rows": 5000,
                    "akshare_spot_saved": 5,
                    "symbol_fetch_count": 5,
                    "symbol_fetch_saved": 0,
                    "final_missing": 5,
                }
            ],
            "ai_missing": [
                {
                    "portfolio_id": "official_cn_a_etf_v1",
                    "position_source_date": "20260528",
                    "missing_symbols": ["600519.SH"],
                }
            ],
        }

        html = upd.build_alert_html(report)

        self.assertIn("update_astock_daily.py", html)
        self.assertIn("rerun_ai_simulation.py --trade-date 20260529", html)
        self.assertIn("600519.SH", html)
        self.assertIn("tushare bulk ok", html)
        self.assertIn("5000", html)


if __name__ == "__main__":
    unittest.main()
