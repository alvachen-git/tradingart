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


if __name__ == "__main__":
    unittest.main()
