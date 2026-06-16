import unittest
import os
from unittest.mock import patch

import pandas as pd

import update_stock_price_qfq as qfq


class TestStockPriceQfq(unittest.TestCase):
    def test_build_qfq_from_raw_and_factor_adjusts_ex_rights_gap(self):
        raw = pd.DataFrame(
            [
                {
                    "trade_date": "20260514",
                    "ts_code": "002837.SZ",
                    "name": "英维克",
                    "open_price": 83.64,
                    "high_price": 85.48,
                    "low_price": 78.48,
                    "close_price": 102.20,
                    "pct_chg": -3.94,
                    "vol": 540_500,
                    "amount": 570_700,
                },
                {
                    "trade_date": "20260601",
                    "ts_code": "002837.SZ",
                    "name": "英维克",
                    "open_price": 67.50,
                    "high_price": 70.92,
                    "low_price": 67.28,
                    "close_price": 66.06,
                    "pct_chg": -27.41,
                    "vol": 652_900,
                    "amount": 449_500,
                },
            ]
        )
        factors = pd.DataFrame(
            [
                {"trade_date": "20260514", "ts_code": "002837.SZ", "adj_factor": 78.48 / 102.20},
                {"trade_date": "20260601", "ts_code": "002837.SZ", "adj_factor": 1.0},
            ]
        )

        out = qfq._build_qfq_from_raw_and_factor(raw, factors, "20260601")
        by_date = {row["trade_date"]: row for _, row in out.iterrows()}

        self.assertAlmostEqual(float(by_date["20260514"]["close_price"]), 78.48, places=2)
        self.assertAlmostEqual(float(by_date["20260601"]["close_price"]), 66.06, places=2)
        self.assertEqual(by_date["20260514"]["source"], "tushare_adj_factor")
        self.assertEqual(by_date["20260514"]["anchor_trade_date"], "20260601")

    def test_build_qfq_forward_fills_recent_factor_gap_for_etf(self):
        raw = pd.DataFrame(
            [
                {
                    "trade_date": "20260612",
                    "ts_code": "510300.SH",
                    "name": "300ETF",
                    "open_price": 4.80,
                    "high_price": 4.85,
                    "low_price": 4.78,
                    "close_price": 4.82,
                    "pct_chg": 1.0,
                    "vol": 1,
                    "amount": 2,
                },
                {
                    "trade_date": "20260615",
                    "ts_code": "510300.SH",
                    "name": "300ETF",
                    "open_price": 4.86,
                    "high_price": 4.90,
                    "low_price": 4.84,
                    "close_price": 4.89,
                    "pct_chg": 1.5,
                    "vol": 1,
                    "amount": 2,
                },
            ]
        )
        factors = pd.DataFrame(
            [{"trade_date": "20260612", "ts_code": "510300.SH", "adj_factor": 1.267}]
        )

        out = qfq._build_qfq_from_raw_and_factor(
            raw,
            factors,
            "20260615",
            source="tushare_fund_adj",
            fill_factor_gaps=True,
            max_factor_gap_days=7,
        )

        self.assertEqual(out["trade_date"].tolist(), ["20260612", "20260615"])
        self.assertEqual(out.iloc[-1]["anchor_trade_date"], "20260615")
        self.assertAlmostEqual(float(out.iloc[-1]["close_price"]), 4.89, places=2)

    def test_build_qfq_does_not_forward_fill_old_factor_gap(self):
        raw = pd.DataFrame(
            [
                {
                    "trade_date": "20260612",
                    "ts_code": "510300.SH",
                    "name": "300ETF",
                    "open_price": 4.80,
                    "high_price": 4.85,
                    "low_price": 4.78,
                    "close_price": 4.82,
                    "pct_chg": 1.0,
                    "vol": 1,
                    "amount": 2,
                },
                {
                    "trade_date": "20260630",
                    "ts_code": "510300.SH",
                    "name": "300ETF",
                    "open_price": 5.00,
                    "high_price": 5.10,
                    "low_price": 4.95,
                    "close_price": 5.05,
                    "pct_chg": 1.5,
                    "vol": 1,
                    "amount": 2,
                },
            ]
        )
        factors = pd.DataFrame(
            [{"trade_date": "20260612", "ts_code": "510300.SH", "adj_factor": 1.267}]
        )

        out = qfq._build_qfq_from_raw_and_factor(
            raw,
            factors,
            "20260630",
            source="tushare_fund_adj",
            fill_factor_gaps=True,
            max_factor_gap_days=7,
        )

        self.assertEqual(out["trade_date"].tolist(), ["20260612"])

    def test_normalize_symbol_for_qfq_script(self):
        self.assertEqual(qfq.normalize_symbol("002837"), "002837.SZ")
        self.assertEqual(qfq.normalize_symbol("600519"), "600519.SH")

    def test_etf_symbol_is_not_treated_as_stock_for_qfq(self):
        self.assertTrue(qfq.is_etf_symbol("510300.SH"))
        self.assertTrue(qfq.is_etf_symbol("159915.SZ"))
        self.assertTrue(qfq.is_etf_symbol("588000.SH"))
        self.assertFalse(qfq.is_a_share_symbol("510300.SH"))
        self.assertTrue(qfq.is_a_share_symbol("600519.SH"))

    def test_asset_scope_filter_keeps_stock_and_etf_only(self):
        symbols = ["600519.SH", "510300.SH", "AAPL.US"]

        out = [s for s in symbols if qfq._asset_scope_allows(s, "stock_etf")]

        self.assertEqual(out, ["600519.SH", "510300.SH"])

    def test_without_proxy_env_temporarily_clears_and_restores_proxy(self):
        old_http = os.environ.get("HTTP_PROXY")
        old_all = os.environ.get("ALL_PROXY")
        os.environ["HTTP_PROXY"] = "http://127.0.0.1:9999"
        os.environ["ALL_PROXY"] = "socks5://127.0.0.1:9999"
        try:
            with qfq._without_proxy_env():
                self.assertNotIn("HTTP_PROXY", os.environ)
                self.assertNotIn("ALL_PROXY", os.environ)
            self.assertEqual(os.environ.get("HTTP_PROXY"), "http://127.0.0.1:9999")
            self.assertEqual(os.environ.get("ALL_PROXY"), "socks5://127.0.0.1:9999")
        finally:
            if old_http is None:
                os.environ.pop("HTTP_PROXY", None)
            else:
                os.environ["HTTP_PROXY"] = old_http
            if old_all is None:
                os.environ.pop("ALL_PROXY", None)
            else:
                os.environ["ALL_PROXY"] = old_all

    @patch("update_stock_price_qfq.save_qfq_df", return_value=1)
    @patch("update_stock_price_qfq._fetch_fund_adj_factor_tushare")
    @patch("update_stock_price_qfq._read_raw_price")
    def test_update_symbol_qfq_uses_tushare_fund_adj_for_etf(self, mock_raw, mock_fund_adj, mock_save):
        mock_raw.return_value = pd.DataFrame(
            [
                {
                    "trade_date": "20260615",
                    "ts_code": "510300.SH",
                    "name": "300ETF",
                    "open_price": 4.85,
                    "high_price": 4.90,
                    "low_price": 4.83,
                    "close_price": 4.89,
                    "pct_chg": 1.52,
                    "vol": 1,
                    "amount": 2,
                }
            ]
        )
        mock_fund_adj.return_value = pd.DataFrame(
            [
                {"trade_date": "20260615", "ts_code": "510300.SH", "adj_factor": 1.267},
            ]
        )

        result = qfq.update_symbol_qfq(object(), object(), "510300.SH", "20260615", "20260615", dry_run=True)

        self.assertTrue(result.ok)
        self.assertEqual(result.source, "tushare_fund_adj")
        mock_fund_adj.assert_called_once()
        mock_save.assert_called_once()

    @patch("update_stock_price_qfq._read_range_stats")
    def test_filter_symbols_by_qfq_gap_uses_bounds_and_row_count(self, mock_stats):
        def fake_stats(_engine, table_name, _symbols, _start, _end):
            if table_name == "stock_price":
                return {
                    "600519.SH": {"min_date": "20260101", "max_date": "20260105", "row_count": 3},
                    "000001.SZ": {"min_date": "20260101", "max_date": "20260105", "row_count": 3},
                    "510300.SH": {"min_date": "20260101", "max_date": "20260105", "row_count": 3},
                }
            return {
                "600519.SH": {"min_date": "20260101", "max_date": "20260105", "row_count": 3},
                "000001.SZ": {"min_date": "20260102", "max_date": "20260105", "row_count": 2},
            }

        mock_stats.side_effect = fake_stats

        out = qfq.filter_symbols_by_qfq_gap(object(), ["600519.SH", "000001.SZ", "510300.SH"], "20260101", "20260105")

        self.assertEqual(out, ["000001.SZ", "510300.SH"])

    def test_resolve_symbols_includes_v3_daily_candidates(self):
        class Args:
            symbols = "002837.SZ"
            portfolio_id = "official_cn_a_etf_v3"
            start_date = "20260501"
            end_date = "20260602"
            portfolio_symbol_scope = "all"
            all_stock_price_symbols = False
            v3_daily_candidates = True
            candidate_date = "20260602"
            candidate_limit = 3

        with (
            patch("update_stock_price_qfq._read_symbols_for_range", return_value=["300502.SZ"]),
            patch("update_stock_price_qfq._read_v3_daily_candidate_symbols", return_value=["600522.SH", "002837.SZ"]) as mock_candidates,
        ):
            out = qfq.resolve_symbols(object(), Args())

        self.assertEqual(out, ["002837.SZ", "300502.SZ", "600522.SH"])
        mock_candidates.assert_called_once()

    def test_resolve_symbols_can_exclude_watchlist_for_portfolio(self):
        class Args:
            symbols = ""
            portfolio_id = "official_cn_a_etf_v2"
            start_date = "20260501"
            end_date = "20260602"
            portfolio_symbol_scope = "trades_positions"
            all_stock_price_symbols = False
            v3_daily_candidates = False
            candidate_date = ""
            candidate_limit = 3

        with patch("update_stock_price_qfq._read_symbols_for_range", return_value=["300502.SZ"]) as mock_read:
            out = qfq.resolve_symbols(object(), Args())

        self.assertEqual(out, ["300502.SZ"])
        self.assertFalse(mock_read.call_args.kwargs["include_watchlist"])


if __name__ == "__main__":
    unittest.main()
