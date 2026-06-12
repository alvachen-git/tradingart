import unittest
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

    def test_normalize_symbol_for_qfq_script(self):
        self.assertEqual(qfq.normalize_symbol("002837"), "002837.SZ")
        self.assertEqual(qfq.normalize_symbol("600519"), "600519.SH")

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
