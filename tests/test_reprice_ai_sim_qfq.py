import unittest
import os

import pandas as pd

os.environ.setdefault("DB_HOST", "127.0.0.1")
os.environ.setdefault("DB_PORT", "3306")
os.environ.setdefault("DB_USER", "test")
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("DB_NAME", "test")

import reprice_ai_sim_qfq as reprice


class TestRepriceAiSimQfq(unittest.TestCase):
    def test_compute_reprice_preserves_trade_identity_and_uses_qfq_prices(self):
        nav_df = pd.DataFrame(
            [
                {"portfolio_id": "official_cn_a_etf_v1", "trade_date": "20260610", "bench_hs300": 1.01, "bench_zz1000": 1.02},
                {"portfolio_id": "official_cn_a_etf_v1", "trade_date": "20260611", "bench_hs300": 1.00, "bench_zz1000": 1.01},
            ]
        )
        trades_df = pd.DataFrame(
            [
                {
                    "id": 7,
                    "trade_id": "keep-me",
                    "portfolio_id": "official_cn_a_etf_v1",
                    "trade_date": "20260610",
                    "created_at": "2026-06-10 20:31:00",
                    "symbol": "300502.SZ",
                    "side": "buy",
                    "quantity": 100,
                    "price": 772.5,
                    "amount": 77_250,
                    "realized_pnl": 0,
                }
            ]
        )
        qfq_df = pd.DataFrame(
            [
                {"trade_date": "20260610", "ts_code": "300502.SZ", "name": "新易盛", "close_price": 551.0693},
                {"trade_date": "20260611", "ts_code": "300502.SZ", "name": "新易盛", "close_price": 526.0},
            ]
        )

        out = reprice.compute_reprice("official_cn_a_etf_v1", nav_df, trades_df, qfq_df)

        self.assertTrue(out.ok)
        self.assertEqual(out.trade_updates[0]["id"], 7)
        self.assertAlmostEqual(out.trade_updates[0]["price"], 551.0693, places=4)
        self.assertAlmostEqual(out.trade_updates[0]["amount"], 55_106.93, places=2)
        latest_pos = [r for r in out.position_rows if r["trade_date"] == "20260611"][0]
        self.assertAlmostEqual(latest_pos["avg_cost"], 551.0693, places=4)
        self.assertAlmostEqual(latest_pos["close_price"], 526.0, places=4)
        self.assertAlmostEqual(latest_pos["unrealized_pnl"], -2506.93, places=2)
        self.assertGreater(out.nav_updates[-1]["nav"], 997_000)

    def test_compute_reprice_reports_missing_qfq_without_fallback(self):
        nav_df = pd.DataFrame(
            [{"portfolio_id": "official_cn_a_etf_v1", "trade_date": "20260611", "bench_hs300": 1.0, "bench_zz1000": 1.0}]
        )
        trades_df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "portfolio_id": "official_cn_a_etf_v1",
                    "trade_date": "20260611",
                    "created_at": "2026-06-11 20:31:00",
                    "symbol": "300502.SZ",
                    "side": "buy",
                    "quantity": 100,
                    "price": 526.0,
                    "amount": 52_600,
                    "realized_pnl": 0,
                }
            ]
        )
        qfq_df = pd.DataFrame(columns=["trade_date", "ts_code", "name", "close_price"])

        out = reprice.compute_reprice("official_cn_a_etf_v1", nav_df, trades_df, qfq_df)

        self.assertFalse(out.ok)
        self.assertEqual([(x.trade_date, x.symbol, x.usage) for x in out.missing_qfq], [("20260611", "300502.SZ", "trade")])
        self.assertEqual(out.trade_updates, [])

    def test_compute_reprice_uses_previous_qfq_close_for_stale_trade_date(self):
        nav_df = pd.DataFrame(
            [
                {"portfolio_id": "official_cn_a_etf_v1", "trade_date": "20260403", "bench_hs300": 1.0, "bench_zz1000": 1.0},
                {"portfolio_id": "official_cn_a_etf_v1", "trade_date": "20260406", "bench_hs300": 1.0, "bench_zz1000": 1.0},
            ]
        )
        trades_df = pd.DataFrame(
            [
                {
                    "id": 10,
                    "portfolio_id": "official_cn_a_etf_v1",
                    "trade_date": "20260403",
                    "created_at": "2026-04-03 20:31:00",
                    "symbol": "601988.SH",
                    "side": "buy",
                    "quantity": 100,
                    "price": 5.82,
                    "amount": 582,
                    "realized_pnl": 0,
                },
                {
                    "id": 11,
                    "portfolio_id": "official_cn_a_etf_v1",
                    "trade_date": "20260406",
                    "created_at": "2026-04-06 20:31:00",
                    "symbol": "601988.SH",
                    "side": "sell",
                    "quantity": 100,
                    "price": 5.82,
                    "amount": 582,
                    "realized_pnl": 0,
                },
            ]
        )
        qfq_df = pd.DataFrame(
            [{"trade_date": "20260403", "ts_code": "601988.SH", "name": "中国银行", "close_price": 5.82}]
        )

        out = reprice.compute_reprice("official_cn_a_etf_v1", nav_df, trades_df, qfq_df)

        self.assertTrue(out.ok)
        self.assertEqual(len(out.trade_updates), 2)
        self.assertAlmostEqual(out.trade_updates[-1]["price"], 5.82)

    def test_parse_portfolio_ids_defaults_to_v1_v2(self):
        self.assertEqual(list(reprice.SUPPORTED_PORTFOLIOS), reprice.parse_portfolio_ids(""))
        self.assertEqual(["official_cn_a_etf_v1", "official_cn_a_etf_v2"], reprice.parse_portfolio_ids("official_cn_a_etf_v1 official_cn_a_etf_v2"))


if __name__ == "__main__":
    unittest.main()
