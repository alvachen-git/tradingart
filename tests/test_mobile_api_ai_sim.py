import unittest
from unittest.mock import patch

_IMPORT_ERROR = None
try:
    import pandas as pd
    from fastapi import HTTPException
    import mobile_api
except Exception as exc:  # pragma: no cover
    pd = None
    HTTPException = Exception
    mobile_api = None
    _IMPORT_ERROR = exc


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiAiSim(unittest.TestCase):
    def test_overview_returns_empty_payload_when_snapshot_missing(self):
        with patch.object(mobile_api, "ai_get_latest_snapshot", return_value={"has_data": False}):
            out = mobile_api.intel_ai_overview(username="u1")

        self.assertFalse(out["has_data"])
        self.assertEqual(out["review_dates"], [])
        self.assertEqual(out["positions"], [])
        self.assertEqual(out["trades"], [])

    def test_overview_aggregates_and_clamps_params(self):
        nav_df = pd.DataFrame([
            {
                "trade_date": "20260326",
                "nav": 1_050_000,
                "bench_hs300": 1.02,
                "bench_zz1000": 1.01,
            },
            {
                "trade_date": "20260327",
                "nav": 1_080_000,
                "bench_hs300": 1.03,
                "bench_zz1000": 1.04,
            },
        ])
        pos_df = pd.DataFrame([
            {
                "trade_date": "20260327",
                "symbol": "600000",
                "name": "浦发银行",
                "weight": 0.2,
                "market_value": 200000,
            }
        ])
        trades_df = pd.DataFrame([
            {
                "trade_date": "20260327",
                "symbol": "600000",
                "side": "buy",
                "quantity": 100,
                "price": 10.2,
                "amount": 1020,
            }
        ])

        with patch.object(
            mobile_api,
            "ai_get_latest_snapshot",
            return_value={
                "has_data": True,
                "trade_date": "20260327",
                "initial_capital": 1_000_000,
                "nav": 1_080_000,
                "position_value": 200_000,
            },
        ), patch.object(
            mobile_api,
            "ai_get_review_dates",
            return_value=["20260327", "20260326"],
        ), patch.object(
            mobile_api,
            "ai_get_daily_review",
            return_value={
                "has_data": True,
                "trade_date": "20260327",
                "summary_md": "总结",
                "next_watchlist": [{"symbol": "600000", "score": 88}],
            },
        ), patch.object(
            mobile_api,
            "ai_get_nav_series",
            return_value=nav_df,
        ) as mocked_nav, patch.object(
            mobile_api,
            "ai_get_positions",
            return_value=pos_df,
        ), patch.object(
            mobile_api,
            "ai_get_trades",
            return_value=trades_df,
        ):
            out = mobile_api.intel_ai_overview(
                nav_days=999,
                trades_days=999,
                positions_limit=999,
                review_limit=999,
                username="u1",
            )

        self.assertTrue(out["has_data"])
        self.assertEqual(out["review_dates"][0], "20260327")
        self.assertAlmostEqual(out["nav_series"][0]["nav_norm"], 1.05, places=6)
        self.assertEqual(out["positions"][0]["symbol"], "600000")
        self.assertEqual(out["watchlist"][0]["symbol"], "600000")
        self.assertEqual(mocked_nav.call_args.kwargs["days"], 250)

    def test_overview_returns_no_positions_when_snapshot_position_value_zero(self):
        nav_df = pd.DataFrame(
            [
                {"trade_date": "20260327", "nav": 942700, "bench_hs300": 0.961, "bench_zz1000": 0.929}
            ]
        )
        with patch.object(
            mobile_api,
            "ai_get_latest_snapshot",
            return_value={
                "has_data": True,
                "trade_date": "20260327",
                "initial_capital": 1_000_000,
                "nav": 942700,
                "position_value": 0,
            },
        ), patch.object(
            mobile_api,
            "ai_get_review_dates",
            return_value=["20260327"],
        ), patch.object(
            mobile_api,
            "ai_get_daily_review",
            return_value={"has_data": True, "trade_date": "20260327", "summary_md": "总结", "next_watchlist": []},
        ), patch.object(
            mobile_api,
            "ai_get_nav_series",
            return_value=nav_df,
        ), patch.object(
            mobile_api,
            "ai_get_positions",
        ) as mocked_positions, patch.object(
            mobile_api,
            "ai_get_trades",
            return_value=pd.DataFrame([]),
        ):
            out = mobile_api.intel_ai_overview(username="u1")

        self.assertEqual(out["positions"], [])
        mocked_positions.assert_not_called()

    def test_review_normalizes_date(self):
        with patch.object(
            mobile_api,
            "ai_get_daily_review",
            return_value={"has_data": True, "trade_date": "20260327"},
        ) as mocked_review:
            out = mobile_api.intel_ai_review(trade_date="2026-03-27", username="u1")

        self.assertTrue(out["has_data"])
        self.assertEqual(mocked_review.call_args.kwargs["trade_date"], "20260327")

    def test_review_rejects_invalid_date(self):
        with self.assertRaises(HTTPException) as cm:
            mobile_api.intel_ai_review(trade_date="202603", username="u1")
        self.assertEqual(cm.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
