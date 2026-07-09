import unittest
from unittest.mock import patch

import pandas as pd

_IMPORT_ERROR = None
try:
    from fastapi import HTTPException
    import mobile_api
except Exception as exc:  # pragma: no cover
    HTTPException = Exception
    mobile_api = None
    _IMPORT_ERROR = exc


def _metrics_history():
    return pd.DataFrame(
        [
            {"trade_date": "20260401", "atm_iv_pct": 18.0},
            {"trade_date": "20260402", "atm_iv_pct": 19.5},
        ]
    )


def _stock_history():
    return pd.DataFrame(
        [
            {"date": "2026-04-01", "symbol": "SPY", "open": 510.0, "high": 515.0, "low": 508.0, "close": 512.3, "volume": 1000, "adjClose": 512.3},
            {"date": "2026-04-02", "symbol": "SPY", "open": 513.0, "high": 518.0, "low": 511.0, "close": 516.0, "volume": 1200, "adjClose": 516.0},
        ]
    )


def _profile_card():
    return {
        "symbol": "SPY",
        "name": "标普500ETF",
        "asset_type": "etf",
        "business": "追踪标普500指数。",
        "strength": "行业分散，流动性深。",
        "risk": "受大型科技权重影响。",
        "recent_hotspot": "近期关注利率变化和大型科技轮动。",
        "option_data": "IV处于偏低区，OI数据完整。",
        "dynamic_source_refs": [{"source": "local"}],
    }


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiUsOptions(unittest.TestCase):
    def test_products_returns_default_spy_and_data_flags(self):
        def latest(symbol, **_kwargs):
            return "20260402" if symbol == "SPY" else ""

        with patch.object(mobile_api, "US_OPTION_DEFAULT_UNDERLYINGS", ("SPY", "NVDA")), patch.object(
            mobile_api, "us_load_latest_option_trade_date", side_effect=latest
        ):
            out = mobile_api.us_options_products(username="u1")

        self.assertEqual(out["default_symbol"], "SPY")
        self.assertEqual([item["symbol"] for item in out["items"]], ["SPY", "NVDA"])
        self.assertTrue(out["items"][0]["has_data"])
        self.assertFalse(out["items"][1]["has_data"])
        self.assertEqual(out["items"][0]["name"], "标普500ETF")

    def test_overview_returns_mobile_payload_with_data(self):
        with (
            patch.object(mobile_api, "us_load_latest_option_trade_date", return_value="20260402"),
            patch.object(mobile_api, "us_load_stock_daily", return_value=_stock_history()),
            patch.object(mobile_api, "us_selected_underlying_price", return_value=512.3),
            patch.object(mobile_api, "us_load_market_metrics_history", return_value=_metrics_history()),
            patch.object(mobile_api, "us_build_underlying_profile_card", return_value=_profile_card()),
            patch.object(
                mobile_api,
                "us_load_option_chain_summary",
                return_value={
                    "rows": 120,
                    "monthly": 60,
                    "short_cycle": 60,
                    "provider_iv_rows": 80,
                    "computed_iv_rows": 30,
                    "open_interest_rows": 110,
                },
            ),
            patch.object(
                mobile_api,
                "us_calculate_overview_metrics_from_market_history",
                return_value={
                    "atm_iv_pct": 19.5,
                    "iv_rank": 18.0,
                    "iv_percentile": 24.0,
                    "rv20_pct": 16.2,
                    "iv_rv20_spread": 3.3,
                    "put_call_oi": 1.1,
                },
            ),
        ):
            out = mobile_api.us_options_overview(symbol="spy", username="u1")

        self.assertTrue(out["has_data"])
        self.assertEqual(out["symbol"], "SPY")
        self.assertEqual(out["display_name"], "标普500ETF")
        self.assertEqual(out["trade_date"], "20260402")
        self.assertEqual(out["underlying_price"], 512.3)
        self.assertEqual(out["metrics"]["atm_iv_pct"], 19.5)
        self.assertEqual(out["profile_card"]["style_label"], "板块风格")
        self.assertIn("大型科技", out["profile_card"]["recent_hotspot"])
        self.assertEqual(len(out["price_history"]), 2)
        self.assertEqual(out["price_history"][0]["close"], 512.3)
        self.assertEqual(len(out["iv_history"]), 2)
        self.assertIn("IV 处于历史偏低区", out["status_brief"])

    def test_overview_no_data_returns_false_without_not_found(self):
        with patch.object(mobile_api, "us_load_latest_option_trade_date", return_value=""):
            out = mobile_api.us_options_overview(symbol="SPY", username="u1")

        self.assertFalse(out["has_data"])
        self.assertEqual(out["symbol"], "SPY")
        self.assertIn("暂无", out["message"])
        self.assertEqual(out["price_history"], [])
        self.assertEqual(out["iv_history"], [])

    def test_surface_returns_all_blocks(self):
        cone = pd.DataFrame([{"dte_target": 30, "p50": 20.0, "sample_count": 42}])
        line = pd.DataFrame([{"dte_target": 30, "dte": 31, "expiration_date": "2026-05-15", "iv_pct": 19.8}])
        curve = pd.DataFrame([{"moneyness_pct": -2.0, "iv_pct": 21.0, "call_put": "P", "dte": 31}])
        with (
            patch.object(mobile_api, "us_load_latest_option_trade_date", return_value="20260402"),
            patch.object(mobile_api, "us_load_stock_daily", return_value=pd.DataFrame()),
            patch.object(mobile_api, "us_selected_underlying_price", return_value=512.3),
            patch.object(mobile_api, "us_load_available_option_trade_dates", return_value=["20260401", "20260402"]),
            patch.object(mobile_api, "us_load_volatility_cone_history", return_value=cone),
            patch.object(mobile_api, "us_load_volatility_cone_line_snapshot", return_value=line),
            patch.object(mobile_api, "us_load_otm_volatility_curve_snapshot", return_value=curve),
        ):
            out = mobile_api.us_options_surface(symbol="SPY", username="u1")

        self.assertTrue(out["has_data"])
        self.assertEqual(out["previous_trade_date"], "20260401")
        self.assertEqual(out["volatility_cone"][0]["p50"], 20.0)
        self.assertEqual(out["today_cone_line"][0]["iv_pct"], 19.8)
        self.assertEqual(out["today_otm_curve"][0]["call_put"], "P")

    def test_defense_returns_latest_and_history(self):
        history = pd.DataFrame(
            [
                {"trade_date": "20260401", "call_strike": 520.0, "put_strike": 500.0},
                {"trade_date": "20260402", "call_strike": 525.0, "put_strike": 505.0},
            ]
        )
        with (
            patch.object(mobile_api, "us_load_latest_option_trade_date", return_value="20260402"),
            patch.object(mobile_api, "us_load_oi_defense_history", return_value=history),
        ):
            out = mobile_api.us_options_defense(symbol="SPY", username="u1")

        self.assertTrue(out["has_data"])
        self.assertEqual(len(out["history"]), 2)
        self.assertEqual(out["latest"]["call_strike"], 525.0)

    def test_anomalies_clamps_limit_and_supports_all_pool(self):
        scan = pd.DataFrame(
            [
                {"trade_date": "20260402", "underlying": "SPY", "option_ticker": f"O:{idx}", "anomaly_score": idx}
                for idx in range(60)
            ]
        )
        with patch.object(mobile_api, "us_load_option_anomaly_scan", return_value=scan) as loader:
            out = mobile_api.us_options_anomalies(symbol=None, limit=99, username="u1")

        self.assertTrue(out["has_data"])
        self.assertEqual(out["limit"], 50)
        self.assertEqual(len(out["items"]), 50)
        self.assertEqual(out["display_name"], "全部观察池")
        self.assertIsNone(loader.call_args.kwargs["underlyings"])

    def test_invalid_symbol_rejects_request(self):
        with self.assertRaises(HTTPException) as cm:
            mobile_api.us_options_overview(symbol="510050.SH", username="u1")

        self.assertEqual(cm.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
