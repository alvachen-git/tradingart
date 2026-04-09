import unittest
from unittest.mock import patch

_IMPORT_ERROR = None
try:
    import mobile_api
except Exception as exc:  # pragma: no cover
    mobile_api = None
    _IMPORT_ERROR = exc


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiMarketChaos(unittest.TestCase):
    def test_market_chaos_returns_normalized_payload_with_truncation(self):
        monitored_markets = [
            {
                "display_title": f"事件{i}",
                "event_slug": f"e{i}",
                "region_tag": "middle_east" if i % 2 == 0 else "global",
                "pair_tag": f"P{i}",
                "probability": 0.1 * i,
                "delta_24h": 0.01 * i,
                "event_raw": 0.2 * i,
            }
            for i in range(1, 15)
        ]
        top_markets = [
            {
                "display_title": f"推升{i}",
                "region_tag": "east_asia",
                "pair_tag": f"T{i}",
                "probability": 0.2 * i,
                "delta_24h": 0.02 * i,
                "event_raw": 0.3 * i,
            }
            for i in range(1, 8)
        ]
        snapshot = {
            "score_raw": "53.2",
            "score_display": 52.8,
            "band": "something_is_brewing",
            "updated_at": "2026-04-09T11:22:33+08:00",
            "methodology_version": "wci_v1",
            "top_markets": top_markets,
            "category_breakdown": [
                {"key": "military_conflict", "baseline": "11.2", "escalation": 9.8},
                {"key": "economic_crisis", "label": "经济风险", "baseline": 4.0, "escalation": 6.0},
            ],
            "source_status": {
                "score_components": {
                    "ongoing_baseline": "21.5",
                    "escalation_pressure": 24.0,
                    "contagion_bonus": 7.7,
                },
                "monitored_markets": monitored_markets,
            },
        }

        recent_snapshots = [
            {
                "source_status": {
                    "monitored_markets": [
                        {
                            "event_slug": "e1",
                            "display_title": "事件1",
                            "probability": 0.10,
                        },
                        {
                            "event_slug": "e2",
                            "display_title": "事件2",
                            "probability": 0.20,
                        },
                    ]
                }
            },
            {
                "source_status": {
                    "monitored_markets": [
                        {
                            "event_slug": "e1",
                            "display_title": "事件1",
                            "probability": 0.14,
                        },
                        {
                            "event_slug": "e2",
                            "display_title": "事件2",
                            "probability": 0.15,
                        },
                    ]
                }
            },
            {
                "source_status": {
                    "monitored_markets": [
                        {
                            "event_slug": "e1",
                            "display_title": "事件1",
                            "probability": 0.20,
                        },
                        {
                            "event_slug": "e2",
                            "display_title": "事件2",
                            "probability": 0.09,
                        },
                    ]
                }
            },
        ]

        with (
            patch.object(mobile_api.de, "get_latest_geopolitical_risk_snapshot", return_value=snapshot),
            patch.object(mobile_api.de, "get_recent_geopolitical_risk_snapshots", return_value=recent_snapshots),
        ):
            out = mobile_api.market_chaos(username="tester")

        self.assertTrue(out["has_data"])
        self.assertEqual(out["band"], "something_is_brewing")
        self.assertEqual(out["band_label"], "全球失序")
        self.assertEqual(out["updated_time_text"], "11:22:33")
        self.assertEqual(out["components"]["ongoing_baseline"], 21.5)
        self.assertEqual(len(out["monitored_markets"]), 12)
        self.assertEqual(out["monitored_markets"][0]["rank"], 1)
        self.assertEqual(out["monitored_markets"][0]["region_label"], "全球")
        self.assertEqual(out["monitored_markets"][1]["region_label"], "中东")
        self.assertEqual(out["monitored_markets"][0]["trend_arrows"], "▲▲")
        self.assertEqual(out["monitored_markets"][0]["trend_direction"], "up")
        self.assertEqual(out["monitored_markets"][0]["trend_flames"], "🔥")
        self.assertEqual(out["monitored_markets"][1]["trend_arrows"], "▼▼")
        self.assertEqual(out["monitored_markets"][1]["trend_direction"], "down")
        self.assertEqual(len(out["top_drivers"]), 5)
        self.assertEqual(out["category_breakdown"][0]["label"], "军事冲突")
        self.assertEqual(out["category_breakdown"][1]["label"], "经济风险")
        self.assertAlmostEqual(out["category_breakdown"][0]["total"], 21.0, places=6)

    def test_market_chaos_returns_empty_payload_when_no_snapshot(self):
        with (
            patch.object(mobile_api.de, "get_latest_geopolitical_risk_snapshot", return_value={}),
            patch.object(mobile_api.de, "get_recent_geopolitical_risk_snapshots", return_value=[]),
        ):
            out = mobile_api.market_chaos(username="tester")

        self.assertFalse(out["has_data"])
        self.assertEqual(out["score_raw"], 0.0)
        self.assertEqual(out["band_label"], "局势偏稳")
        self.assertEqual(out["monitored_markets"], [])
        self.assertEqual(out["top_drivers"], [])
        self.assertEqual(out["category_breakdown"], [])

    def test_market_chaos_fallbacks_when_backend_raises(self):
        with patch.object(mobile_api.de, "get_latest_geopolitical_risk_snapshot", side_effect=RuntimeError("db down")):
            out = mobile_api.market_chaos(username="tester")

        self.assertFalse(out["has_data"])
        self.assertEqual(out["updated_time_text"], "")


if __name__ == "__main__":
    unittest.main()
