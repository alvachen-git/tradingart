import unittest

_IMPORT_ERROR = None
try:
    import pandas as pd
    import mobile_api
except Exception as exc:  # pragma: no cover
    pd = None
    mobile_api = None
    _IMPORT_ERROR = exc


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiIvChgFallback(unittest.TestCase):
    def test_compute_iv_change_from_recent_two_days(self):
        df = pd.DataFrame(
            [
                {"code": "ma2605", "td": "20260327", "iv": 70.5},
                {"code": "ma2605", "td": "20260326", "iv": 68.0},
                {"code": "ma2609", "td": "20260327", "iv": 45.2},
                {"code": "ma2609", "td": "20260326", "iv": 45.2},
                {"code": "px2605", "td": "20260327", "iv": 0.0},
                {"code": "px2605", "td": "20260326", "iv": 0.0},
            ]
        )
        out = mobile_api._compute_iv_chg_fallback_map(df)
        self.assertEqual(out.get("ma2605"), 2.5)
        self.assertEqual(out.get("ma2609"), 0.0)
        # 前一日 <= 0 时不生成回退值
        self.assertNotIn("px2605", out)


if __name__ == "__main__":
    unittest.main()
