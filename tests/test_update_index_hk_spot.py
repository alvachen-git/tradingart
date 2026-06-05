import os
import unittest
from datetime import datetime

import pandas as pd

import update_index


class UpdateIndexHkSpotTests(unittest.TestCase):
    def test_match_hk_spot_index_row_accepts_plain_codes(self):
        df = pd.DataFrame(
            [
                {"代码": "HSI", "名称": "恒生指数", "最新价": 24961.951},
                {"代码": "HSTECH", "名称": "恒生科技指数", "最新价": 4888.39},
            ]
        )

        hsi = update_index._match_hk_spot_index_row(df, "HSI")
        hstech = update_index._match_hk_spot_index_row(df, "HSTECH")

        self.assertIsNotNone(hsi)
        self.assertEqual(float(hsi["最新价"]), 24961.951)
        self.assertIsNotNone(hstech)
        self.assertEqual(float(hstech["最新价"]), 4888.39)

    def test_match_hk_spot_index_row_accepts_hk_prefixed_codes(self):
        df = pd.DataFrame(
            [
                {"代码": "hkHSI", "名称": "恒生指数", "最新价": 25123.45},
                {"代码": "hkHSTECH", "名称": "恒生科技指数", "最新价": 4901.23},
            ]
        )

        hsi = update_index._match_hk_spot_index_row(df, "HSI")
        hstech = update_index._match_hk_spot_index_row(df, "HSTECH")

        self.assertIsNotNone(hsi)
        self.assertEqual(float(hsi["最新价"]), 25123.45)
        self.assertIsNotNone(hstech)
        self.assertEqual(float(hstech["最新价"]), 4901.23)

    def test_clear_proxy_env_for_market_fetch_removes_proxy_keys(self):
        env_keys = update_index._PROXY_ENV_KEYS + update_index._NO_PROXY_ENV_KEYS
        original = {key: os.environ.get(key) for key in env_keys}
        try:
            for key in update_index._PROXY_ENV_KEYS:
                os.environ[key] = "http://127.0.0.1:7890"
            for key in update_index._NO_PROXY_ENV_KEYS:
                os.environ.pop(key, None)

            cleared = update_index._clear_proxy_env_for_market_fetch()

            self.assertEqual(set(cleared), set(env_keys))
            for key in update_index._PROXY_ENV_KEYS:
                self.assertNotIn(key, os.environ)
            for key in update_index._NO_PROXY_ENV_KEYS:
                self.assertEqual(os.environ.get(key), "*")
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_should_send_hk_update_alert_only_after_close_for_today(self):
        before_close = datetime(2026, 6, 5, 16, 19)
        after_close = datetime(2026, 6, 5, 16, 20)

        self.assertFalse(update_index._should_send_hk_update_alert("20260605", "20260605", before_close))
        self.assertTrue(update_index._should_send_hk_update_alert("20260605", "20260605", after_close))
        self.assertFalse(update_index._should_send_hk_update_alert("20260604", "20260605", after_close))
        self.assertFalse(update_index._should_send_hk_update_alert("20260604", "20260604", after_close))

    def test_send_hk_update_failure_alert_uses_default_recipient(self):
        sent = []

        def fake_sender(to_email, subject, html):
            sent.append((to_email, subject, html))
            return True

        failures = [
            {
                "ts_code": "HSI",
                "name": "恒生指数",
                "reason": "未写入目标日期",
                "latest_date": "20260529",
                "source": "tushare",
                "source_errors": ["spot_today: empty"],
            }
        ]

        ok = update_index._send_hk_update_failure_alert(
            failures,
            "20260605",
            "20260605",
            email_sender=fake_sender,
        )

        self.assertTrue(ok)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][0], update_index.DEFAULT_INDEX_UPDATE_ALERT_EMAIL)
        self.assertIn("港股指数更新失败", sent[0][1])
        self.assertIn("HSI", sent[0][2])
        self.assertIn("spot_today: empty", sent[0][2])

    def test_send_hk_update_failure_alert_swallows_sender_exception(self):
        def broken_sender(to_email, subject, html):
            raise RuntimeError("smtp down")

        failures = [
            {
                "ts_code": "HSTECH",
                "name": "恒生科技指数",
                "reason": "所有数据源均不可用",
                "latest_date": None,
                "source": None,
                "source_errors": [],
            }
        ]

        ok = update_index._send_hk_update_failure_alert(
            failures,
            "20260605",
            "20260605",
            email_sender=broken_sender,
        )

        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
