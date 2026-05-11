import sys
import types
import unittest

from sqlalchemy import create_engine, text

if "data_engine" not in sys.modules:
    fake_data_engine = types.ModuleType("data_engine")
    fake_data_engine.engine = None
    sys.modules["data_engine"] = fake_data_engine

import subscription_service as sub


class TestSubscriptionTrial(unittest.TestCase):
    def setUp(self):
        self.orig_engine = sub.engine
        self.orig_has_cols = sub._HAS_SUB_SOURCE_COLUMNS
        self.orig_trial_ready = sub._TRIAL_TABLE_READY

        self.engine = create_engine("sqlite:///:memory:", future=True)
        sub.engine = self.engine
        sub._HAS_SUB_SOURCE_COLUMNS = None
        sub._TRIAL_TABLE_READY = False

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE content_channels (
                        id INTEGER PRIMARY KEY,
                        code TEXT,
                        name TEXT,
                        is_active INTEGER DEFAULT 1,
                        is_premium INTEGER DEFAULT 1
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE user_subscriptions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL,
                        channel_id INTEGER NOT NULL,
                        is_active INTEGER NOT NULL DEFAULT 1,
                        expire_at DATETIME,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        source_type TEXT,
                        source_ref TEXT,
                        source_note TEXT,
                        granted_at DATETIME,
                        operator TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO content_channels(id, code, name, is_active, is_premium) VALUES
                    (1, 'daily_report', '复盘晚报', 1, 1),
                    (2, 'expiry_option_radar', '末日期权晚报', 1, 1),
                    (3, 'broker_position_report', '持仓密报', 1, 1),
                    (4, 'fund_flow_report', '资金流晚报', 1, 1),
                    (5, 'macro_risk_radar', '宏观周报', 1, 1),
                    (6, 'safe_stock_report', '小爱选股晚报', 1, 1)
                    """
                )
            )

    def tearDown(self):
        sub.engine = self.orig_engine
        sub._HAS_SUB_SOURCE_COLUMNS = self.orig_has_cols
        sub._TRIAL_TABLE_READY = self.orig_trial_ready

    def test_grant_new_user_trial_idempotent(self):
        ok1, msg1 = sub.grant_new_user_trial("new_user_1")
        ok2, msg2 = sub.grant_new_user_trial("new_user_1")

        self.assertTrue(ok1)
        self.assertIn(msg1, {"trial_granted", "already_granted"})
        self.assertTrue(ok2)
        self.assertEqual(msg2, "already_granted")

        with self.engine.connect() as conn:
            cnt_trial = conn.execute(
                text("SELECT COUNT(*) FROM user_trial_grants WHERE user_id='new_user_1'")
            ).scalar_one()
            cnt_sub = conn.execute(
                text("SELECT COUNT(*) FROM user_subscriptions WHERE user_id='new_user_1' AND channel_id=1")
            ).scalar_one()
            source_type = conn.execute(
                text(
                    "SELECT source_type FROM user_subscriptions WHERE user_id='new_user_1' AND channel_id=1 LIMIT 1"
                )
            ).scalar_one()

        self.assertEqual(int(cnt_trial), 1)
        self.assertEqual(int(cnt_sub), 1)
        self.assertEqual(str(source_type), "trial")

    def test_grant_new_user_trial_all_reports_idempotent(self):
        ok1, msg1 = sub.grant_new_user_trial_all_reports("new_user_all")
        ok2, msg2 = sub.grant_new_user_trial_all_reports("new_user_all")

        self.assertTrue(ok1)
        self.assertTrue(msg1.startswith("trial_granted:") or msg1 == "already_granted")
        self.assertTrue(ok2)
        self.assertEqual(msg2, "already_granted")

        with self.engine.connect() as conn:
            cnt_sub = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM user_subscriptions
                    WHERE user_id='new_user_all' AND channel_id IN (1,2,3,4,5,6)
                    """
                )
            ).scalar_one()
            macro_trial_days = conn.execute(
                text(
                    """
                    SELECT days FROM user_trial_grants
                    WHERE user_id='new_user_all'
                      AND trial_code=:trial_code
                    LIMIT 1
                    """
                ),
                {"trial_code": f"new_user_all_reports_{sub.DEFAULT_NEW_USER_TRIAL_DAYS}d_v1:macro_risk_radar"},
            ).scalar_one()
            expected_marker = f"new_user_all_reports_{sub.DEFAULT_NEW_USER_TRIAL_DAYS}d_v1:marker"
            cnt_marker = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM user_trial_grants
                    WHERE user_id='new_user_all'
                      AND trial_code=:marker
                    """
                ),
                {"marker": expected_marker},
            ).scalar_one()
            cnt_trial_rows = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM user_trial_grants
                    WHERE user_id='new_user_all'
                    """
                )
            ).scalar_one()

        self.assertEqual(int(cnt_sub), 6)
        self.assertEqual(int(macro_trial_days), 30)
        self.assertEqual(int(cnt_marker), 1)
        self.assertEqual(int(cnt_trial_rows), 7)


if __name__ == "__main__":
    unittest.main()
