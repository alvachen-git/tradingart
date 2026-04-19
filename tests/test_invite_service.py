import sys
import types
import unittest

from sqlalchemy import create_engine, text

if "data_engine" not in sys.modules:
    fake_data_engine = types.ModuleType("data_engine")
    fake_data_engine.engine = None
    sys.modules["data_engine"] = fake_data_engine

import invite_service as invite_svc
import payment_service as pay_svc


class TestInviteService(unittest.TestCase):
    def setUp(self):
        self.orig_inv_engine = invite_svc.engine
        self.orig_pay_engine = pay_svc.engine

        self.engine = create_engine("sqlite:///:memory:", future=True)
        invite_svc.engine = self.engine
        pay_svc.engine = self.engine

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE user_points (
                        user_id TEXT PRIMARY KEY,
                        balance INTEGER NOT NULL DEFAULT 0,
                        total_earned INTEGER NOT NULL DEFAULT 0,
                        total_spent INTEGER NOT NULL DEFAULT 0,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE points_transactions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL,
                        type TEXT NOT NULL,
                        amount INTEGER NOT NULL,
                        balance_after INTEGER NOT NULL,
                        ref_id TEXT,
                        description TEXT,
                        biz_id TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX uq_points_txn_user_type_biz "
                    "ON points_transactions(user_id, type, biz_id)"
                )
            )

    def tearDown(self):
        invite_svc.engine = self.orig_inv_engine
        pay_svc.engine = self.orig_pay_engine

    def test_get_or_create_invite_code_is_stable(self):
        code1 = invite_svc.get_or_create_invite_code("inviter_a")
        code2 = invite_svc.get_or_create_invite_code("inviter_a")
        self.assertTrue(code1.startswith("AIB"))
        self.assertEqual(code1, code2)

    def test_apply_invite_reward_success(self):
        code = invite_svc.get_or_create_invite_code("inviter_b")
        out = invite_svc.apply_invite_on_register(
            invitee_user_id="invitee_b1",
            invite_code=code,
            register_ip="1.1.1.1",
            device_fingerprint="device-a",
        )
        self.assertTrue(out["applied"])
        self.assertTrue(out["rewarded"])
        self.assertEqual(out["inviter_user_id"], "inviter_b")

        wallet = pay_svc.get_user_points("inviter_b")
        self.assertEqual(int(wallet["balance"]), 300)

    def test_apply_invite_self_invite_rejected(self):
        code = invite_svc.get_or_create_invite_code("user_self")
        out = invite_svc.apply_invite_on_register(
            invitee_user_id="user_self",
            invite_code=code,
            register_ip="2.2.2.2",
            device_fingerprint="device-self",
        )
        self.assertFalse(out["applied"])
        self.assertEqual(out["reason"], "self_invite")

        wallet = pay_svc.get_user_points("user_self")
        self.assertEqual(int(wallet["balance"]), 0)

    def test_apply_invite_idempotent_for_same_invitee(self):
        code = invite_svc.get_or_create_invite_code("inviter_c")
        out1 = invite_svc.apply_invite_on_register(
            invitee_user_id="invitee_c1",
            invite_code=code,
            register_ip="3.3.3.3",
            device_fingerprint="device-c",
        )
        out2 = invite_svc.apply_invite_on_register(
            invitee_user_id="invitee_c1",
            invite_code=code,
            register_ip="3.3.3.3",
            device_fingerprint="device-c",
        )
        self.assertTrue(out1["rewarded"])
        self.assertTrue(out2["rewarded"])
        self.assertEqual(out2["reason"], "already_rewarded")

        with self.engine.connect() as conn:
            tx_count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM points_transactions
                    WHERE user_id = 'inviter_c' AND type = 'admin_grant'
                    """
                )
            ).scalar_one()
        self.assertEqual(int(tx_count), 1)

    def test_apply_invite_rate_limit_by_ip(self):
        code = invite_svc.get_or_create_invite_code("inviter_d")
        out1 = invite_svc.apply_invite_on_register(
            invitee_user_id="invitee_d1",
            invite_code=code,
            register_ip="4.4.4.4",
            device_fingerprint="device-d1",
        )
        out2 = invite_svc.apply_invite_on_register(
            invitee_user_id="invitee_d2",
            invite_code=code,
            register_ip="4.4.4.4",
            device_fingerprint="device-d2",
        )
        self.assertTrue(out1["rewarded"])
        self.assertFalse(out2["rewarded"])
        self.assertEqual(out2["reason"], "ip_or_device_rate_limited")

        wallet = pay_svc.get_user_points("inviter_d")
        self.assertEqual(int(wallet["balance"]), 300)

    def test_apply_invite_rate_limit_by_device(self):
        code = invite_svc.get_or_create_invite_code("inviter_d2")
        out1 = invite_svc.apply_invite_on_register(
            invitee_user_id="invitee_d3",
            invite_code=code,
            register_ip="6.6.6.6",
            device_fingerprint="same-device",
        )
        out2 = invite_svc.apply_invite_on_register(
            invitee_user_id="invitee_d4",
            invite_code=code,
            register_ip="7.7.7.7",
            device_fingerprint="same-device",
        )
        self.assertTrue(out1["rewarded"])
        self.assertFalse(out2["rewarded"])
        self.assertEqual(out2["reason"], "ip_or_device_rate_limited")

        wallet = pay_svc.get_user_points("inviter_d2")
        self.assertEqual(int(wallet["balance"]), 300)

    def test_get_invite_stats(self):
        code = invite_svc.get_or_create_invite_code("inviter_e")
        invite_svc.apply_invite_on_register(
            invitee_user_id="invitee_e1",
            invite_code=code,
            register_ip="5.5.5.5",
            device_fingerprint="device-e1",
        )
        stats = invite_svc.get_invite_stats("inviter_e")
        self.assertEqual(int(stats["invited_count"]), 1)
        self.assertEqual(int(stats["rewarded_points"]), 300)

    def test_get_invite_context(self):
        code = invite_svc.get_or_create_invite_code("inviter_ctx")
        ctx = invite_svc.get_invite_context(code)
        self.assertTrue(ctx["is_valid"])
        self.assertEqual(ctx["inviter_user_id"], "inviter_ctx")

        invalid = invite_svc.get_invite_context("bad-code")
        self.assertFalse(invalid["is_valid"])

    def test_track_invite_event_records_context(self):
        code = invite_svc.get_or_create_invite_code("inviter_evt")
        ok = invite_svc.track_invite_event(
            code,
            "landing_view",
            session_id="sess-1",
            register_ip="8.8.8.8",
            device_fingerprint="device-evt",
            metadata={"source": "landing"},
        )
        self.assertTrue(ok)

        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT invite_code, inviter_user_id, event_type, session_id, register_ip_hash, device_hash, extra_json
                    FROM invite_landing_events
                    WHERE invite_code = :code
                    LIMIT 1
                    """
                ),
                {"code": code},
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]), code)
        self.assertEqual(str(row[1]), "inviter_evt")
        self.assertEqual(str(row[2]), "landing_view")
        self.assertEqual(str(row[3]), "sess-1")
        self.assertTrue(str(row[4]))
        self.assertTrue(str(row[5]))
        self.assertIn("landing", str(row[6]))


if __name__ == "__main__":
    unittest.main()
