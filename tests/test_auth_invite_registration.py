import sys
import types
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

if "data_engine" not in sys.modules:
    fake_data_engine = types.ModuleType("data_engine")
    fake_data_engine.engine = None
    sys.modules["data_engine"] = fake_data_engine

if "streamlit" not in sys.modules:
    fake_streamlit = types.ModuleType("streamlit")
    sys.modules["streamlit"] = fake_streamlit

if "bcrypt" not in sys.modules:
    fake_bcrypt = types.ModuleType("bcrypt")
    fake_bcrypt.hashpw = lambda password, salt: b"hashed_pw"
    fake_bcrypt.gensalt = lambda: b"salt"
    fake_bcrypt.checkpw = lambda password, hashed: True
    sys.modules["bcrypt"] = fake_bcrypt

import auth_utils as auth


class TestAuthInviteRegistration(unittest.TestCase):
    def setUp(self):
        self.orig_engine = auth.engine
        self.engine = create_engine("sqlite:///:memory:", future=True)
        auth.engine = self.engine

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE users (
                        username TEXT PRIMARY KEY,
                        phone TEXT UNIQUE,
                        phone_verified INTEGER NOT NULL DEFAULT 0,
                        password_hash TEXT,
                        level INTEGER NOT NULL DEFAULT 1,
                        experience INTEGER NOT NULL DEFAULT 0,
                        capital INTEGER NOT NULL DEFAULT 1000000,
                        is_active INTEGER NOT NULL DEFAULT 1,
                        email_verified INTEGER NOT NULL DEFAULT 0,
                        created_at DATETIME
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE user_profile (
                        user_id TEXT PRIMARY KEY,
                        risk_preference TEXT,
                        focus_assets TEXT,
                        current_mood TEXT
                    )
                    """
                )
            )

    def tearDown(self):
        auth.engine = self.orig_engine

    def test_register_survives_invite_failure_and_keeps_trial_grant(self):
        fake_invite_module = types.ModuleType("invite_service")

        def _raise_invite_error(**kwargs):
            raise RuntimeError("invite boom")

        fake_invite_module.apply_invite_on_register = _raise_invite_error

        with patch.object(auth, "normalize_cn_phone", return_value=(True, "13800138000", "")):
            with patch.object(auth, "hash_password", return_value="hashed_pw"):
                with patch.object(auth.sub_svc, "grant_new_user_trial_all_reports", return_value=(True, "trial_granted")) as trial_mock:
                    with patch.dict(sys.modules, {"invite_service": fake_invite_module}):
                        ok, msg = auth.register_with_phone_password(
                            phone="13800138000",
                            password="secret123",
                            username="invite_auth_user",
                            invite_code="AIBXYZ1234",
                            register_ip="8.8.8.8",
                            device_fingerprint="device-auth-1",
                        )

        self.assertTrue(ok)
        self.assertIn("注册成功", msg)
        trial_mock.assert_called_once_with("invite_auth_user")

        with self.engine.connect() as conn:
            user_row = conn.execute(
                text("SELECT username, phone, phone_verified FROM users WHERE username = :u"),
                {"u": "invite_auth_user"},
            ).fetchone()
            profile_row = conn.execute(
                text("SELECT user_id FROM user_profile WHERE user_id = :u"),
                {"u": "invite_auth_user"},
            ).fetchone()

        self.assertIsNotNone(user_row)
        self.assertEqual(str(user_row[0]), "invite_auth_user")
        self.assertEqual(str(user_row[1]), "13800138000")
        self.assertEqual(int(user_row[2]), 1)
        self.assertIsNotNone(profile_row)

    def test_existing_phone_blocks_registration_before_invite_reward(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO users (
                        username, phone, phone_verified, password_hash,
                        level, experience, capital, is_active, email_verified, created_at
                    ) VALUES (
                        :u, :p, 1, :h,
                        1, 0, 1000000, 1, 0, CURRENT_TIMESTAMP
                    )
                    """
                ),
                {"u": "existing_user", "p": "18516020270", "h": "hashed_old"},
            )

        fake_invite_module = types.ModuleType("invite_service")
        invite_calls = []

        def _track_invite_call(**kwargs):
            invite_calls.append(kwargs)
            return {"applied": True, "rewarded": True, "reason": "ok"}

        fake_invite_module.apply_invite_on_register = _track_invite_call

        with patch.object(auth, "normalize_cn_phone", return_value=(True, "18516020270", "")):
            with patch.object(auth, "hash_password", return_value="hashed_pw"):
                with patch.object(auth.sub_svc, "grant_new_user_trial_all_reports", return_value=(True, "trial_granted")) as trial_mock:
                    with patch.dict(sys.modules, {"invite_service": fake_invite_module}):
                        ok, msg = auth.register_with_phone_password(
                            phone="18516020270",
                            password="secret123",
                            username="invite_auth_user_2",
                            invite_code="AIBXYZ1234",
                            register_ip="9.9.9.9",
                            device_fingerprint="device-auth-2",
                        )

        self.assertFalse(ok)
        self.assertEqual(msg, "该手机号已注册，请更换手机号")
        self.assertEqual(invite_calls, [])
        trial_mock.assert_not_called()

        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT COUNT(*) FROM users")).scalar_one()
        self.assertEqual(int(rows), 1)


if __name__ == "__main__":
    unittest.main()
