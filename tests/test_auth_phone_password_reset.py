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


class TestAuthPhonePasswordReset(unittest.TestCase):
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
                        password_hash TEXT,
                        is_active INTEGER NOT NULL DEFAULT 1
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE user_sessions (
                        username TEXT,
                        session_token TEXT,
                        token_expire DATETIME
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO users (username, phone, password_hash, is_active)
                    VALUES (:u, :p, :h, 1)
                    """
                ),
                {"u": "phone_user", "p": "13800138000", "h": "old_hash"},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO user_sessions (username, session_token, token_expire)
                    VALUES (:u, :t, CURRENT_TIMESTAMP)
                    """
                ),
                {"u": "phone_user", "t": "old_session"},
            )

    def tearDown(self):
        auth.engine = self.orig_engine

    def test_reset_password_with_phone_updates_hash_and_clears_sessions(self):
        with patch.object(auth, "normalize_cn_phone", return_value=(True, "13800138000", "ok")):
            with patch.object(auth, "verify_login_sms_code", return_value=(True, "ok")):
                with patch.object(auth, "hash_password", return_value="new_hash"):
                    ok, msg, username = auth.reset_password_with_phone(
                        "13800138000",
                        "123456",
                        "newpass123",
                    )

        self.assertTrue(ok)
        self.assertEqual(username, "phone_user")
        self.assertIn("账号 phone_user", msg)

        with self.engine.connect() as conn:
            user_row = conn.execute(
                text("SELECT password_hash FROM users WHERE username = :u"),
                {"u": "phone_user"},
            ).fetchone()
            session_count = conn.execute(
                text("SELECT COUNT(*) FROM user_sessions WHERE username = :u"),
                {"u": "phone_user"},
            ).scalar_one()

        self.assertEqual(user_row[0], "new_hash")
        self.assertEqual(int(session_count), 0)

    def test_reset_password_with_phone_rejects_missing_phone(self):
        with patch.object(auth, "normalize_cn_phone", return_value=(True, "13900139000", "ok")):
            with patch.object(auth, "verify_login_sms_code", return_value=(True, "ok")):
                ok, msg, username = auth.reset_password_with_phone(
                    "13900139000",
                    "123456",
                    "newpass123",
                )

        self.assertFalse(ok)
        self.assertEqual(msg, "该手机号未注册")
        self.assertEqual(username, "")

        with self.engine.connect() as conn:
            old_hash = conn.execute(
                text("SELECT password_hash FROM users WHERE username = :u"),
                {"u": "phone_user"},
            ).scalar_one()
        self.assertEqual(old_hash, "old_hash")

    def test_reset_password_with_phone_rejects_bad_code(self):
        with patch.object(auth, "normalize_cn_phone", return_value=(True, "13800138000", "ok")):
            with patch.object(auth, "verify_login_sms_code", return_value=(False, "验证码错误")):
                ok, msg, username = auth.reset_password_with_phone(
                    "13800138000",
                    "000000",
                    "newpass123",
                )

        self.assertFalse(ok)
        self.assertEqual(msg, "验证码错误")
        self.assertEqual(username, "")

        with self.engine.connect() as conn:
            old_hash = conn.execute(
                text("SELECT password_hash FROM users WHERE username = :u"),
                {"u": "phone_user"},
            ).scalar_one()
        self.assertEqual(old_hash, "old_hash")

    def test_reset_password_with_phone_rejects_short_password(self):
        with patch.object(auth, "normalize_cn_phone", return_value=(True, "13800138000", "ok")):
            with patch.object(auth, "verify_login_sms_code", return_value=(True, "ok")) as verify_mock:
                ok, msg, username = auth.reset_password_with_phone(
                    "13800138000",
                    "123456",
                    "short",
                )

        self.assertFalse(ok)
        self.assertEqual(msg, "新密码长度不能少于6位")
        self.assertEqual(username, "")
        verify_mock.assert_not_called()

        with self.engine.connect() as conn:
            old_hash = conn.execute(
                text("SELECT password_hash FROM users WHERE username = :u"),
                {"u": "phone_user"},
            ).scalar_one()
        self.assertEqual(old_hash, "old_hash")


if __name__ == "__main__":
    unittest.main()
