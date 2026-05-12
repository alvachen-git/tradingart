import unittest
from unittest.mock import patch

_IMPORT_ERROR = None
try:
    from fastapi import HTTPException
    from fastapi.security import HTTPAuthorizationCredentials
    import mobile_api
except Exception as exc:  # pragma: no cover
    HTTPException = Exception
    HTTPAuthorizationCredentials = None
    mobile_api = None
    _IMPORT_ERROR = exc


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiAuthGuard(unittest.TestCase):
    def _cred(self, token: str = "u1:tok-1"):
        return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)

    def test_get_current_user_returns_username_when_legacy_token_valid(self):
        with patch.object(mobile_api.auth, "check_token", return_value=True) as mocked_check:
            out = mobile_api.get_current_user(credentials=self._cred())

        self.assertEqual(out, "u1")
        self.assertEqual(mocked_check.call_args.kwargs["strict"], True)

    def test_get_current_user_returns_username_when_raw_token_valid(self):
        with patch.object(mobile_api.auth, "get_username_by_token", return_value="u2") as mocked_get:
            out = mobile_api.get_current_user(credentials=self._cred("tok-2"))

        self.assertEqual(out, "u2")
        self.assertEqual(mocked_get.call_args.kwargs["strict"], True)

    def test_get_current_user_returns_401_when_legacy_token_invalid(self):
        with patch.object(mobile_api.auth, "check_token", return_value=False):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.get_current_user(credentials=self._cred())

        self.assertEqual(cm.exception.status_code, 401)
        self.assertIn("Token 无效或已过期", str(cm.exception.detail))

    def test_get_current_user_returns_401_when_raw_token_invalid(self):
        with patch.object(mobile_api.auth, "get_username_by_token", return_value=""):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.get_current_user(credentials=self._cred("tok-3"))

        self.assertEqual(cm.exception.status_code, 401)
        self.assertIn("Token 无效或已过期", str(cm.exception.detail))

    def test_get_current_user_returns_503_when_auth_backend_errors_legacy(self):
        with patch.object(mobile_api.auth, "check_token", side_effect=RuntimeError("db down")):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.get_current_user(credentials=self._cred())

        self.assertEqual(cm.exception.status_code, 503)
        self.assertIn("认证服务繁忙", str(cm.exception.detail))

    def test_get_current_user_returns_503_when_auth_backend_errors_raw(self):
        with patch.object(mobile_api.auth, "get_username_by_token", side_effect=RuntimeError("db down")):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.get_current_user(credentials=self._cred("tok-4"))

        self.assertEqual(cm.exception.status_code, 503)
        self.assertIn("认证服务繁忙", str(cm.exception.detail))

    def test_password_reset_send_phone_code_returns_message(self):
        class _Client:
            host = "127.0.0.1"

        class _Request:
            client = _Client()

        body = mobile_api.PasswordResetSendPhoneCodeRequest(phone="13800138000")
        with patch.object(
            mobile_api.auth,
            "send_reset_password_phone_code",
            return_value=(True, "验证码已发送"),
        ) as mocked_send:
            out = mobile_api.password_reset_send_phone_code(body, _Request())

        self.assertEqual(out, {"message": "验证码已发送"})
        mocked_send.assert_called_once_with("13800138000", client_ip="127.0.0.1")

    def test_password_reset_send_phone_code_raises_400_on_failure(self):
        body = mobile_api.PasswordResetSendPhoneCodeRequest(phone="bad-phone")
        with patch.object(
            mobile_api.auth,
            "send_reset_password_phone_code",
            return_value=(False, "手机号格式错误"),
        ):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.password_reset_send_phone_code(body, None)

        self.assertEqual(cm.exception.status_code, 400)
        self.assertEqual(cm.exception.detail, "手机号格式错误")

    def test_password_reset_returns_username_after_success(self):
        body = mobile_api.PasswordResetRequest(
            phone="13800138000",
            sms_code="123456",
            new_password="newpass123",
            new_password_confirm="newpass123",
        )
        with patch.object(
            mobile_api.auth,
            "reset_password_with_phone",
            return_value=(True, "密码重置成功，请使用账号 phone_user 和新密码登录", "phone_user"),
        ) as mocked_reset:
            out = mobile_api.password_reset(body)

        self.assertEqual(
            out,
            {
                "message": "密码重置成功，请使用账号 phone_user 和新密码登录",
                "username": "phone_user",
            },
        )
        mocked_reset.assert_called_once_with("13800138000", "123456", "newpass123")

    def test_password_reset_rejects_mismatched_passwords_before_auth_call(self):
        body = mobile_api.PasswordResetRequest(
            phone="13800138000",
            sms_code="123456",
            new_password="newpass123",
            new_password_confirm="newpass456",
        )
        with patch.object(mobile_api.auth, "reset_password_with_phone") as mocked_reset:
            with self.assertRaises(HTTPException) as cm:
                mobile_api.password_reset(body)

        self.assertEqual(cm.exception.status_code, 400)
        self.assertEqual(cm.exception.detail, "两次密码不一致")
        mocked_reset.assert_not_called()


if __name__ == "__main__":
    unittest.main()
