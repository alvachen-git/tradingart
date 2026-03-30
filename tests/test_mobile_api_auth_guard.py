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


if __name__ == "__main__":
    unittest.main()
