import unittest
from unittest.mock import patch

_IMPORT_ERROR = None
try:
    from fastapi import HTTPException
    from fastapi.security import HTTPAuthorizationCredentials
    from starlette.requests import Request
    import mobile_api
except Exception as exc:  # pragma: no cover
    HTTPException = Exception
    HTTPAuthorizationCredentials = None
    Request = None
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

    def _build_request(self, host: str = "www.aiprota.com", origin: str = "", referer: str = "", cookie: str = ""):
        headers = [(b"host", host.encode("utf-8"))]
        if origin:
            headers.append((b"origin", origin.encode("utf-8")))
        if referer:
            headers.append((b"referer", referer.encode("utf-8")))
        if cookie:
            headers.append((b"cookie", cookie.encode("utf-8")))
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/auth/session/bootstrap",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "scheme": "https",
        }
        return Request(scope)

    def test_ensure_same_origin_rejects_cross_origin(self):
        req = self._build_request(origin="https://evil.example.com")
        with self.assertRaises(HTTPException) as cm:
            mobile_api._ensure_same_origin(req)
        self.assertEqual(cm.exception.status_code, 403)

    def test_bootstrap_session_returns_logged_in_true_when_cookie_token_valid(self):
        req = self._build_request(
            origin="https://www.aiprota.com",
            referer="https://www.aiprota.com/pages/index/index",
            cookie="username=u1; token=raw-1",
        )
        with patch.object(mobile_api, "_enforce_bootstrap_rate_limit"), patch.object(
            mobile_api.auth, "check_token", return_value=True
        ), patch.object(
            mobile_api, "_fetch_session_expire_at", return_value="2026-05-01 00:00:00"
        ):
            out = mobile_api.bootstrap_session(req)

        self.assertTrue(out["logged_in"])
        self.assertEqual(out["username"], "u1")
        self.assertEqual(out["token"], "raw-1")
        self.assertEqual(out["expire_at"], "2026-05-01 00:00:00")

    def test_bootstrap_session_returns_missing_cookie_when_cookie_absent(self):
        req = self._build_request(origin="https://www.aiprota.com")
        with patch.object(mobile_api, "_enforce_bootstrap_rate_limit"):
            out = mobile_api.bootstrap_session(req)
        self.assertFalse(out["logged_in"])
        self.assertEqual(out["reason"], "missing_cookie")

    def test_bootstrap_session_returns_invalid_session_when_token_invalid(self):
        req = self._build_request(
            origin="https://www.aiprota.com",
            cookie="username=u1; token=raw-2",
        )
        with patch.object(mobile_api, "_enforce_bootstrap_rate_limit"), patch.object(
            mobile_api.auth, "check_token", return_value=False
        ):
            out = mobile_api.bootstrap_session(req)
        self.assertFalse(out["logged_in"])
        self.assertEqual(out["reason"], "invalid_session")


if __name__ == "__main__":
    unittest.main()
