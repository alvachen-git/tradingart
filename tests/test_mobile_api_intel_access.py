import unittest
from unittest.mock import patch

_IMPORT_ERROR = None
try:
    from fastapi import HTTPException
    import mobile_api
except Exception as exc:  # pragma: no cover
    HTTPException = Exception
    mobile_api = None
    _IMPORT_ERROR = exc


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiIntelAccess(unittest.TestCase):
    def test_premium_report_requires_subscription(self):
        with patch.object(
            mobile_api.sub_svc,
            "get_content_by_id",
            return_value={"id": 1, "channel_id": 100, "is_premium": True},
        ), patch.object(
            mobile_api.sub_svc,
            "check_subscription_access",
            return_value={"has_access": False, "reason": "not_subscribed"},
        ):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.intel_report_detail(1, username="u1")
        self.assertEqual(cm.exception.status_code, 403)

    def test_subscribe_reject_when_api_disabled(self):
        body = mobile_api.SubscribeRequest(channel_code="daily_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", False):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.intel_subscribe(body=body, username="u1")
        self.assertEqual(cm.exception.status_code, 403)

    def test_subscribe_reject_when_not_in_whitelist(self):
        body = mobile_api.SubscribeRequest(channel_code="daily_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", True), patch.object(
            mobile_api, "_ALLOW_SELF_SUB_IN_PROD", True
        ), patch.object(
            mobile_api,
            "_is_production_env",
            return_value=False,
        ), patch.object(
            mobile_api,
            "_EFFECTIVE_FREE_CHANNEL_CODES",
            set(),
        ), patch.object(
            mobile_api.sub_svc,
            "get_channel_by_code",
            return_value={"id": 1, "code": "daily_report"},
        ):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.intel_subscribe(body=body, username="u1")
        self.assertEqual(cm.exception.status_code, 403)

    def test_subscribe_ok_when_enabled_and_whitelisted(self):
        body = mobile_api.SubscribeRequest(channel_code="daily_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", True), patch.object(
            mobile_api, "_ALLOW_SELF_SUB_IN_PROD", True
        ), patch.object(
            mobile_api,
            "_is_production_env",
            return_value=False,
        ), patch.object(
            mobile_api,
            "_EFFECTIVE_FREE_CHANNEL_CODES",
            {"daily_report"},
        ), patch.object(
            mobile_api.sub_svc,
            "get_channel_by_code",
            return_value={"id": 1, "code": "daily_report"},
        ), patch.object(
            mobile_api.sub_svc,
            "add_subscription",
            return_value=(True, "ok"),
        ) as mocked_add:
            out = mobile_api.intel_subscribe(body=body, username="u1")

        self.assertEqual(out["message"], "ok")
        kwargs = mocked_add.call_args.kwargs
        self.assertEqual(kwargs["source_type"], "self_subscribe_whitelist")

    def test_subscribe_reject_for_force_paid_channel_even_if_whitelisted(self):
        body = mobile_api.SubscribeRequest(channel_code="fund_flow_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", True), patch.object(
            mobile_api, "_ALLOW_SELF_SUB_IN_PROD", True
        ), patch.object(
            mobile_api,
            "_is_production_env",
            return_value=False,
        ), patch.object(
            mobile_api,
            "_EFFECTIVE_FREE_CHANNEL_CODES",
            {"daily_report"},
        ), patch.object(
            mobile_api.sub_svc,
            "get_channel_by_code",
            return_value={"id": 3, "code": "fund_flow_report"},
        ):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.intel_subscribe(body=body, username="u1")
        self.assertEqual(cm.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()
