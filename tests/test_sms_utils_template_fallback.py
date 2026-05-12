import os
import sys
import types
import unittest
from unittest.mock import patch


if "data_engine" not in sys.modules:
    fake_data_engine = types.ModuleType("data_engine")
    fake_data_engine.engine = None
    sys.modules["data_engine"] = fake_data_engine

import sms_utils


class TestSmsUtilsTemplateFallback(unittest.TestCase):
    def test_login_template_uses_login_code_first(self):
        with patch.dict(
            os.environ,
            {
                "ALIYUN_SMS_TEMPLATE_CODE_LOGIN": "SMS_LOGIN",
                "ALIYUN_SMS_TEMPLATE_CODE_REGISTER": "SMS_REGISTER",
            },
            clear=True,
        ):
            self.assertEqual(sms_utils._purpose_template_code("login"), "SMS_LOGIN")

    def test_login_template_falls_back_to_generic_code(self):
        with patch.dict(
            os.environ,
            {
                "ALIYUN_SMS_TEMPLATE_CODE": "SMS_GENERIC",
                "ALIYUN_SMS_TEMPLATE_CODE_REGISTER": "SMS_REGISTER",
            },
            clear=True,
        ):
            self.assertEqual(sms_utils._purpose_template_code("login"), "SMS_GENERIC")

    def test_login_template_falls_back_to_register_code(self):
        with patch.dict(
            os.environ,
            {"ALIYUN_SMS_TEMPLATE_CODE_REGISTER": "SMS_REGISTER"},
            clear=True,
        ):
            self.assertEqual(sms_utils._purpose_template_code("login"), "SMS_REGISTER")

    def test_register_template_can_use_generic_code(self):
        with patch.dict(os.environ, {"ALIYUN_SMS_TEMPLATE_CODE": "SMS_GENERIC"}, clear=True):
            self.assertEqual(sms_utils._purpose_template_code("register"), "SMS_GENERIC")


if __name__ == "__main__":
    unittest.main()
