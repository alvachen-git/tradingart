import os
import unittest

from llm_compat import build_deepseek_flash_llm


class LlmCompatTest(unittest.TestCase):
    def setUp(self):
        self._old_env = {
            key: os.environ.get(key)
            for key in ("DEEPSEEK_API_KEY", "DEEPSEEK_FAST_MODEL", "DEEPSEEK_BASE_URL")
        }

    def tearDown(self):
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_build_deepseek_flash_llm_defaults_to_flash_with_thinking_disabled(self):
        os.environ["DEEPSEEK_API_KEY"] = "test-key"
        os.environ.pop("DEEPSEEK_FAST_MODEL", None)
        os.environ.pop("DEEPSEEK_BASE_URL", None)

        llm = build_deepseek_flash_llm()

        self.assertEqual(llm.model_name, "deepseek-v4-flash")
        self.assertEqual(llm.extra_body, {"thinking": {"type": "disabled"}})
        self.assertEqual(str(llm.openai_api_base).rstrip("/"), "https://api.deepseek.com")

    def test_build_deepseek_flash_llm_requires_api_key(self):
        os.environ.pop("DEEPSEEK_API_KEY", None)

        with self.assertRaisesRegex(RuntimeError, "DEEPSEEK_API_KEY not configured"):
            build_deepseek_flash_llm()


if __name__ == "__main__":
    unittest.main()
