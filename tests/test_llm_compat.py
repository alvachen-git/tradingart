import os
import unittest
from unittest.mock import patch

from llm_compat import build_deepseek_flash_llm, build_report_tongyi_llm, invoke_report_llm_with_fallback


class LlmCompatTest(unittest.TestCase):
    def setUp(self):
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "DEEPSEEK_API_KEY",
                "DEEPSEEK_FAST_MODEL",
                "DEEPSEEK_BASE_URL",
                "DASHSCOPE_API_KEY",
                "REPORT_LLM_MODEL",
                "REPORT_LLM_TIMEOUT_SECONDS",
                "REPORT_LLM_FALLBACK_MODEL",
                "EXPIRY_OPTION_REPORT_LLM_MODEL",
                "EXPIRY_OPTION_REPORT_LLM_TIMEOUT_SECONDS",
            )
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

    def test_build_report_tongyi_llm_defaults_to_qwen36_with_600s_timeout(self):
        os.environ["DASHSCOPE_API_KEY"] = "test-key"
        os.environ.pop("REPORT_LLM_MODEL", None)
        os.environ.pop("REPORT_LLM_TIMEOUT_SECONDS", None)

        with patch("llm_compat.ChatTongyiCompat") as mock_chat:
            build_report_tongyi_llm(env_prefix="EXPIRY_OPTION_REPORT", temperature=0.1)

        kwargs = mock_chat.call_args.kwargs
        self.assertEqual(kwargs["model"], "qwen3.6-plus")
        self.assertEqual(kwargs["request_timeout"], 600)
        self.assertEqual(kwargs["max_retries"], 1)

    def test_build_report_tongyi_llm_accepts_script_default_model(self):
        os.environ["DASHSCOPE_API_KEY"] = "test-key"
        os.environ.pop("REPORT_LLM_MODEL", None)
        os.environ.pop("EXPIRY_OPTION_REPORT_LLM_MODEL", None)

        with patch("llm_compat.ChatTongyiCompat") as mock_chat:
            build_report_tongyi_llm(
                env_prefix="EXPIRY_OPTION_REPORT",
                temperature=0.1,
                default_model="qwen3.5-plus",
            )

        self.assertEqual(mock_chat.call_args.kwargs["model"], "qwen3.5-plus")

    def test_build_report_tongyi_llm_env_model_overrides_script_default(self):
        os.environ["DASHSCOPE_API_KEY"] = "test-key"
        os.environ["EXPIRY_OPTION_REPORT_LLM_MODEL"] = "qwen-plus"

        with patch("llm_compat.ChatTongyiCompat") as mock_chat:
            build_report_tongyi_llm(
                env_prefix="EXPIRY_OPTION_REPORT",
                temperature=0.1,
                default_model="qwen3.5-plus",
            )

        self.assertEqual(mock_chat.call_args.kwargs["model"], "qwen-plus")

    def test_report_llm_fallback_uses_qwen_plus_not_turbo(self):
        class Primary:
            model_name = "qwen3.6-plus"

            def invoke(self, messages):
                raise TimeoutError("read timed out")

        class Fallback:
            model_name = "qwen-plus"

            def invoke(self, messages):
                return "ok"

        with patch("llm_compat.build_report_tongyi_llm", return_value=Fallback()) as mock_build:
            result = invoke_report_llm_with_fallback(
                Primary(),
                [],
                env_prefix="EXPIRY_OPTION_REPORT",
                temperature=0.1,
            )

        self.assertEqual(result, "ok")
        self.assertEqual(mock_build.call_args.kwargs["model"], "qwen-plus")


if __name__ == "__main__":
    unittest.main()
