from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import BaseMessage


DEFAULT_REPORT_LLM_MODEL = "qwen3.6-plus"
DEFAULT_REPORT_LLM_FALLBACK_MODEL = "qwen-plus"
DEFAULT_REPORT_LLM_TIMEOUT_SECONDS = 600
DEFAULT_REPORT_LLM_MAX_RETRIES = 1


def is_qwen_multimodal_family(model_name: str | None) -> bool:
    """Return True for qwen3.5/qwen3.6 models that need multimodal endpoint routing."""
    if not model_name:
        return False
    m = str(model_name).lower()
    return m.startswith("qwen3.5-") or m.startswith("qwen3.6-")


def build_deepseek_flash_llm(
    *,
    temperature: float = 0.2,
    streaming: bool = False,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    **kwargs: Any,
) -> Any:
    """
    Build the project's fast-response DeepSeek chat model.

    This intentionally disables DeepSeek thinking mode for quick-reply paths,
    where latency matters more than multi-step reasoning.
    """
    try:
        from langchain_deepseek import ChatDeepSeek
    except Exception as exc:
        raise RuntimeError("langchain-deepseek is not installed") from exc

    resolved_api_key = (api_key or os.getenv("DEEPSEEK_API_KEY") or "").strip()
    if not resolved_api_key:
        raise RuntimeError("DEEPSEEK_API_KEY not configured")

    extra_body = dict(kwargs.pop("extra_body", {}) or {})
    thinking = dict(extra_body.get("thinking", {}) or {})
    thinking["type"] = "disabled"
    extra_body["thinking"] = thinking

    return ChatDeepSeek(
        model=(model or os.getenv("DEEPSEEK_FAST_MODEL") or "deepseek-v4-flash"),
        api_key=resolved_api_key,
        base_url=(base_url or os.getenv("DEEPSEEK_BASE_URL") or "https://api.deepseek.com"),
        temperature=temperature,
        streaming=streaming,
        extra_body=extra_body,
        **kwargs,
    )


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def build_report_tongyi_llm(
    *,
    env_prefix: str,
    temperature: float = 0.1,
    model: str | None = None,
    api_key: str | None = None,
    request_timeout: int | None = None,
    max_retries: int | None = None,
    **kwargs: Any,
) -> ChatTongyi:
    """
    Build a DashScope report-generation LLM with stable shared defaults.

    Env order:
    - <ENV_PREFIX>_LLM_MODEL / REPORT_LLM_MODEL / qwen3.6-plus
    - <ENV_PREFIX>_LLM_TIMEOUT_SECONDS / REPORT_LLM_TIMEOUT_SECONDS / 600
    - <ENV_PREFIX>_LLM_MAX_RETRIES / REPORT_LLM_MAX_RETRIES / 1
    """
    prefix = str(env_prefix or "REPORT").strip().upper()
    resolved_model = (
        model
        or os.getenv(f"{prefix}_LLM_MODEL")
        or os.getenv("REPORT_LLM_MODEL")
        or DEFAULT_REPORT_LLM_MODEL
    )
    resolved_timeout = request_timeout
    if resolved_timeout is None:
        resolved_timeout = _read_int_env(
            f"{prefix}_LLM_TIMEOUT_SECONDS",
            _read_int_env("REPORT_LLM_TIMEOUT_SECONDS", DEFAULT_REPORT_LLM_TIMEOUT_SECONDS),
        )
    resolved_retries = max_retries
    if resolved_retries is None:
        resolved_retries = _read_int_env(
            f"{prefix}_LLM_MAX_RETRIES",
            _read_int_env("REPORT_LLM_MAX_RETRIES", DEFAULT_REPORT_LLM_MAX_RETRIES),
        )
    return ChatTongyiCompat(
        model=resolved_model,
        temperature=temperature,
        api_key=api_key or os.getenv("DASHSCOPE_API_KEY"),
        request_timeout=resolved_timeout,
        max_retries=resolved_retries,
        **kwargs,
    )


def _is_retryable_report_llm_error(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    retryable_markers = (
        "timeout",
        "timed out",
        "read timed out",
        "connection aborted",
        "connection reset",
        "temporarily unavailable",
        "too many requests",
        "rate limit",
        "429",
        "500",
        "502",
        "503",
        "504",
    )
    return any(marker in text for marker in retryable_markers)


def invoke_report_llm_with_fallback(
    llm: Any,
    messages: List[BaseMessage],
    *,
    env_prefix: str,
    temperature: float = 0.1,
    api_key: str | None = None,
) -> Any:
    """Invoke a report LLM, retrying once on qwen-plus if the primary call is transiently unavailable."""
    try:
        return llm.invoke(messages)
    except Exception as exc:
        if not _is_retryable_report_llm_error(exc):
            raise

        prefix = str(env_prefix or "REPORT").strip().upper()
        fallback_model = (
            os.getenv(f"{prefix}_LLM_FALLBACK_MODEL")
            or os.getenv("REPORT_LLM_FALLBACK_MODEL")
            or DEFAULT_REPORT_LLM_FALLBACK_MODEL
        )
        primary_model = str(getattr(llm, "model_name", "") or getattr(llm, "model", "") or "")
        if fallback_model == primary_model:
            raise

        print(
            f"[ReportLLM] primary model={primary_model or 'unknown'} failed with {type(exc).__name__}; "
            f"fallback to {fallback_model}"
        )
        fallback_llm = build_report_tongyi_llm(
            env_prefix=prefix,
            temperature=temperature,
            model=fallback_model,
            api_key=api_key,
        )
        return fallback_llm.invoke(messages)


class ChatTongyiCompat(ChatTongyi):
    """
    Project-local compatibility adapter for qwen3.5-* and qwen3.6-* models.

    Why:
    - Current langchain_community.ChatTongyi routes unknown qwen models to
      dashscope.Generation (text-generation endpoint).
    - qwen3.5-plus / qwen3.6-plus work on dashscope.MultiModalConversation in
      the current SDK.

    This adapter:
    1) Forces qwen3.5-* and qwen3.6-* models to use MultiModalConversation client.
    2) Converts message content strings to multimodal text blocks.
    3) Normalizes multimodal list content in responses back to plain text for
       LangChain message objects.
    """

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if is_qwen_multimodal_family(self.model_name):
            try:
                import dashscope

                object.__setattr__(self, "client", dashscope.MultiModalConversation)
                print(
                    f"[LLMCompat] Force MultiModalConversation for model={self.model_name}"
                )
            except Exception as e:
                print(
                    f"[LLMCompat] Failed to switch multimodal qwen client to MultiModalConversation: {e}"
                )

    @property
    def _default_params(self) -> Dict[str, Any]:
        params = dict(super()._default_params)
        # MultiModalConversation works without this; removing avoids endpoint-specific
        # parameter incompatibilities on some SDK versions.
        if is_qwen_multimodal_family(self.model_name):
            params.pop("result_format", None)
        return params

    def _invocation_params(
        self, messages: List[BaseMessage], stop: Any, **kwargs: Any
    ) -> Dict[str, Any]:
        params = super()._invocation_params(messages=messages, stop=stop, **kwargs)
        if not is_qwen_multimodal_family(self.model_name):
            return params

        params["messages"] = self._to_multimodal_messages(params.get("messages", []))
        return params

    def completion_with_retry(self, **kwargs: Any) -> Any:
        resp = super().completion_with_retry(**kwargs)
        if is_qwen_multimodal_family(self.model_name):
            resp = self._normalize_multimodal_response(resp)
        return resp

    def stream_completion_with_retry(self, **kwargs: Any) -> Any:
        if not is_qwen_multimodal_family(self.model_name):
            yield from super().stream_completion_with_retry(**kwargs)
            return

        for resp in super().stream_completion_with_retry(**kwargs):
            yield self._normalize_multimodal_response(resp)

    @staticmethod
    def _to_multimodal_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        converted: List[Dict[str, Any]] = []
        for msg in messages:
            m = dict(msg)
            content = m.get("content")
            if isinstance(content, str):
                m["content"] = [{"text": content}]
            converted.append(m)
        return converted

    @staticmethod
    def _normalize_multimodal_response(resp: Any) -> Any:
        """
        Convert response message content from list[{text: ...}] to plain text.
        Works for dict-like dashscope responses used by ChatTongyi.
        """
        try:
            resp_copy = json.loads(json.dumps(resp))
            choice = resp_copy.get("output", {}).get("choices", [{}])[0]
            message = choice.get("message", {})
            content = message.get("content")
            if isinstance(content, list):
                message["content"] = "".join(
                    item.get("text", "")
                    for item in content
                    if isinstance(item, dict)
                )
            return resp_copy
        except Exception:
            return resp
