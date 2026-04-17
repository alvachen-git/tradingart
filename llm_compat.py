from __future__ import annotations

import json
from typing import Any, Dict, List

from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import BaseMessage


def is_qwen_multimodal_family(model_name: str | None) -> bool:
    """Return True for qwen3.5/qwen3.6 models that need multimodal endpoint routing."""
    if not model_name:
        return False
    m = str(model_name).lower()
    return m.startswith("qwen3.5-") or m.startswith("qwen3.6-")


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

