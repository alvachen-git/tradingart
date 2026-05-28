import unittest
from unittest.mock import patch

import search_tools


class _FakeMessage:
    content = "搜索结果"


class _FakeChoice:
    message = _FakeMessage()


class _FakeResponse:
    choices = [_FakeChoice()]


class _FakeCompletions:
    def __init__(self):
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return _FakeResponse()


class _FakeChat:
    def __init__(self):
        self.completions = _FakeCompletions()


class _FakeZhipuClient:
    last_instance = None

    def __init__(self, api_key):
        self.api_key = api_key
        self.chat = _FakeChat()
        _FakeZhipuClient.last_instance = self


class TestSearchToolsTimeout(unittest.TestCase):
    def test_search_web_passes_timeout_to_zhipu_request(self):
        with patch.object(search_tools, "ZHIPU_API_KEY", "test-key"), patch.object(
            search_tools, "_SEARCH_WEB_TIMEOUT_SECONDS", 7.0
        ), patch.object(search_tools, "_build_search_queries", return_value=["阳光电源 最近动态"]), patch.object(
            search_tools, "ZhipuAI", _FakeZhipuClient
        ):
            result = search_tools._search_web_impl("帮我看看阳光电源最近有什么利好")

        self.assertEqual(result, "搜索结果")
        self.assertEqual(_FakeZhipuClient.last_instance.chat.completions.kwargs["timeout"], 7.0)


if __name__ == "__main__":
    unittest.main()
