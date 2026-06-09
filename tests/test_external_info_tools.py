import importlib
import sys
import types
import unittest
from unittest import mock


def _import_polymarket_tool_with_stub():
    sys.modules.pop("polymarket_tool", None)
    risk_stub = types.ModuleType("risk_index_service")
    risk_stub.fetch_polymarket_events = lambda **_kwargs: []
    risk_stub.normalize_probability = lambda value: max(0.0, min(1.0, float(value or 0)))
    sys.modules["risk_index_service"] = risk_stub
    return importlib.import_module("polymarket_tool")


class FakeResponse:
    def __init__(self, status_code=200, payload=None, error=None):
        self.status_code = status_code
        self._payload = payload or {}
        self._error = error
        if self._error is not None:
            setattr(self._error, "response", self)

    def raise_for_status(self):
        if self._error is not None:
            raise self._error

    def json(self):
        return self._payload


class FakeRequests:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        if not self.responses:
            raise RuntimeError("unexpected request")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class PolymarketGuardTest(unittest.TestCase):
    def test_timeout_blocks_second_call_in_same_task(self):
        polymarket_tool = _import_polymarket_tool_with_stub()
        polymarket_tool.reset_polymarket_task_guard()

        with mock.patch.object(
            polymarket_tool,
            "fetch_polymarket_events",
            side_effect=TimeoutError("connect timed out"),
        ) as fetch_mock:
            first = polymarket_tool._get_polymarket_sentiment_text("Fed")
            second = polymarket_tool._get_polymarket_sentiment_text("Gold")

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertIn("已跳过", first)
        self.assertIn("已跳过", second)

    def test_reset_allows_next_task_to_try_again(self):
        polymarket_tool = _import_polymarket_tool_with_stub()
        polymarket_tool.reset_polymarket_task_guard()

        with mock.patch.object(
            polymarket_tool,
            "fetch_polymarket_events",
            side_effect=[TimeoutError("first timeout"), []],
        ) as fetch_mock:
            first = polymarket_tool._get_polymarket_sentiment_text("Fed")
            polymarket_tool.reset_polymarket_task_guard()
            second = polymarket_tool._get_polymarket_sentiment_text("Fed")

        self.assertEqual(fetch_mock.call_count, 2)
        self.assertIn("已跳过", first)
        self.assertIn("暂无", second)

    def test_http_error_blocks_repeated_polymarket_attempts(self):
        polymarket_tool = _import_polymarket_tool_with_stub()
        polymarket_tool.reset_polymarket_task_guard()

        with mock.patch.object(
            polymarket_tool,
            "fetch_polymarket_events",
            side_effect=RuntimeError("404 Client Error"),
        ) as fetch_mock:
            first = polymarket_tool._get_polymarket_sentiment_text("Fed")
            second = polymarket_tool._get_polymarket_sentiment_text("Oil")

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertIn("已跳过", first)
        self.assertIn("已跳过", second)


class NewsToolFallbackTest(unittest.TestCase):
    def setUp(self):
        sys.modules.pop("news_tools", None)
        self.news_tools = importlib.import_module("news_tools")

    def test_cls_direct_success_returns_cls_news(self):
        fake_requests = FakeRequests(
            [
                FakeResponse(
                    payload={
                        "data": {
                            "roll_data": [
                                {
                                    "title": "黄金上涨",
                                    "content": "美联储利率预期降温，黄金白银走强。",
                                    "ctime": 1718000000,
                                }
                            ]
                        }
                    }
                )
            ]
        )
        self.news_tools.requests = fake_requests

        result = self.news_tools._get_financial_news_text("黄金")

        self.assertIn("财联社", result)
        self.assertIn("黄金上涨", result)
        self.assertEqual(len(fake_requests.calls), 1)

    def test_cls_404_falls_back_to_eastmoney(self):
        fake_requests = FakeRequests(
            [
                FakeResponse(status_code=404, error=RuntimeError("404 Client Error")),
                FakeResponse(status_code=404, error=RuntimeError("404 Client Error")),
                FakeResponse(status_code=404, error=RuntimeError("404 Client Error")),
                FakeResponse(status_code=404, error=RuntimeError("404 Client Error")),
                FakeResponse(
                    payload={
                        "data": {
                            "fastNewsList": [
                                {
                                    "title": "白银短线走强",
                                    "summary": "贵金属板块活跃。",
                                    "showTime": "2026-06-09 09:30:00",
                                    "code": "202606090001",
                                }
                            ]
                        }
                    }
                ),
            ]
        )
        self.news_tools.requests = fake_requests

        result = self.news_tools._get_financial_news_text("白银")

        self.assertIn("东方财富", result)
        self.assertIn("白银短线走强", result)
        self.assertEqual(len(fake_requests.calls), 5)

    def test_all_news_sources_failed_returns_clear_message(self):
        fake_requests = FakeRequests(
            [
                FakeResponse(status_code=404, error=RuntimeError("404 Client Error")),
                FakeResponse(status_code=404, error=RuntimeError("404 Client Error")),
                FakeResponse(status_code=404, error=RuntimeError("404 Client Error")),
                FakeResponse(status_code=404, error=RuntimeError("404 Client Error")),
                TimeoutError("eastmoney timeout"),
            ]
        )
        self.news_tools.requests = fake_requests

        result = self.news_tools._get_financial_news_text("黄金")

        self.assertIn("所有新闻接口暂时不可用", result)
        self.assertEqual(len(fake_requests.calls), 5)


if __name__ == "__main__":
    unittest.main()
