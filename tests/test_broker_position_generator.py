import importlib.util
import sys
import types
import uuid
from pathlib import Path

import pytest


def _load_module(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    target = root / "broker_position_generator.py"

    monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PASSWORD", "p")
    monkeypatch.setenv("DB_HOST", "127.0.0.1")
    monkeypatch.setenv("DB_PORT", "3306")
    monkeypatch.setenv("DB_NAME", "db")
    monkeypatch.setenv("DASHSCOPE_API_KEY", "test-key")

    fake_data_engine = types.ModuleType("data_engine")
    fake_data_engine.PRODUCT_MAP = {}
    fake_data_engine.search_broker_holdings_on_date = lambda *a, **k: ""
    fake_data_engine.tool_analyze_broker_positions = lambda *a, **k: ""
    fake_data_engine.tool_analyze_position_change = lambda *a, **k: ""
    fake_data_engine.get_latest_data_date = lambda: "20260325"
    monkeypatch.setitem(sys.modules, "data_engine", fake_data_engine)

    fake_kline_tools = types.ModuleType("kline_tools")
    fake_kline_tools.analyze_kline_pattern = lambda *a, **k: "ok"
    monkeypatch.setitem(sys.modules, "kline_tools", fake_kline_tools)

    fake_plot_tools = types.ModuleType("plot_tools")
    fake_plot_tools.draw_chart_tool = lambda *a, **k: ""
    monkeypatch.setitem(sys.modules, "plot_tools", fake_plot_tools)

    fake_news_tools = types.ModuleType("news_tools")
    fake_news_tools.get_financial_news = lambda *a, **k: ""
    monkeypatch.setitem(sys.modules, "news_tools", fake_news_tools)

    fake_search_tools = types.ModuleType("search_tools")
    fake_search_tools.search_web = lambda *a, **k: ""
    monkeypatch.setitem(sys.modules, "search_tools", fake_search_tools)

    fake_sub = types.ModuleType("subscription_service")
    fake_sub.publish_content = lambda **kwargs: (True, "ok")
    monkeypatch.setitem(sys.modules, "subscription_service", fake_sub)

    fake_llm = types.ModuleType("llm_compat")

    class DummyLLM:
        def __init__(self, *args, **kwargs):
            pass

        def invoke(self, *args, **kwargs):
            return types.SimpleNamespace(content="<html>ok</html>")

    fake_llm.ChatTongyiCompat = DummyLLM
    monkeypatch.setitem(sys.modules, "llm_compat", fake_llm)

    fake_core = types.ModuleType("langchain_core")
    fake_core_messages = types.ModuleType("langchain_core.messages")
    fake_core_tools = types.ModuleType("langchain_core.tools")

    class HumanMessage:
        def __init__(self, content):
            self.content = content

    def tool(fn):
        return fn

    fake_core_messages.HumanMessage = HumanMessage
    fake_core_tools.tool = tool
    monkeypatch.setitem(sys.modules, "langchain_core", fake_core)
    monkeypatch.setitem(sys.modules, "langchain_core.messages", fake_core_messages)
    monkeypatch.setitem(sys.modules, "langchain_core.tools", fake_core_tools)

    fake_langgraph = types.ModuleType("langgraph")
    fake_prebuilt = types.ModuleType("langgraph.prebuilt")

    def create_react_agent(llm, tools, prompt):
        class Agent:
            def invoke(self, *args, **kwargs):
                return {"messages": [types.SimpleNamespace(content="material " * 40)]}

        return Agent()

    fake_prebuilt.create_react_agent = create_react_agent
    monkeypatch.setitem(sys.modules, "langgraph", fake_langgraph)
    monkeypatch.setitem(sys.modules, "langgraph.prebuilt", fake_prebuilt)

    module_name = f"broker_position_generator_test_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, target)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_retry_ssl_error_then_success(monkeypatch):
    mod = _load_module(monkeypatch)
    monkeypatch.setattr(mod, "BROKER_REPORT_MAX_RETRY_SECONDS", 30)
    monkeypatch.setattr(mod, "_compute_backoff_seconds", lambda attempt, remaining: 0.0)
    monkeypatch.setattr(mod.time, "sleep", lambda _: None)

    state = {"n": 0}

    def flaky():
        state["n"] += 1
        if state["n"] < 3:
            raise RuntimeError("SSLEOFError: UNEXPECTED_EOF_WHILE_READING")
        return "ok"

    result = mod._invoke_with_retry(flaky, "unit_collect")
    assert result["ok"] is True
    assert result["result"] == "ok"
    assert result["attempts"] == 3


def test_retry_budget_exhausted(monkeypatch):
    mod = _load_module(monkeypatch)
    monkeypatch.setattr(mod, "BROKER_REPORT_MAX_RETRY_SECONDS", 1)
    monkeypatch.setattr(mod, "_compute_backoff_seconds", lambda attempt, remaining: 0.0)
    monkeypatch.setattr(mod.time, "sleep", lambda _: None)

    clock = {"t": 0.0}

    def fake_time():
        clock["t"] += 0.6
        return clock["t"]

    monkeypatch.setattr(mod.time, "time", fake_time)

    def always_fail():
        raise RuntimeError("HTTPSConnectionPool: Max retries exceeded")

    result = mod._invoke_with_retry(always_fail, "unit_collect")
    assert result["ok"] is False
    assert result["error_category"] == "network_transport"
    assert result["attempts"] >= 1


def test_403_is_non_retryable(monkeypatch):
    mod = _load_module(monkeypatch)
    category, retryable = mod._classify_retry(RuntimeError("403 Client Error: Forbidden"))
    assert category == "auth_forbidden"
    assert retryable is False


def test_invoke_with_retry_403_fails_immediately(monkeypatch):
    mod = _load_module(monkeypatch)
    monkeypatch.setattr(mod, "BROKER_REPORT_MAX_RETRY_SECONDS", 60)
    monkeypatch.setattr(mod, "_compute_backoff_seconds", lambda attempt, remaining: 0.0)
    monkeypatch.setattr(mod.time, "sleep", lambda _: None)

    state = {"n": 0}

    def always_403():
        state["n"] += 1
        raise RuntimeError("403 Client Error: Forbidden")

    result = mod._invoke_with_retry(always_403, "unit_collect")
    assert result["ok"] is False
    assert result["error_category"] == "auth_forbidden"
    assert result["attempts"] == 1


def test_main_collect_failure_exit_code_1(monkeypatch):
    mod = _load_module(monkeypatch)
    monkeypatch.setattr(mod, "should_skip_non_trading_publish", lambda: False)
    monkeypatch.setattr(
        mod,
        "collect_broker_position_data",
        lambda: {
            "ok": False,
            "error_category": "network_transport",
            "error_message": "timeout",
            "attempts": 3,
            "elapsed_seconds": 12.0,
        },
    )
    monkeypatch.setattr(mod, "_write_material_debug", lambda material, meta: None)

    with pytest.raises(SystemExit) as exc:
        mod.main()
    assert exc.value.code == 1


def test_main_draft_failure_exit_code_2(monkeypatch):
    mod = _load_module(monkeypatch)
    monkeypatch.setattr(mod, "should_skip_non_trading_publish", lambda: False)
    monkeypatch.setattr(
        mod,
        "collect_broker_position_data",
        lambda: {
            "ok": True,
            "material": "x" * 200,
            "attempts": 1,
            "elapsed_seconds": 1.0,
        },
    )
    monkeypatch.setattr(
        mod,
        "draft_broker_position_report",
        lambda material: {
            "ok": False,
            "error_category": "network_transport",
            "error_message": "timeout",
            "attempts": 2,
            "elapsed_seconds": 5.0,
        },
    )
    monkeypatch.setattr(mod, "_write_material_debug", lambda material, meta: None)

    with pytest.raises(SystemExit) as exc:
        mod.main()
    assert exc.value.code == 2


def test_main_publish_failure_exit_code_3(monkeypatch):
    mod = _load_module(monkeypatch)
    monkeypatch.setattr(mod, "should_skip_non_trading_publish", lambda: False)
    monkeypatch.setattr(
        mod,
        "collect_broker_position_data",
        lambda: {
            "ok": True,
            "material": "x" * 200,
            "attempts": 1,
            "elapsed_seconds": 1.0,
        },
    )
    monkeypatch.setattr(
        mod,
        "draft_broker_position_report",
        lambda material: {
            "ok": True,
            "report_html": "<html>" + ("x" * 400) + "</html>",
            "attempts": 1,
            "elapsed_seconds": 3.0,
        },
    )
    monkeypatch.setattr(mod, "publish_broker_position_report", lambda html: (False, "publish error"))
    monkeypatch.setattr(mod, "_write_material_debug", lambda material, meta: None)

    with pytest.raises(SystemExit) as exc:
        mod.main()
    assert exc.value.code == 3
