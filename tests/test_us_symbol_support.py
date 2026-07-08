import sys
import types
from importlib.machinery import ModuleSpec

if "dotenv" not in sys.modules:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.__spec__ = ModuleSpec("dotenv", loader=None)
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_stub

import symbol_map


def test_resolve_us_ticker_case_insensitive(monkeypatch):
    monkeypatch.setattr(symbol_map, "get_all_market_map", lambda: {})
    assert symbol_map.resolve_symbol("aapl") == ("AAPL.US", "stock")
    assert symbol_map.resolve_symbol("TsLa") == ("TSLA.US", "stock")


def test_resolve_us_cn_alias(monkeypatch):
    monkeypatch.setattr(symbol_map, "get_all_market_map", lambda: {})
    assert symbol_map.resolve_symbol("苹果") == ("AAPL.US", "stock")
    assert symbol_map.resolve_symbol("特斯拉") == ("TSLA.US", "stock")
    assert symbol_map.resolve_symbol("英伟达") == ("NVDA.US", "stock")


def test_resolve_us_explicit_code(monkeypatch):
    monkeypatch.setattr(symbol_map, "get_all_market_map", lambda: {})
    assert symbol_map.resolve_symbol("AAPL.US") == ("AAPL.US", "stock")


def test_resolve_a_share_explicit_code_with_suffix(monkeypatch):
    monkeypatch.setattr(symbol_map, "get_all_market_map", lambda: {})
    assert symbol_map.resolve_symbol("688809.SH") == ("688809.SH", "stock")
    assert symbol_map.resolve_symbol("001399.SZ") == ("001399.SZ", "stock")


def test_resolve_a_share_name_inside_sentence(monkeypatch):
    monkeypatch.setattr(symbol_map, "get_all_market_map", lambda: {"强一股份": "688809.SH"})
    assert symbol_map.resolve_symbol("强一股份的技术面分析下") == ("688809.SH", "stock")


def test_resolve_hk_prefixed_and_unpadded_codes(monkeypatch):
    monkeypatch.setattr(symbol_map, "get_all_market_map", lambda: {})
    assert symbol_map.resolve_symbol("HK2513") == ("02513.HK", "stock")
    assert symbol_map.resolve_symbol("HKG2513") == ("02513.HK", "stock")
    assert symbol_map.resolve_symbol("2513.HK") == ("02513.HK", "stock")
    assert symbol_map.resolve_symbol("2513港股") == ("02513.HK", "stock")


def test_resolve_invalid_symbol(monkeypatch):
    monkeypatch.setattr(symbol_map, "get_all_market_map", lambda: {})
    assert symbol_map.resolve_symbol("NOT_A_REAL_SYMBOL_123") == (None, None)
