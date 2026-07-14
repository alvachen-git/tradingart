import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMMODITY_PAGE = ROOT / "pages" / "02_商品期权.py"
BACKTEST_PAGE = ROOT / "pages" / "12_策略回测.py"


def _function_args(source: str, function_name: str) -> list[str]:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            return [arg.arg for arg in node.args.args]
    raise AssertionError(f"function not found: {function_name}")


def _literal_assignment(source: str, variable_name: str):
    tree = ast.parse(source)
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == variable_name for target in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"assignment not found: {variable_name}")


def test_commodity_page_includes_red_date_option():
    source = COMMODITY_PAGE.read_text(encoding="utf-8")
    commodity_map = _literal_assignment(source, "COMMODITY_MAP")
    assert commodity_map["CJ"] == "红枣"


def test_commodity_contract_query_keeps_indexable_predicates():
    source = COMMODITY_PAGE.read_text(encoding="utf-8")
    assert "WHERE trade_date >= :cutoff" in source
    assert "ts_code LIKE :prefix_like" in source
    assert "ts_code REGEXP :prefix_regex" in source
    assert "REPLACE(trade_date" not in source
    assert "UPPER(ts_code)" not in source


def test_commodity_market_cache_keys_are_shared_across_users():
    source = COMMODITY_PAGE.read_text(encoding="utf-8")
    assert _function_args(source, "get_contracts") == [
        "v",
        "cutoff_yyyymmdd",
        "current_yymm",
    ]
    assert _function_args(source, "get_chart_data") == [
        "code",
        "is_continuous_flag",
    ]
    assert "_CACHE_PROBE_SEEN" not in source
    assert "_probe_cache" not in source


def test_backtest_page_only_loads_strikes_for_manual_mode():
    source = BACKTEST_PAGE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    manual_if = None
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "strike_mode"
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "手动选择"
        ):
            manual_if = node
            break
    assert manual_if is not None

    manual_calls = {
        node.func.id
        for node in ast.walk(manual_if)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert {
        "_cached_etf_first_trade_date",
        "_cached_etf_expiries",
        "_cached_etf_strikes_for_range",
        "_cached_etf_strikes_for_expiry",
    }.issubset(manual_calls)
    assert "commodity_opt_daily" not in source
