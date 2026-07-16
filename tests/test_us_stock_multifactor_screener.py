import pandas as pd
import pytest

import us_stock_multifactor_screener as multi
from kline_algo import calculate_kline_signals


SCREEN_QUERY = "美股里，帮我筛选，前期跌幅大、最近技术面出现止跌转折、且期权波动率还偏高的"


def _stock_rows(symbol="AAA", periods=100, last_volume=1000):
    dates = pd.bdate_range("2026-01-01", periods=periods)
    closes = [120.0] * 35 + [100.0] * (periods - 35)
    volumes = [1000.0] * periods
    volumes[-1] = last_volume
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": symbol,
            "open": closes,
            "high": [value + 2.0 for value in closes],
            "low": [value - 2.0 for value in closes],
            "close": closes,
            "volume": volumes,
            "adjClose": closes,
        }
    )


def _option_history(symbol="AAA", periods=80, latest_iv=80.0):
    dates = pd.bdate_range("2026-01-01", periods=periods)
    values = [20.0 + index * 0.25 for index in range(periods)]
    values[-1] = latest_iv
    return pd.DataFrame(
        {
            "trade_date": dates.strftime("%Y%m%d"),
            "underlying": symbol,
            "atm_iv_pct": values,
            "iv_change_1d": [0.0] * periods,
            "iv_rv20_spread": [10.0] * periods,
            "total_volume": [10000.0] * periods,
        }
    )


def _algo_frame(closes, opens=None, highs=None, lows=None):
    opens = opens or closes
    highs = highs or [value + 1.0 for value in closes]
    lows = lows or [value - 1.0 for value in closes]
    return pd.DataFrame(
        {
            "date": pd.bdate_range("2026-01-01", periods=len(closes)),
            "open_price": opens,
            "high_price": highs,
            "low_price": lows,
            "close_price": closes,
        }
    )


def test_exact_customer_query_parses_to_three_deterministic_filters():
    parsed = multi.parse_us_stock_screen_query(SCREEN_QUERY)

    assert [(item.metric, item.operator, item.value) for item in parsed.filters] == [
        ("max_drawdown_60d_pct", "lte", -20.0),
        ("reversal_confirmed", "eq", True),
        ("iv_percentile", "gte", 70.0),
    ]
    assert parsed.universe == "option_covered"
    assert any("IV偏高" in item for item in parsed.defaults)


def test_explicit_thresholds_override_qualitative_defaults():
    parsed = multi.parse_us_stock_screen_query(
        "帮我筛选美股，近120日回撤超过30%，IV Percentile超过85%，量比超过2"
    )

    assert ("max_drawdown_120d_pct", "lte", -30.0) in [
        (item.metric, item.operator, item.value) for item in parsed.filters
    ]
    assert ("iv_percentile", "gte", 85.0) in [
        (item.metric, item.operator, item.value) for item in parsed.filters
    ]
    assert ("volume_ratio_20d", "gte", 2.0) in [
        (item.metric, item.operator, item.value) for item in parsed.filters
    ]


@pytest.mark.parametrize(
    "pattern",
    ["多头吞噬", "5日平台突破", "假跌破(诱空)", "破底翻(两根确认)", "破底翻(三根确认)"],
)
def test_strong_kline_patterns_enter_strict_reversal_bucket(monkeypatch, pattern):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": [pattern], "trends": [], "score": 80})
    stock = _stock_rows()
    stock.loc[stock.index[-1], ["open", "high", "low", "close", "adjClose"]] = [99.0, 112.0, 98.0, 110.0, 110.0]
    features, warning = multi.build_us_stock_feature_frame(stock, trade_date=stock["date"].max())

    assert warning == ""
    assert bool(features.iloc[0]["reversal_confirmed"])
    assert pattern in features.iloc[0]["strict_patterns"]
    assert features.iloc[0]["key_level"] is not None


def test_secondary_hammer_waits_without_price_or_volume_confirmation(monkeypatch):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": ["锤子线(长下影)"], "trends": [], "score": 60})
    stock = _stock_rows(last_volume=1000)
    features, warning = multi.build_us_stock_feature_frame(stock, trade_date=stock["date"].max())

    assert warning == ""
    assert not bool(features.iloc[0]["reversal_confirmed"])
    assert "锤子线" in features.iloc[0]["waiting_patterns"]


def test_secondary_hammer_is_confirmed_by_volume(monkeypatch):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": ["锤子线(长下影)"], "trends": [], "score": 60})
    stock = _stock_rows(last_volume=2000)
    features, warning = multi.build_us_stock_feature_frame(stock, trade_date=stock["date"].max())

    assert warning == ""
    assert bool(features.iloc[0]["reversal_confirmed"])
    assert "锤子线" in features.iloc[0]["strict_patterns"]


def test_iv_percentile_requires_minimum_history(monkeypatch):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": ["多头吞噬"], "trends": [], "score": 80})
    stock = _stock_rows(periods=100)
    short_history = _option_history(periods=30)
    short_history["trade_date"] = stock["date"].tail(30).dt.strftime("%Y%m%d").tolist()
    features, warning = multi.build_us_stock_feature_frame(
        stock,
        short_history,
        trade_date=stock["date"].max(),
    )

    assert warning == ""
    assert int(features.iloc[0]["iv_history_samples"]) == 30
    assert pd.isna(features.iloc[0]["iv_percentile"])


def test_strict_intersection_and_zero_result_near_miss_are_separate():
    parsed = multi.parse_us_stock_screen_query(SCREEN_QUERY)
    features = pd.DataFrame(
        [
            {
                "symbol": "AAA", "latest_price": 20.0, "max_drawdown_60d_pct": -25.0,
                "reversal_confirmed": True, "strict_patterns": "多头吞噬", "waiting_patterns": "",
                "iv_percentile": 80.0, "iv_history_samples": 252, "volume_ratio_20d": 1.2,
            },
            {
                "symbol": "BBB", "latest_price": 30.0, "max_drawdown_60d_pct": -22.0,
                "reversal_confirmed": True, "strict_patterns": "假跌破(诱空)", "waiting_patterns": "",
                "iv_percentile": 65.0, "iv_history_samples": 252, "volume_ratio_20d": 1.1,
            },
        ]
    )

    evaluated = multi.evaluate_us_stock_screen(features, parsed)
    assert evaluated["strict"]["symbol"].tolist() == ["AAA"]

    for item in parsed.filters:
        if item.metric == "iv_percentile":
            item.value = 90.0
    evaluated = multi.evaluate_us_stock_screen(features, parsed)
    assert evaluated["strict"].empty
    assert evaluated["near"]["symbol"].tolist()[0] == "AAA"
    assert len(evaluated["near"]) == 2
    assert evaluated["near"].iloc[0]["未满足条件"] == "IV Percentile"


def test_unsupported_dimension_is_reported_not_ignored():
    parsed = multi.parse_us_stock_screen_query("帮我筛选美股，市盈率低且IV偏高")
    assert "估值" in parsed.unsupported
    assert any(item.metric == "iv_percentile" for item in parsed.filters)


def test_shared_kline_algo_detects_bullish_engulfing():
    closes = [100.0] * 25 + [80.0] * 13 + [75.0, 90.0]
    opens = closes.copy()
    opens[-2] = 78.0
    opens[-1] = 74.0
    highs = [value + 1.0 for value in closes]
    lows = [value - 1.0 for value in closes]
    highs[-2] = 79.0
    highs[-1] = 91.0
    lows[-1] = 73.0

    result = calculate_kline_signals(_algo_frame(closes, opens=opens, highs=highs, lows=lows))
    assert "多头吞噬" in result["patterns"]


def test_shared_kline_algo_detects_platform_breakout_and_false_breakdown():
    closes = [100.0] * 39 + [103.0]
    opens = [100.0] * 40
    highs = [101.0] * 39 + [103.5]
    lows = [99.0] * 39 + [99.8]
    breakout = calculate_kline_signals(_algo_frame(closes, opens=opens, highs=highs, lows=lows))
    assert any("平台突破" in item for item in breakout["patterns"])

    closes = [100.0] * 38 + [98.0, 100.5]
    opens = [100.0] * 38 + [100.0, 98.5]
    highs = [101.0] * 38 + [100.0, 101.0]
    lows = [99.0] * 38 + [97.5, 98.0]
    false_break = calculate_kline_signals(_algo_frame(closes, opens=opens, highs=highs, lows=lows))
    assert any("假跌破" in item for item in false_break["patterns"])


def test_shared_kline_algo_flat_market_has_no_strong_reversal():
    result = calculate_kline_signals(_algo_frame([100.0] * 40))
    assert not any(any(token in item for token in multi.STRONG_PATTERN_TOKENS) for item in result["patterns"])


def test_relative_volume_natural_language_is_not_silently_dropped():
    parsed = multi.parse_us_stock_screen_query(
        "筛选最近出现假跌破或破底翻，且成交量超过20日均量1.5倍的美股"
    )

    assert ("patterns_any", "contains_any", ["假跌破", "破底翻"]) in [
        (item.metric, item.operator, item.value) for item in parsed.filters
    ]
    assert ("volume_ratio_20d", "gte", 1.5) in [
        (item.metric, item.operator, item.value) for item in parsed.filters
    ]


def _nested_volume_pattern_plan(limit=10):
    return multi.ScreenPlan(
        where=multi.ScreenExpression(
            kind="all",
            clauses=[
                multi.ScreenExpression(
                    kind="any",
                    clauses=[
                        multi.ScreenExpression(kind="condition", metric="pattern", operator="eq", value="假跌破", source_text="假跌破"),
                        multi.ScreenExpression(kind="condition", metric="pattern", operator="eq", value="破底翻", source_text="破底翻"),
                    ],
                ),
                multi.ScreenExpression(
                    kind="condition", metric="volume_ratio", operator="gte", value=1.5,
                    window=20, unit="ratio", source_text="成交量超过20日均量1.5倍",
                ),
            ],
        ),
        limit=limit,
        confidence=0.98,
    )


def test_nested_screen_plan_preserves_pattern_or_volume_and_logic():
    parsed = multi.validate_screen_plan(
        _nested_volume_pattern_plan(),
        "筛选最近出现假跌破或破底翻，且成交量超过20日均量1.5倍的美股",
    )
    assert parsed.ambiguous == []
    features = pd.DataFrame(
        [
            {"symbol": "AAA", "strict_patterns": "假跌破(诱空)", "volume_ratio_20d": 1.6},
            {"symbol": "BBB", "strict_patterns": "破底翻(两根确认)", "volume_ratio_20d": 1.2},
            {"symbol": "CCC", "strict_patterns": "多头吞噬", "volume_ratio_20d": 2.0},
        ]
    )

    evaluated = multi.evaluate_us_stock_screen(features, parsed)
    assert evaluated["strict"]["symbol"].tolist() == ["AAA"]
    assert evaluated["strict_total"] == 1


def test_unknown_metric_and_changed_explicit_threshold_are_blocked():
    unknown = multi.ScreenPlan(
        where=multi.ScreenExpression(
            kind="condition", metric="sql_formula", operator="gte", value=1,
            source_text="执行SQL并筛选",
        )
    )
    parsed = multi.validate_screen_plan(unknown, "执行SQL并筛选美股")
    assert any("不支持的筛选指标" in item for item in parsed.ambiguous)

    changed = _nested_volume_pattern_plan()
    changed.where.clauses[-1].value = 2.0
    parsed = multi.validate_screen_plan(
        changed,
        "筛选最近出现假跌破或破底翻，且成交量超过20日均量1.5倍的美股",
    )
    assert any("阈值与用户原文不一致" in item for item in parsed.ambiguous)


def test_strict_total_is_computed_before_display_limit():
    parsed = multi.ParsedScreenRequest(
        filters=[multi.USStockFilter(metric="volume_ratio_20d", operator="gte", value=1.5, label="20日量比")],
        limit=1,
    )
    features = pd.DataFrame(
        [
            {"symbol": "AAA", "volume_ratio_20d": 3.0},
            {"symbol": "BBB", "volume_ratio_20d": 2.0},
            {"symbol": "CCC", "volume_ratio_20d": 1.6},
        ]
    )
    evaluated = multi.evaluate_us_stock_screen(features, parsed)
    assert evaluated["strict_total"] == 3
    assert evaluated["displayed_count"] == 1


def test_iv_exclusion_statistics_are_split_by_reason():
    parsed = multi.parse_us_stock_screen_query(SCREEN_QUERY)
    features = pd.DataFrame(
        [
            {"symbol": "OK", "iv_history_samples": 252, "atm_iv_pct": 50, "iv_percentile": 80, "max_drawdown_60d_pct": -25, "reversal_confirmed": True},
            {"symbol": "NONE", "iv_history_samples": None, "atm_iv_pct": None, "iv_percentile": None, "max_drawdown_60d_pct": -25, "reversal_confirmed": True},
            {"symbol": "MISSING", "iv_history_samples": 0, "atm_iv_pct": None, "iv_percentile": None, "max_drawdown_60d_pct": -25, "reversal_confirmed": True},
            {"symbol": "SHORT", "iv_history_samples": 55, "atm_iv_pct": 40, "iv_percentile": None, "max_drawdown_60d_pct": -25, "reversal_confirmed": True},
        ]
    )
    evaluated = multi.evaluate_us_stock_screen(features, parsed)
    assert evaluated["no_iv_record_rows"] == 1
    assert evaluated["current_iv_missing_rows"] == 1
    assert evaluated["insufficient_iv_rows"] == 2
    assert evaluated["insufficient_valid_iv_rows"] == 1
    assert evaluated["iv_eligible_rows"] == 1
    assert evaluated["missing_rows"] == 3


def test_secondary_confirmation_includes_signal_date_and_basis(monkeypatch):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": ["锤子线(长下影)"], "trends": [], "score": 60})
    stock = _stock_rows(last_volume=2000)
    features, warning = multi.build_us_stock_feature_frame(stock, trade_date=stock["date"].max())
    row = features.iloc[0]
    assert warning == ""
    assert row["signal_date"]
    assert "放量确认" in row["confirmation_basis"]


def test_llm_compiler_reads_screen_plan_tool_call_and_preserves_limit():
    expected = _nested_volume_pattern_plan(limit=10)

    class BoundCompiler:
        def invoke(self, messages):
            assert "不能" in messages[0].content or "禁止" in messages[0].content
            payload = expected.model_dump() if hasattr(expected, "model_dump") else expected.dict()
            return type(
                "RawToolMessage",
                (),
                {"tool_calls": [{"name": "ScreenPlan", "args": payload}]},
            )()

    class FakeCompiler:
        model_name = "fake-screen-compiler"

        def bind_tools(self, tools, *, tool_choice):
            assert tools == [multi.ScreenPlan]
            assert tool_choice == {"type": "function", "function": {"name": "ScreenPlan"}}
            return BoundCompiler()

    outcome = multi.compile_screen_plan_with_llm("筛选美股", FakeCompiler(), limit=7)

    assert outcome.status == "success"
    assert outcome.error == ""
    assert outcome.has_tool_call
    assert outcome.model == "fake-screen-compiler"
    assert outcome.plan is not None
    assert outcome.plan.limit == 7


def test_llm_compiler_no_tool_call_falls_back_without_validating_none(monkeypatch):
    real_model_validate = multi._model_validate

    def reject_none(model_cls, value):
        assert value is not None, "missing tool calls must be handled before Pydantic validation"
        return real_model_validate(model_cls, value)

    monkeypatch.setattr(multi, "_model_validate", reject_none)

    class BoundCompiler:
        def invoke(self, _messages):
            return type("RawTextMessage", (), {"tool_calls": []})()

    class FakeCompiler:
        model_name = "fake-screen-compiler"

        def bind_tools(self, _tools, *, tool_choice):
            assert tool_choice["function"]["name"] == "ScreenPlan"
            return BoundCompiler()

    outcome = multi.compile_screen_plan_with_llm("筛选美股，RSI14低于30", FakeCompiler())

    assert outcome.status == "no_tool_call"
    assert outcome.plan is None
    assert not outcome.has_tool_call
    assert "未返回ScreenPlan工具调用" in outcome.error


@pytest.mark.parametrize(
    ("raw_message", "expected_status"),
    [
        (type("BadArgsMessage", (), {"tool_calls": [{"name": "ScreenPlan", "args": {"where": None}}]})(), "parse_error"),
        (TimeoutError("compiler timed out"), "timeout"),
    ],
)
def test_llm_compiler_parse_error_and_timeout_do_not_retry(raw_message, expected_status):
    calls = []

    class BoundCompiler:
        def invoke(self, _messages):
            calls.append(1)
            if isinstance(raw_message, Exception):
                raise raw_message
            return raw_message

    class FakeCompiler:
        model_name = "fake-screen-compiler"

        def bind_tools(self, _tools, *, tool_choice):
            assert tool_choice["function"]["name"] == "ScreenPlan"
            return BoundCompiler()

    outcome = multi.compile_screen_plan_with_llm("筛选美股，RSI14低于30", FakeCompiler())

    assert outcome.status == expected_status
    assert outcome.plan is None
    assert calls == [1]


def test_llm_compiler_attaches_only_matching_registered_fuzzy_defaults():
    payload = {
        "where": {
            "kind": "all",
            "clauses": [
                {
                    "kind": "condition", "metric": "max_drawdown", "operator": "lte",
                    "value": -20, "window": 60, "unit": "percent", "source_text": "前期跌幅大",
                },
                {
                    "kind": "condition", "metric": "reversal_confirmed", "operator": "eq",
                    "value": "true", "unit": "boolean", "source_text": "最近出现止跌转折",
                },
                {
                    "kind": "condition", "metric": "iv_percentile", "operator": "gte",
                    "value": 70, "unit": "percent", "source_text": "期权波动率偏高",
                },
            ],
        },
        "confidence": 0.99,
    }

    class BoundCompiler:
        def invoke(self, _messages):
            return type("RawToolMessage", (), {"tool_calls": [{"name": "ScreenPlan", "args": payload}]})()

    class FakeCompiler:
        model_name = "fake-screen-compiler"

        def bind_tools(self, _tools, *, tool_choice):
            assert tool_choice["function"]["name"] == "ScreenPlan"
            return BoundCompiler()

    outcome = multi.compile_screen_plan_with_llm(
        "美股里帮我筛选前期跌幅大、最近出现止跌转折、期权波动率偏高的",
        FakeCompiler(),
    )

    assert outcome.status == "success"
    assert outcome.plan is not None
    assert outcome.plan.defaults_used == [
        multi.METRIC_REGISTRY["max_drawdown"].default_rule,
        multi.METRIC_REGISTRY["reversal_confirmed"].default_rule,
        multi.METRIC_REGISTRY["iv_percentile"].default_rule,
    ]


def test_fuzzy_default_cannot_claim_public_rule_with_changed_threshold():
    plan = multi.ScreenPlan(
        where=multi.ScreenExpression(
            kind="condition",
            metric="max_drawdown",
            operator="lte",
            value=-30,
            window=60,
            unit="percent",
            source_text="前期跌幅大",
        ),
        defaults_used=[multi.METRIC_REGISTRY["max_drawdown"].default_rule],
    )

    parsed = multi.validate_screen_plan(plan, "筛选美股，前期跌幅大")

    assert any("与公开默认口径不一致" in item for item in parsed.ambiguous)


def test_non_pattern_feature_request_never_calls_pattern_engine(monkeypatch):
    def forbidden_pattern_state(_group):
        raise AssertionError("non-pattern screens must not evaluate K-line patterns")

    monkeypatch.setattr(multi, "_pattern_state", forbidden_pattern_state)
    stock = _stock_rows(periods=100)
    filters = [
        multi.USStockFilter(
            metric="max_drawdown_60d_pct",
            operator="lte",
            value=-20.0,
            label="60日最大回撤",
        )
    ]

    features, warning = multi.build_us_stock_feature_frame(
        stock,
        trade_date=stock["date"].max(),
        filters=filters,
    )

    assert warning == ""
    assert features["symbol"].tolist() == ["AAA"]


def test_pattern_feature_request_invokes_pattern_engine(monkeypatch):
    calls = []

    def fake_pattern_state(group):
        calls.append(str(group.iloc[-1]["symbol"]))
        return {
            "strict_patterns": "假跌破(诱空)",
            "recent_patterns": "假跌破(诱空)",
            "waiting_patterns": "",
            "reversal_confirmed": True,
        }

    monkeypatch.setattr(multi, "_pattern_state", fake_pattern_state)
    stock = _stock_rows(periods=100)
    filters = [
        multi.USStockFilter(
            metric="patterns_any",
            operator="contains_any",
            value=["假跌破"],
            label="K线形态",
        )
    ]

    features, warning = multi.build_us_stock_feature_frame(
        stock,
        trade_date=stock["date"].max(),
        filters=filters,
    )

    assert warning == ""
    assert calls == ["AAA"]
    assert features.iloc[0]["strict_patterns"] == "假跌破(诱空)"


def test_default_ranking_uses_requested_condition_margin_not_unrequested_factors():
    parsed = multi.ParsedScreenRequest(
        filters=[multi.USStockFilter(metric="rsi14", operator="gte", value=50.0, label="RSI14")],
        limit=2,
    )
    features = pd.DataFrame(
        [
            {
                "symbol": "MOST_RELEVANT",
                "rsi14": 90.0,
                "iv_percentile": 0.0,
                "max_drawdown_60d_pct": 0.0,
                "volume_ratio_20d": 0.0,
                "reversal_confirmed": False,
            },
            {
                "symbol": "GENERIC_SCORE_DISTRACTOR",
                "rsi14": 60.0,
                "iv_percentile": 100.0,
                "max_drawdown_60d_pct": -80.0,
                "volume_ratio_20d": 4.0,
                "reversal_confirmed": True,
            },
        ]
    )

    evaluated = multi.evaluate_us_stock_screen(features, parsed)

    assert evaluated["strict"]["symbol"].tolist() == [
        "MOST_RELEVANT",
        "GENERIC_SCORE_DISTRACTOR",
    ]


def test_explicit_sort_by_still_overrides_requested_condition_relevance():
    parsed = multi.ParsedScreenRequest(
        filters=[multi.USStockFilter(metric="rsi14", operator="gte", value=50.0, label="RSI14")],
        sort_by="latest_price",
        sort_order="asc",
        limit=2,
    )
    features = pd.DataFrame(
        [
            {"symbol": "HIGH_MARGIN", "rsi14": 90.0, "latest_price": 20.0},
            {"symbol": "LOW_PRICE", "rsi14": 60.0, "latest_price": 10.0},
        ]
    )

    evaluated = multi.evaluate_us_stock_screen(features, parsed)

    assert evaluated["strict"]["symbol"].tolist() == ["LOW_PRICE", "HIGH_MARGIN"]


def test_incomplete_fallback_parse_blocks_before_database_access():
    output = multi.run_us_stock_screen(query="筛选美股，成交量像火箭一样突然暴增到天际", engine=None)
    assert "无法完整" in output
    assert "成交量条件未被降级解析器识别" in output


def test_rsi_is_registered_routed_and_supported_by_rule_fallback():
    parsed = multi.parse_us_stock_screen_query("筛选美股，RSI14低于30")
    assert [(item.metric, item.operator, item.value) for item in parsed.filters] == [("rsi14", "lte", 30.0)]
    assert multi.is_us_multifactor_screen_query("筛选美股，RSI14低于30")
    spec = multi.METRIC_REGISTRY["rsi"]
    assert spec.required_columns == ("close",)
    assert spec.feature_template == "rsi{window}"


def test_result_formatter_uses_compact_cards_and_hides_internal_confirmation_fields():
    parsed = multi.ParsedScreenRequest(
        filters=[
            multi.USStockFilter(metric="max_drawdown_60d_pct", operator="lte", value=-20, label="近60日最大回撤"),
            multi.USStockFilter(metric="reversal_confirmed", operator="eq", value=True, label="已确认止跌转折"),
            multi.USStockFilter(metric="iv_percentile", operator="gte", value=70, label="IV Percentile"),
        ],
    )
    strict = pd.DataFrame([
        {
            "symbol": "USO", "latest_price": 117.79, "max_drawdown_60d_pct": -32.5,
            "reversal_confirmed": True, "iv_percentile": 70.2, "atm_iv_pct": 52.6,
            "strict_patterns": "5日平台突破；小区间突破", "waiting_patterns": "锤子线(长下影)",
            "signal_date": "2026-07-13", "key_level": 115.0,
            "confirmation_basis": "这是一段不应进入结果卡片的长确认文字",
            "waiting_basis": "等待收盘突破信号高点",
            "命中条件": "近60日最大回撤；已确认止跌转折；IV Percentile",
        }
    ])
    output = multi.format_us_stock_screen_result(
        parsed,
        {
            "strict": strict, "waiting": strict, "near": pd.DataFrame(), "strict_total": 1,
            "displayed_count": 1, "feature_rows": 141, "evaluated_rows": 59,
            "iv_eligible_rows": 59, "missing_rows": 82, "no_iv_record_rows": 79,
            "current_iv_missing_rows": 2, "insufficient_valid_iv_rows": 1,
        },
        trade_date="2026-07-13",
        source_universe_count=142,
    )

    assert "1. **USO.US** · 现价 117.79" in output
    assert "指标：60日最大回撤 -32.5%｜IV Percentile 70.2%" in output
    assert "已确认形态：5日平台突破；小区间突破" in output
    assert "信号日 2026-07-13｜关键位 115.00" in output
    assert "| 代码" not in output
    assert "reversal_confirmed" not in output
    assert "待确认形态" not in output
    assert "确认依据" not in output
    assert "等待条件" not in output
    assert "次级K线信号等待确认" not in output
    assert "命中条件" not in output
    assert "ATM IV" not in output


def test_near_miss_cards_show_only_requested_metrics_and_missing_condition():
    parsed = multi.ParsedScreenRequest(
        filters=[
            multi.USStockFilter(metric="patterns_any", operator="contains_any", value=["假跌破", "破底翻"], label="K线形态"),
            multi.USStockFilter(metric="volume_ratio_20d", operator="gte", value=1.5, label="20日量比"),
        ]
    )
    near = pd.DataFrame([
        {
            "symbol": "USO", "latest_price": 117.79, "return_20d_pct": -8.6,
            "volume_ratio_20d": 2.13, "strict_patterns": "5日平台突破",
            "signal_date": "2026-07-13", "key_level": 115.0, "未满足条件": "K线形态",
        }
    ])
    output = multi.format_us_stock_screen_result(
        parsed,
        {
            "strict": pd.DataFrame(), "waiting": pd.DataFrame(), "near": near,
            "strict_total": 0, "displayed_count": 0, "feature_rows": 141, "evaluated_rows": 141,
        },
        trade_date="2026-07-13",
        source_universe_count=142,
    )

    assert "指标：20日量比 2.13x" in output
    assert "命中形态：5日平台突破" in output
    assert "未满足：K线形态" in output
    assert "20日涨跌幅" not in output
    assert "| 代码" not in output


def test_us_pool_query_fallback_preserves_all_explicit_thresholds_and_pattern_or():
    query = (
        "从美股池里找过去60个交易日最大回撤至少25%，RSI14不高于40，"
        "今天成交量达到此前30日均量2倍，同时假跌破或多头吞噬命中一个即可，只看前5只"
    )
    parsed = multi.parse_us_stock_screen_query(query, limit=5)

    assert parsed.ambiguous == []
    assert parsed.limit == 5
    assert [(item.metric, item.operator, item.value) for item in parsed.filters] == [
        ("max_drawdown_60d_pct", "lte", -25.0),
        ("patterns_any", "contains_any", ["多头吞噬", "假跌破"]),
        ("volume_ratio_30d", "gte", 2.0),
        ("rsi14", "lte", 40.0),
    ]


def test_explicit_pattern_cards_fall_back_to_recent_matched_pattern():
    parsed = multi.ParsedScreenRequest(
        filters=[
            multi.USStockFilter(metric="patterns_any", operator="contains_any", value=["假跌破", "多头吞噬"], label="K线形态"),
            multi.USStockFilter(metric="volume_ratio_30d", operator="gte", value=2.0, label="30日量比"),
        ]
    )
    near = pd.DataFrame([
        {
            "symbol": "SPCE", "latest_price": 2.42, "volume_ratio_30d": 0.28,
            "strict_patterns": "", "recent_patterns": "假跌破(诱空)", "key_level": 2.47,
            "未满足条件": "30日量比",
        }
    ])

    output = multi._display_candidates(near, parsed=parsed, include_failure=True)

    assert "命中形态：假跌破(诱空)" in output
    assert "已确认形态" not in output


def _split_history(symbol: str, *, ratio: float = 10.0, periods: int = 100) -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-01", periods=periods)
    split_at = periods - 10
    adjusted_close = [100.0] * split_at + [110.0] * (periods - split_at)
    raw_close = [value * ratio for value in adjusted_close[:split_at]] + adjusted_close[split_at:]
    raw_volume = [1000.0] * split_at + [1000.0 * ratio] * (periods - split_at)
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": symbol,
            "open": raw_close,
            "high": [value * 1.01 for value in raw_close],
            "low": [value * 0.99 for value in raw_close],
            "close": raw_close,
            "volume": raw_volume,
            "adjClose": adjusted_close,
        }
    )


@pytest.mark.parametrize("ratio", [4.0, 10.0, 25.0])
def test_trusted_split_factor_adjusts_price_and_volume_without_fake_drawdown(monkeypatch, ratio):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": [], "trends": [], "score": 50})
    stock = _split_history("SPLIT", ratio=ratio)

    features, warning = multi.build_us_stock_feature_frame(stock, trade_date=stock["date"].max())

    assert warning == ""
    row = features.iloc[0]
    assert float(row["max_drawdown_60d_pct"]) > -20.0
    assert float(row["volume_ratio_20d"]) == pytest.approx(1.0)
    assert features.attrs["quality_stats"]["split_adjusted_symbols"] == ["SPLIT"]
    assert features.attrs["quality_stats"]["price_anomaly_symbols"] == []


def test_trusted_reverse_split_adjusts_historical_volume_down(monkeypatch):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": [], "trends": [], "score": 50})
    dates = pd.bdate_range("2026-01-01", periods=100)
    split_at = 90
    adjusted_close = [100.0] * 100
    raw_close = [10.0] * split_at + [100.0] * 10
    stock = pd.DataFrame(
        {
            "date": dates,
            "symbol": "REVERSE",
            "open": raw_close,
            "high": [value * 1.01 for value in raw_close],
            "low": [value * 0.99 for value in raw_close],
            "close": raw_close,
            "volume": [10000.0] * split_at + [1000.0] * 10,
            "adjClose": adjusted_close,
        }
    )

    features, warning = multi.build_us_stock_feature_frame(stock, trade_date=dates.max())

    assert warning == ""
    assert float(features.iloc[0]["volume_ratio_20d"]) == pytest.approx(1.0)
    assert features.attrs["quality_stats"]["split_adjusted_symbols"] == ["REVERSE"]


def test_unadjusted_split_scale_break_is_excluded_and_reported(monkeypatch):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": [], "trends": [], "score": 50})
    broken = _split_history("BROKEN", ratio=10.0)
    broken["adjClose"] = broken["close"]
    clean = _stock_rows(symbol="CLEAN", periods=100)
    stock = pd.concat([broken, clean], ignore_index=True)

    features, warning = multi.build_us_stock_feature_frame(stock, trade_date=clean["date"].max())

    assert warning == ""
    assert features["symbol"].tolist() == ["CLEAN"]
    quality = features.attrs["quality_stats"]
    assert quality["price_history_rows"] == 2
    assert quality["price_anomaly_symbols"] == ["BROKEN"]

    parsed = multi.ParsedScreenRequest(
        filters=[multi.USStockFilter(metric="latest_price", operator="gte", value=0, label="最新价")]
    )
    evaluated = multi.evaluate_us_stock_screen(features, parsed)
    output = multi.format_us_stock_screen_result(
        parsed,
        evaluated,
        trade_date=clean["date"].max(),
        source_universe_count=2,
    )
    assert "价格历史充足2只 → 实际参与1只" in output
    assert "未确认价格断层排除1只（BROKEN.US）" in output


def test_one_day_tenfold_bad_tick_is_quarantined_not_treated_as_split(monkeypatch):
    monkeypatch.setattr(multi, "calculate_kline_signals", lambda _df: {"patterns": [], "trends": [], "score": 50})
    bad = _stock_rows(symbol="BK", periods=100)
    bad.loc[bad.index[-5], ["open", "high", "low", "close", "adjClose"]] = [10.0, 10.2, 9.8, 10.0, 10.0]
    clean = _stock_rows(symbol="CLEAN", periods=100)

    features, warning = multi.build_us_stock_feature_frame(
        pd.concat([bad, clean], ignore_index=True),
        trade_date=clean["date"].max(),
    )

    assert warning == ""
    assert "BK" not in set(features["symbol"])
    assert features.attrs["quality_stats"]["price_anomaly_symbols"] == ["BK"]


def test_large_real_move_is_never_silently_rewritten_as_a_split():
    stock = _stock_rows(symbol="CRASH", periods=100)
    stock.loc[stock.index[-1], ["open", "high", "low", "close", "adjClose"]] = [10.0, 10.5, 9.5, 10.0, 10.0]

    normalized, warning = multi._normalize_stock_history(stock)

    assert warning == ""
    assert float(normalized.iloc[-1]["close"]) == 10.0
    assert bool(normalized.iloc[-1]["_unresolved_price_break"])


def test_cash_dividend_adjustment_does_not_rescale_volume():
    stock = _stock_rows(symbol="DIV", periods=100)
    stock.loc[stock.index[:-1], "adjClose"] = stock.loc[stock.index[:-1], "close"] * 0.98
    stock["volume"] = range(1000, 1100)

    normalized, warning = multi._normalize_stock_history(stock)

    assert warning == ""
    assert normalized["volume"].tolist() == stock["volume"].astype(float).tolist()
    assert normalized.attrs["quality_stats"]["split_adjusted_symbols"] == []
