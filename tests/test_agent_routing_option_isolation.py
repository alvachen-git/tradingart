import agent_core


def test_option_position_query_removes_portfolio_analyst():
    plan = ["portfolio_analyst", "monitor", "strategist"]
    out = agent_core._enforce_option_portfolio_isolation("创业板期权持仓怎么调？", plan)
    assert "portfolio_analyst" not in out
    assert out[:2] == ["analyst", "strategist"]


def test_explicit_stock_portfolio_coupling_keeps_portfolio_analyst():
    plan = ["portfolio_analyst", "monitor"]
    out = agent_core._enforce_option_portfolio_isolation("结合我的股票持仓做期权对冲建议", plan)
    assert "portfolio_analyst" in out
    assert out[:2] == ["analyst", "strategist"]


def test_non_option_query_keeps_plan_unchanged():
    plan = ["researcher", "macro_analyst"]
    out = agent_core._enforce_option_portfolio_isolation("美联储什么时候降息", plan)
    assert out == plan


def test_option_position_query_detection():
    assert agent_core._is_option_position_query("创业板期权持仓怎么调？")
    assert agent_core._is_option_position_query("创业板4月3.2认购买方23张，目前这个持仓怎么调整比较好")
    assert not agent_core._is_option_position_query("创业板指数今天涨跌多少")


def test_generalist_uses_mid_tier_for_simple_compare():
    tier = agent_core._select_generalist_model_tier({
        "user_query": "比较一下宁德时代和阳光电源谁更强",
        "is_followup": False,
        "recent_context": "",
        "memory_context": "",
    })
    assert tier == "mid"


def test_generalist_uses_smart_tier_for_chart_requests():
    tier = agent_core._select_generalist_model_tier({
        "user_query": "帮我画一下黄金和白银的价差图",
        "is_followup": False,
        "recent_context": "",
        "memory_context": "",
    })
    assert tier == "smart"


def test_generalist_uses_smart_tier_for_followup_with_context():
    tier = agent_core._select_generalist_model_tier({
        "user_query": "那为什么",
        "is_followup": True,
        "recent_context": "用户: 比较黄金和白银\nAI: 黄金强于白银，主因在宏观预期差。",
        "memory_context": "",
    })
    assert tier == "smart"


def test_strip_stock_portfolio_sections_for_option_priority():
    raw = "【技术分析】趋势偏强\n【持仓分析】股票组合风险中等\n- 前3大持仓...\n【期权策略】建议牛市价差"
    out = agent_core._strip_stock_portfolio_sections(raw)
    assert "【持仓分析】" not in out
    assert "【技术分析】" in out and "【期权策略】" in out


def test_strategist_node_prepends_delta_cash_block(monkeypatch):
    delta_report = "### 【DeltaCash】\n- Total Delta Cash: `123,000` 元"

    class _DummyAgent:
        def invoke(self, *_args, **_kwargs):
            class _Msg:
                content = "策略正文"
            return {"messages": [_Msg()]}

    monkeypatch.setattr(
        agent_core,
        "compute_option_delta_cash",
        lambda **_kwargs: {
            "is_etf": True,
            "asset_class": "etf",
            "report": delta_report,
            "publishable": True,
        },
    )
    monkeypatch.setattr(agent_core, "create_react_agent", lambda *_args, **_kwargs: _DummyAgent())

    state = {
        "symbol": "159915",
        "user_query": "创业板4月3.2认购买方23张，目前这个持仓怎么调整比较好",
        "risk_preference": "稳健型",
        "fund_data": "无",
        "trend_signal": "看涨",
        "memory_context": "",
        "technical_summary": "趋势偏强",
        "key_levels": "",
        "portfolio_top_corr_index": "",
        "portfolio_top_corr_value": "",
        "portfolio_summary": "",
        "vision_position_domain": "",
        "vision_position_payload": {},
    }
    out = agent_core.strategist_node(state, llm=object())
    content = out["messages"][0].content
    assert "【DeltaCash】" in content
    assert "策略正文" in content
    assert "1. 持仓拆解表" in content
    assert "6. 当日执行清单" in content


def test_strategist_node_data_gap_does_not_force_delta_cash_block(monkeypatch):
    class _DummyAgent:
        def invoke(self, *_args, **_kwargs):
            class _Msg:
                content = "策略正文"
            return {"messages": [_Msg()]}

    monkeypatch.setattr(
        agent_core,
        "compute_option_delta_cash",
        lambda **_kwargs: {
            "is_etf": True,
            "asset_class": "etf",
            "report": "### 【DeltaCash】\n- 数据缺口: 未找到可用IV",
            "metrics": {"coverage_ratio": 0.0},
            "missing_notes": ["未找到可用IV数据"],
            "blocking_missing_notes": ["未找到可用IV数据"],
            "publishable": False,
        },
    )
    monkeypatch.setattr(agent_core, "create_react_agent", lambda *_args, **_kwargs: _DummyAgent())

    state = {
        "symbol": "510300",
        "user_query": "我有510300的4月4.6认购买方23张，目前这个持仓怎么调比较好",
        "risk_preference": "稳健型",
        "fund_data": "无",
        "trend_signal": "看涨",
        "memory_context": "",
        "technical_summary": "趋势偏强",
        "key_levels": "",
        "portfolio_top_corr_index": "",
        "portfolio_top_corr_value": "",
        "portfolio_summary": "",
        "vision_position_domain": "",
        "vision_position_payload": {},
    }
    out = agent_core.strategist_node(state, llm=object())
    content = out["messages"][0].content
    assert "### 【DeltaCash】" not in content
    assert "数据缺口" in content
    assert "暂不输出" in content


def test_strategist_node_partial_delta_still_shows_block_without_amount_action(monkeypatch):
    class _DummyAgent:
        def invoke(self, *_args, **_kwargs):
            class _Msg:
                content = "策略正文"
            return {"messages": [_Msg()]}

    partial_report = """### 【DeltaCash】
- 标的 | Total Delta Cash | 数据状态
|---|---:|---|
| 159915.SZ | 123,000 | 部分可用 |
- 覆盖率低于60%，暂不输出金额级调整量
"""
    monkeypatch.setattr(
        agent_core,
        "compute_option_delta_cash",
        lambda **_kwargs: {
            "is_etf": False,
            "asset_class": "multi",
            "report": partial_report,
            "metrics": {"coverage_ratio": 0.5},
            "portfolio_summary": {"coverage_ratio": 0.5, "displayable": True, "execution_ready": False},
            "displayable": True,
            "execution_ready": False,
            "publishable": False,
        },
    )
    monkeypatch.setattr(agent_core, "create_react_agent", lambda *_args, **_kwargs: _DummyAgent())

    state = {
        "symbol": "159915",
        "user_query": "创业板期权持仓怎么调",
        "risk_preference": "稳健型",
        "fund_data": "无",
        "trend_signal": "看涨",
        "memory_context": "",
        "technical_summary": "趋势偏强",
        "key_levels": "",
        "portfolio_top_corr_index": "",
        "portfolio_top_corr_value": "",
        "portfolio_summary": "",
        "vision_position_domain": "",
        "vision_position_payload": {},
    }
    out = agent_core.strategist_node(state, llm=object())
    content = out["messages"][0].content
    assert "### 【DeltaCash】" in content
    assert "暂不输出金额级调整量" in content
    assert out["option_delta_displayable"] is True
    assert out["option_delta_execution_ready"] is False


def test_strategist_node_fallback_extracts_account_capital_from_query(monkeypatch):
    observed = {}
    delta_report = "### 【DeltaCash】\n- Total Delta Cash: `123,000` 元"

    class _DummyAgent:
        def invoke(self, *_args, **_kwargs):
            class _Msg:
                content = "策略正文"
            return {"messages": [_Msg()]}

    def _fake_compute(**kwargs):
        observed["account_total_capital"] = kwargs.get("account_total_capital")
        return {"is_etf": True, "asset_class": "etf", "report": delta_report, "publishable": True}

    monkeypatch.setattr(agent_core, "compute_option_delta_cash", _fake_compute)
    monkeypatch.setattr(agent_core, "create_react_agent", lambda *_args, **_kwargs: _DummyAgent())

    state = {
        "symbol": "510300",
        "user_query": "我账户总资金100万，我有510300的4月4.6认购买方23张，怎么调？",
        "risk_preference": "稳健型",
        "fund_data": "无",
        "trend_signal": "看涨",
        "memory_context": "",
        "technical_summary": "趋势偏强",
        "key_levels": "",
        "portfolio_top_corr_index": "",
        "portfolio_top_corr_value": "",
        "portfolio_summary": "",
        "account_total_capital": None,
        "vision_position_domain": "",
        "vision_position_payload": {},
    }
    out = agent_core.strategist_node(state, llm=object())
    content = out["messages"][0].content
    assert "【DeltaCash】" in content
    assert observed.get("account_total_capital") == 1000000.0


def test_strategist_node_uses_vision_option_legs(monkeypatch):
    observed = {}
    delta_report = "### 【DeltaCash】\n- Total Delta Cash: `123,000` 元"

    class _DummyAgent:
        def invoke(self, *_args, **_kwargs):
            class _Msg:
                content = "策略正文"
            return {"messages": [_Msg()]}

    def _fake_compute(**kwargs):
        observed["vision_legs"] = kwargs.get("vision_legs")
        return {"is_etf": False, "asset_class": "index", "report": delta_report, "publishable": True}

    monkeypatch.setattr(agent_core, "compute_option_delta_cash", _fake_compute)
    monkeypatch.setattr(agent_core, "create_react_agent", lambda *_args, **_kwargs: _DummyAgent())

    state = {
        "symbol": "",
        "user_query": "请根据截图给我调整建议",
        "risk_preference": "稳健型",
        "fund_data": "无",
        "trend_signal": "看涨",
        "memory_context": "",
        "technical_summary": "趋势偏强",
        "key_levels": "",
        "portfolio_top_corr_index": "",
        "portfolio_top_corr_value": "",
        "portfolio_summary": "",
        "vision_position_domain": "mixed",
        "vision_position_payload": {
            "option_legs": [
                {"underlying_hint": "IO", "month": 4, "strike": 4000, "cp": "call", "side": "long", "qty": 2}
            ]
        },
    }
    out = agent_core.strategist_node(state, llm=object())
    assert observed.get("vision_legs")
    assert "【DeltaCash】" in out["messages"][0].content


def test_sanitize_option_direction_terms_maps_long_short_to_cn():
    raw = "主腿是 short call，保护腿是 long put，另有短Call和长Put。"
    out = agent_core._sanitize_option_direction_terms(raw)  # noqa: SLF001
    assert "short call" not in out.lower()
    assert "long put" not in out.lower()
    assert "卖认购" in out
    assert "买认沽" in out


def test_apply_option_fact_lock_strictly_overrides_leg_section():
    text = """1. 持仓拆解表
| 标的 | 方向 |
|---|---|
| 159915.SZ | 长Put |

2. 组合净暴露与到期错配
- 原文"""
    canonical_legs = [
        {
            "underlying_hint": "159915.SZ",
            "contract_code": "90007163.SH",
            "month": 4,
            "strike": 3.5,
            "qty": 5,
            "signed_qty": 5,
            "direction_cn": "买认购",
            "cp_cn": "认购",
            "side_cn": "买方",
        }
    ]
    out = agent_core._apply_option_fact_lock(text=text, canonical_legs=canonical_legs, strict_cover=True)  # noqa: SLF001
    locked = out["text"]
    assert "长Put" not in locked
    assert "买认购" in locked
    assert "已按识别持仓自动纠偏" in locked
    assert out["option_direction_conflict_count"] > 0


def test_validate_option_direction_consistency_detects_conflict_by_contract_code():
    canonical_legs = [
        {
            "contract_code": "90007163.SH",
            "direction_cn": "买认购",
        }
    ]
    result = agent_core._validate_option_direction_consistency(  # noqa: SLF001
        "合约90007163.SH 方向为卖认沽，后续观察。",
        canonical_legs,
    )
    assert result["conflict_count"] > 0


def test_replace_option_legs_section_replaces_holdings_breakdown_heading():
    canonical_block = "1. 持仓拆解表\n| 序号 | 标的 |\n|---:|---|\n| 1 | 159915.SZ |"
    text = """### 🎯 核心结论
- ...

### 📋 持仓拆解表 (Holdings Breakdown)
| 序号 | 标的 |
|---:|---|
| 1 | 510500.SH |

### ⚖️ 风控与对冲
- ...
"""
    out = agent_core._replace_option_legs_section(text, canonical_block)  # noqa: SLF001
    assert out.count("持仓拆解表") == 1
    assert "510500.SH" not in out
    assert "159915.SZ" in out


def test_validate_authoritative_price_consistency_detects_conflict():
    result = agent_core._validate_authoritative_price_consistency(  # noqa: SLF001
        final_text="510500.SH现价:3.008元；159915.SZ现价:3.542元",
        authoritative_quotes={
            "510500.SH": {"close_price": 8.111, "trade_date": "20260415", "source": "stock_price", "missing": False},
            "159915.SZ": {"close_price": 3.542, "trade_date": "20260415", "source": "stock_price", "missing": False},
        },
        threshold=0.01,
    )
    assert result["conflict_count"] == 1
    assert result["conflicts"][0]["ts_code"] == "510500.SH"


def test_apply_authoritative_quote_lock_overrides_conflicting_price():
    raw = "### 市场深度解析\n- 510500.SH现价:3.008元（支撑2.70，压力3.10）"
    out = agent_core._apply_authoritative_quote_lock(  # noqa: SLF001
        text=raw,
        authoritative_quotes={
            "510500.SH": {"close_price": 8.111, "trade_date": "20260415", "source": "stock_price", "missing": False}
        },
        strict_cover=False,
    )
    assert "8.111" in out["text"]
    assert "3.008" not in out["text"]
    assert out["price_conflict_count"] > 0
    assert "已按权威行情自动纠偏" in out["text"]


def test_replace_delta_cash_section_replaces_existing_block():
    raw = "### 【DeltaCash】\n- Total Delta Cash: `1` 元\n\n### 🎯 核心结论\n- ..."
    replaced = agent_core._replace_delta_cash_section(  # noqa: SLF001
        text=raw,
        delta_report="### 【DeltaCash】\n- Total Delta Cash: `123,000` 元",
    )
    assert replaced.count("### 【DeltaCash】") == 1
    assert "`123,000`" in replaced
    assert "`1`" not in replaced


def test_replace_delta_cash_section_does_not_prepend_when_missing_heading():
    raw = "### 🎯 核心结论\n- 仅保留正文"
    replaced = agent_core._replace_delta_cash_section(  # noqa: SLF001
        text=raw,
        delta_report="### 【DeltaCash】\n- Total Delta Cash: `123,000` 元",
    )
    assert replaced == raw


def test_ensure_delta_section_from_meta_inserts_when_missing():
    raw = "### 🎯 核心结论\n- 正文"
    out = agent_core._ensure_delta_section_from_meta(  # noqa: SLF001
        text=raw,
        delta_report="### 【DeltaCash】\n- Total Delta Cash: `123,000` 元",
        delta_meta={},
        delta_gap_note="",
        displayable=True,
    )
    assert out.count("### 【DeltaCash】") == 1
    assert out.endswith("### 🎯 核心结论\n- 正文")


def test_dedupe_option_position_sections_keeps_single_holdings_and_delta():
    raw = """### 【DeltaCash】
- A

### 【DeltaCash】
- B

1. 持仓拆解表
- 表A

1. 持仓拆解表
- 表B
"""
    out = agent_core._dedupe_option_position_sections(raw)  # noqa: SLF001
    assert out.count("### 【DeltaCash】") == 1
    assert out.count("1. 持仓拆解表") == 1


def test_replace_authoritative_quote_section_does_not_prepend_when_missing_heading():
    raw = "### 🎯 核心结论\n- 仅保留正文"
    replaced = agent_core._replace_authoritative_quote_section(  # noqa: SLF001
        text=raw,
        quote_block="### 标的现价（权威数据）\n\n| 标的 | 收盘价 |\n|---|---:|\n| 510500.SH | 8.111 |",
    )
    assert replaced == raw


def test_replace_option_legs_section_does_not_prepend_when_missing_heading():
    raw = "### 🎯 核心结论\n- 未出现持仓拆解表标题"
    replaced = agent_core._replace_option_legs_section(  # noqa: SLF001
        text=raw,
        canonical_block="1. 持仓拆解表\n| 序号 | 标的 |\n|---:|---|\n| 1 | 159915.SZ |",
    )
    assert replaced == raw


def test_ensure_option_position_structure_respects_synonym_headings():
    raw = """### 🎯 核心结论
- ...

### ⚖️ 持仓拆解与净暴露
- 已有持仓拆解

### 📊 三情景推演
- 已有情景

### 🛡️ 风险提示
- 已有风控

### ✅ 执行清单
- 已有清单

### 🧩 交易策略部署
- 已有策略
"""
    out = agent_core._ensure_option_position_structure(  # noqa: SLF001
        text=raw,
        delta_cash_report="",
        delta_cash_gap_note="",
        trend_signal="看涨",
        risk_preference="稳健型",
        key_levels="",
    )
    assert out.count("1. 持仓拆解表") == 0


def test_finalizer_lock_restores_missing_option_sections_without_duplicate():
    text = """【最终决策】
### 🎯 核心结论
- 仅有摘要

### 📈 市场深度解析
- 略
"""
    out = agent_core._ensure_option_position_structure(  # noqa: SLF001
        text=text,
        delta_cash_report="### 【DeltaCash】\n- Total Delta Cash: `100,000` 元",
        delta_cash_gap_note="",
        trend_signal="看涨",
        risk_preference="稳健型",
        key_levels="",
    )
    assert out.count("### 【DeltaCash】") == 1
    assert "3. 关键触发位与三情景分支" in out
    assert out.count("1. 持仓拆解表") == 1


def test_strategist_node_passes_underlying_trend_map(monkeypatch):
    observed = {}

    class _DummyAgent:
        def invoke(self, *_args, **_kwargs):
            class _Msg:
                content = "策略正文"
            return {"messages": [_Msg()]}

    def _fake_compute(**kwargs):
        observed["trend_map"] = kwargs.get("trend_map")
        return {
            "is_etf": False,
            "asset_class": "multi",
            "report": "### 【DeltaCash】\n- Total Delta Cash: `123,000` 元",
            "publishable": True,
            "per_underlying": {},
            "portfolio_summary": {},
            "risk_contribution_ranking": [],
        }

    monkeypatch.setattr(agent_core, "compute_option_delta_cash", _fake_compute)
    monkeypatch.setattr(
        agent_core,
        "fetch_underlying_spot_map",
        lambda **_kwargs: {
            "510500.SH": {"close_price": 8.111, "trade_date": "20260415", "source": "stock_price", "missing": False},
            "159915.SZ": {"close_price": 3.542, "trade_date": "20260415", "source": "stock_price", "missing": False},
        },
    )
    monkeypatch.setattr(agent_core, "create_react_agent", lambda *_args, **_kwargs: _DummyAgent())

    state = {
        "symbol": "",
        "user_query": "请根据截图给我多标的期权建议",
        "risk_preference": "稳健型",
        "fund_data": "无",
        "trend_signal": "看涨",
        "memory_context": "",
        "technical_summary": "510500.SH震荡，159915.SZ看跌",
        "key_levels": "",
        "portfolio_top_corr_index": "",
        "portfolio_top_corr_value": "",
        "portfolio_summary": "",
        "vision_position_domain": "option",
        "vision_position_payload": {
            "option_legs": [
                {"underlying_hint": "510500.SH", "month": 4, "strike": 7.0, "cp": "put", "side": "short", "qty": 2},
                {"underlying_hint": "159915.SZ", "month": 4, "strike": 3.4, "cp": "call", "side": "long", "qty": 1},
            ]
        },
    }
    agent_core.strategist_node(state, llm=object())
    assert observed["trend_map"]["510500.SH"] in {"震荡", "看涨", "看跌"}
    assert observed["trend_map"]["159915.SZ"] in {"震荡", "看涨", "看跌"}


def test_strategist_node_includes_authoritative_quote_block_for_multi_underlyings(monkeypatch):
    class _DummyAgent:
        def invoke(self, *_args, **_kwargs):
            class _Msg:
                content = "策略正文"

            return {"messages": [_Msg()]}

    monkeypatch.setattr(
        agent_core,
        "compute_option_delta_cash",
        lambda **_kwargs: {
            "is_etf": True,
            "asset_class": "etf",
            "report": "### 【DeltaCash】\n- Total Delta Cash: `123,000` 元",
            "publishable": True,
        },
    )
    monkeypatch.setattr(
        agent_core,
        "fetch_underlying_spot_map",
        lambda **_kwargs: {
            "510500.SH": {"close_price": 8.111, "trade_date": "20260415", "source": "stock_price", "missing": False},
            "159915.SZ": {"close_price": 3.542, "trade_date": "20260415", "source": "stock_price", "missing": False},
        },
    )
    monkeypatch.setattr(agent_core, "create_react_agent", lambda *_args, **_kwargs: _DummyAgent())

    state = {
        "symbol": "510500",
        "user_query": "请按截图做多标的期权持仓分析",
        "risk_preference": "稳健型",
        "fund_data": "无",
        "trend_signal": "看涨",
        "memory_context": "",
        "technical_summary": "趋势偏强",
        "key_levels": "",
        "portfolio_top_corr_index": "",
        "portfolio_top_corr_value": "",
        "portfolio_summary": "",
        "vision_position_domain": "option",
        "vision_position_payload": {
            "option_legs": [
                {"underlying_hint": "510500.SH", "month": 4, "strike": 7.0, "cp": "put", "side": "short", "qty": 2},
                {"underlying_hint": "159915.SZ", "month": 4, "strike": 3.4, "cp": "call", "side": "long", "qty": 1},
            ]
        },
    }
    out = agent_core.strategist_node(state, llm=object())
    content = out["messages"][0].content
    quote_block = out.get("authoritative_quote_block", "")
    assert "### 标的现价（权威数据）" in quote_block
    assert "510500.SH" in quote_block and "8.111" in quote_block
    assert "159915.SZ" in quote_block and "3.542" in quote_block
    assert "【DeltaCash】" in content


def test_normalize_option_section_id_maps_heading_variants():
    assert agent_core._normalize_option_section_id("1. 持仓拆解表") == "holdings"  # noqa: SLF001
    assert agent_core._normalize_option_section_id("一、持仓拆解") == "holdings"  # noqa: SLF001
    assert agent_core._normalize_option_section_id("### 【DeltaCash】") == "delta"  # noqa: SLF001
    assert agent_core._normalize_option_section_id("## Delta Cash") == "delta"  # noqa: SLF001


def test_ensure_delta_section_from_meta_recognizes_non_bracket_delta_heading():
    raw = "## DeltaCash\n- 已有Delta\n\n### 🎯 核心结论\n- 正文"
    out = agent_core._ensure_delta_section_from_meta(  # noqa: SLF001
        text=raw,
        delta_report="### 【DeltaCash】\n- Total Delta Cash: `123,000` 元",
        delta_meta={},
        delta_gap_note="",
        displayable=True,
    )
    assert out == raw


def test_dedupe_option_position_sections_handles_variant_headings():
    raw = """一、持仓拆解
- HOLD_A

### 📋 持仓拆解表
- HOLD_B

## DeltaCash
- DELTA_A

### 【DeltaCash】
- DELTA_B
"""
    out = agent_core._dedupe_option_position_sections(raw)  # noqa: SLF001
    assert "HOLD_A" in out and "HOLD_B" not in out
    assert "DELTA_A" in out and "DELTA_B" not in out


def test_compose_option_sections_filters_non_core_intel_sections():
    raw = """### 🎯 核心结论
- SUMMARY

### 情报与舆情
- NOISE_1

### 今日财经热点
- NOISE_2

### 📋 持仓拆解表
- HOLDINGS
"""
    out = agent_core._compose_option_sections(  # noqa: SLF001
        text=raw,
        structured_sections={"delta": "### 【DeltaCash】\n- DELTA"},
        keep_only_whitelist=True,
    )
    assert "SUMMARY" in out
    assert "HOLDINGS" in out
    assert "DELTA" in out
    assert "NOISE_1" not in out
    assert "NOISE_2" not in out


def test_news_impact_query_detection():
    assert agent_core._is_news_impact_query("黄金为什么涨？")
    assert agent_core._is_news_impact_query("美联储鹰派表态对A股有什么影响？")
    assert agent_core._is_news_impact_query("日本央行如果突然收紧，对美股和黄金会怎么传导？")
    assert agent_core._is_news_impact_query("美国如果非农大超预期，纳指一般先交易什么？")
    assert not agent_core._is_news_impact_query("请全面分析黄金")


def test_finalizer_uses_news_flash_template_for_news_queries(monkeypatch):
    class _DummyResp:
        content = """> 📅 日期：2026年04月24日 00:40
> ✍️ 签发：交易台CIO | 🎯 模式：事件快评

### 交易台一句话
- 黄金这波走强，主线先看避险和实际利率预期回落。

### 主线
- 市场先交易避险溢价，不是单纯追通胀。

### 盘面验证
- 金价偏强，美元和美债没有形成持续压制。

### 反向风险
- 如果避险情绪退潮，这波容易先回吐。

### 接下来盯什么
- 盯美元、10Y美债和地缘消息。

### 交易应对
- 先别追高，等回踩确认再说。"""

    class _DummyLLM:
        def invoke(self, prompt):
            assert "事件快评" in prompt
            return _DummyResp()

    state = {
        "messages": [
            agent_core.HumanMessage(
                content="【情报与舆情】\n黄金偏强，主线是避险和实际利率预期。"
            )
        ],
        "user_query": "黄金为什么涨？",
        "symbol": "AU",
        "symbol_name": "黄金",
        "risk_preference": "稳健型",
        "macro_view": "美债和美元没有形成持续压制。",
        "trend_signal": "",
        "key_levels": "",
        "memory_context": "",
        "vision_position_domain": "",
        "vision_position_payload": {},
    }

    out = agent_core.finalizer_node(state, llm=_DummyLLM())
    content = out["messages"][0].content
    assert "### 交易台一句话" in content
    assert "### 主线" in content
    assert "### 接下来盯什么" in content
    assert "Executive Summary" not in content


def test_finalizer_uses_news_flash_template_for_transmission_style_queries(monkeypatch):
    class _DummyResp:
        content = """> 📅 日期：2026年04月26日 12:20
> ✍️ 签发：交易台CIO | 🎯 模式：事件快评

### 交易台一句话
- 日本央行突然收紧，先冲击全球套息和流动性，再传到美股，黄金通常先压后稳。

### 主线
- 市场先交易日元套息平仓和风险资产去杠杆。

### 盘面验证
- 先看日元、美债和美股期货有没有同步波动放大。

### 反向风险
- 如果只是口头偏鹰、没有真正落地，回吐会很快。

### 接下来盯什么
- 盯 USDJPY、10Y 美债和纳指期货。

### 交易应对
- 别抢第一脚，先等市场把流动性冲击定价完。"""

    class _DummyLLM:
        def invoke(self, prompt):
            assert "事件快评" in prompt
            return _DummyResp()

    state = {
        "messages": [
            agent_core.HumanMessage(
                content="【情报与舆情】\n日本央行潜在收紧可能触发套息平仓。"
            )
        ],
        "user_query": "日本央行如果突然收紧，对美股和黄金会怎么传导？",
        "symbol": "NDX",
        "symbol_name": "纳指",
        "risk_preference": "稳健型",
        "macro_view": "美元流动性偏紧，长端利率高位震荡。",
        "trend_signal": "",
        "key_levels": "",
        "memory_context": "",
        "vision_position_domain": "",
        "vision_position_payload": {},
    }

    out = agent_core.finalizer_node(state, llm=_DummyLLM())
    content = out["messages"][0].content
    assert "### 交易台一句话" in content
    assert "### 盘面验证" in content
    assert "### 反向风险" in content
    assert "Executive Summary" not in content


def test_finalizer_keeps_cio_template_for_non_news_queries(monkeypatch):
    class _DummyResp:
        content = """### 🎯 核心结论
- 正常 CIO 长报告

### 📈 市场深度解析
- 这里是常规分析。

### ⚖️ 交易策略部署
- 这里是常规策略。"""

    class _DummyKnowledge:
        @staticmethod
        def invoke(_query):
            return "知识库上下文"

    class _DummyLLM:
        def invoke(self, prompt):
            assert "事件快评" not in prompt
            return _DummyResp()

    monkeypatch.setattr(agent_core, "search_investment_knowledge", _DummyKnowledge())

    state = {
        "messages": [
            agent_core.HumanMessage(content="【技术分析】\n趋势偏强。"),
            agent_core.HumanMessage(content="【数据监控】\n资金面稳定。"),
        ],
        "user_query": "请全面分析黄金",
        "symbol": "AU",
        "symbol_name": "黄金",
        "risk_preference": "稳健型",
        "macro_view": "宏观中性。",
        "trend_signal": "看涨",
        "key_levels": "",
        "memory_context": "",
        "vision_position_domain": "",
        "vision_position_payload": {},
    }

    out = agent_core.finalizer_node(state, llm=_DummyLLM())
    content = out["messages"][0].content
    assert "### 🎯 核心结论" in content
    assert "### ⚖️ 交易策略部署" in content
