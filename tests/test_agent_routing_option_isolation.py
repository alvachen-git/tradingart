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
        "compute_etf_option_delta_cash",
        lambda **_kwargs: {
            "is_etf": True,
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
        "compute_etf_option_delta_cash",
        lambda **_kwargs: {
            "is_etf": True,
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
    }
    out = agent_core.strategist_node(state, llm=object())
    content = out["messages"][0].content
    assert "### 【DeltaCash】" not in content
    assert "数据缺口" in content
    assert "暂不输出" in content


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
        return {"is_etf": True, "report": delta_report, "publishable": True}

    monkeypatch.setattr(agent_core, "compute_etf_option_delta_cash", _fake_compute)
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
    }
    out = agent_core.strategist_node(state, llm=object())
    content = out["messages"][0].content
    assert "【DeltaCash】" in content
    assert observed.get("account_total_capital") == 1000000.0
