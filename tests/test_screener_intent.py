import agent_core


def test_semiconductor_recommendation_constraint_is_not_risk_query():
    query = "推荐有潜力的半导体股票，最好不要涨太多的"
    assert not agent_core._is_screener_risk_query(query)


def test_explicit_avoid_stock_query_is_risk_query():
    assert agent_core._is_screener_risk_query("哪些半导体股票风险高，应该规避")
    assert agent_core._is_screener_risk_query("这几只股票不要买")


def test_low_risk_recommendation_is_not_avoid_list_query():
    assert not agent_core._is_screener_risk_query("推荐几只低风险的好股票")
