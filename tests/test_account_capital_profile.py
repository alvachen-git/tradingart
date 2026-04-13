import data_engine as de


def test_parse_account_total_capital_with_wan_unit():
    text = "我账户总资金100万，期权仓位怎么调？"
    out = de.parse_account_total_capital(text)
    assert out == 1000000.0


def test_parse_account_total_capital_with_yi_unit():
    text = "账户资金是2.5亿，做稳健一点"
    out = de.parse_account_total_capital(text)
    assert out == 250000000.0


def test_parse_account_total_capital_ignores_non_capital_numbers():
    text = "我有4月4.6认购买方23张，怎么调整"
    out = de.parse_account_total_capital(text)
    assert out is None
