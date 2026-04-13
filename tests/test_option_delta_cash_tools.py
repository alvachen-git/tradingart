import option_delta_tools as odt


class _FakeLoader:
    def __init__(self, with_iv=True):
        self.with_iv = with_iv

    def get_underlying_spot(self, underlying_code):
        return {
            "ts_code": underlying_code,
            "trade_date": "20260412",
            "close_price": 3.437,
        }

    def get_latest_iv(self, underlying_code):
        if not self.with_iv:
            return None
        return {
            "etf_code": underlying_code,
            "trade_date": "20260412",
            "iv": 22.0,
        }

    def find_option_contract(self, underlying_code, option_flag, strike, month, as_of_yyyymmdd):
        if abs(float(strike) - 3.2) < 1e-8 and option_flag == "c":
            return {
                "status": "ok",
                "ts_code": "159915C2604M03200",
                "delist_date": "20260424",
                "exercise_price": 3.2,
                "is_exact_strike": True,
                "trade_date": "20260412",
                "close": 0.220,
                "vol": 1000,
                "oi": 20000,
            }
        if abs(float(strike) - 3.3) < 1e-8 and option_flag == "c":
            return {
                "status": "ok",
                "ts_code": "159915C2604M03300",
                "delist_date": "20260424",
                "exercise_price": 3.3,
                "is_exact_strike": True,
                "trade_date": "20260412",
                "close": 0.160,
                "vol": 1200,
                "oi": 18000,
            }
        return None


def test_parse_etf_option_legs_buy_and_sell():
    text = "我有创业板4月3.2认购买方23张，还有3.3认购卖方50张"
    legs = odt.parse_etf_option_legs(text)
    assert len(legs) == 2
    by_strike = {round(float(x["strike"]), 3): x for x in legs}
    assert by_strike[3.2]["signed_qty"] == 23
    assert by_strike[3.3]["signed_qty"] == -50
    assert by_strike[3.2]["option_flag"] == "c"
    assert by_strike[3.3]["option_flag"] == "c"


def test_compute_delta_cash_metrics_sign_direction():
    legs = [
        {"signed_qty": 23, "delta": 0.60},
        {"signed_qty": -50, "delta": 0.55},
    ]
    out = odt.compute_delta_cash_metrics(legs, underlying_price=3.437, multiplier=10000)
    assert out["gross_notional"] > 0
    assert out["total_delta_cash"] < 0  # 净卖出认购应偏空
    assert out["delta_ratio"] < 0


def test_get_delta_target_band_aggressive_bearish():
    band = odt.get_delta_target_band(trend_signal="看跌", risk_preference="激进型")
    assert band["low"] == -2.0
    assert band["high"] == -0.6
    assert round(band["mid"], 2) == -1.30


def test_build_delta_adjustment_direction_consistency():
    # 当前过于偏空，趋势看涨时应提高Delta（减空/加多）
    out = odt.build_delta_adjustment(
        total_delta_cash=-500000,
        gross_notional=1000000,
        trend_signal="看涨",
        risk_preference="稳健型",
    )
    assert out["adjust_cash"] > 0
    assert "提高 Delta Cash" in out["action"]


def test_build_delta_adjustment_uses_account_ratio_base_when_provided():
    out = odt.build_delta_adjustment(
        total_delta_cash=200000,
        gross_notional=1000000,
        trend_signal="看涨",
        risk_preference="稳健型",
        ratio_base=5000000,
        ratio_basis="account_total_capital",
    )
    assert out["ratio_basis"] == "account_total_capital"
    assert abs(out["current_ratio"] - 0.04) < 1e-9


def test_compute_etf_option_delta_cash_success_report_contains_required_blocks():
    text = "创业板4月3.2认购买方23张，还有3.3认购卖方50张，目前这个持仓怎么调比较好"
    out = odt.compute_etf_option_delta_cash(
        user_query=text,
        symbol_hint="159915",
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_FakeLoader(with_iv=True),
        as_of_date="20260412",
    )
    assert out["is_etf"] is True
    assert len(out["legs"]) == 2
    assert "Total Delta Cash" in out["report"]
    assert "技术面目标区间" in out["report"]
    assert "建议调整量" in out["report"]
    assert out["publishable"] is True
    assert out["metrics"]["coverage_ratio"] == 1.0
    assert "未提供账户总资金" in out["report"]


def test_compute_etf_option_delta_cash_with_account_capital_uses_account_ratio():
    text = "创业板4月3.2认购买方23张，还有3.3认购卖方50张"
    out = odt.compute_etf_option_delta_cash(
        user_query=text,
        symbol_hint="159915",
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_FakeLoader(with_iv=True),
        as_of_date="20260412",
        account_total_capital=10000000,
    )
    assert out["is_etf"] is True
    assert out["publishable"] is True
    assert out["metrics"]["effective_ratio_basis"] == "account_total_capital"
    assert out["metrics"]["account_total_capital"] == 10000000
    assert "账户总资金" in out["report"]
    assert "执行口径 Delta Ratio(账户)" in out["report"]


def test_compute_etf_option_delta_cash_accepts_string_account_capital():
    text = "创业板4月3.2认购买方23张，还有3.3认购卖方50张"
    out = odt.compute_etf_option_delta_cash(
        user_query=text,
        symbol_hint="159915",
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_FakeLoader(with_iv=True),
        as_of_date="20260412",
        account_total_capital="3000000",
    )
    assert out["is_etf"] is True
    assert out["publishable"] is True
    assert out["metrics"]["effective_ratio_basis"] == "account_total_capital"
    assert abs(out["metrics"]["account_total_capital"] - 3000000.0) < 1e-9
    assert "执行口径 Delta Ratio(账户)" in out["report"]


def test_compute_etf_option_delta_cash_missing_iv_has_gap_message():
    text = "创业板4月3.2认购买方23张"
    out = odt.compute_etf_option_delta_cash(
        user_query=text,
        symbol_hint="159915",
        trend_signal="震荡",
        risk_preference="稳健型",
        loader=_FakeLoader(with_iv=False),
        as_of_date="20260412",
    )
    assert out["is_etf"] is True
    assert "数据缺口" in out["report"]
    assert any("缺现价或IV" in str(x.get("missing_reason", "")) for x in out["legs"])
    assert out["publishable"] is False
    assert any("IV" in x for x in out["blocking_missing_notes"])


def test_compute_etf_option_delta_cash_loader_exception_degrades_gracefully():
    class _BrokenLoader:
        def get_underlying_spot(self, _):
            raise RuntimeError("db down")

        def get_latest_iv(self, _):
            raise RuntimeError("db down")

        def find_option_contract(self, **_kwargs):
            raise RuntimeError("db down")

    out = odt.compute_etf_option_delta_cash(
        user_query="510300 4月4.6认购买方23张 怎么调",
        symbol_hint="510300",
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_BrokenLoader(),
        as_of_date="20260412",
    )
    assert out["is_etf"] is True
    assert "数据缺口" in out["report"]
    assert len(out["legs"]) == 1
    assert out["publishable"] is False


def test_compute_etf_option_delta_cash_missing_option_latest_price_not_publishable():
    class _NoPriceLoader(_FakeLoader):
        def find_option_contract(self, underlying_code, option_flag, strike, month, as_of_yyyymmdd):
            return {
                "status": "missing_price",
                "ts_code": "510300C2604M04600",
                "delist_date": "20260424",
                "exercise_price": 4.6,
                "missing_reason": "合约510300C2604M04600暂无最新收盘数据",
            }

    out = odt.compute_etf_option_delta_cash(
        user_query="510300 4月4.6认购买方23张 怎么调",
        symbol_hint="510300",
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_NoPriceLoader(with_iv=True),
        as_of_date="20260412",
    )
    assert out["is_etf"] is True
    assert out["publishable"] is False
    assert any("最新收盘数据" in x for x in out["blocking_missing_notes"])
