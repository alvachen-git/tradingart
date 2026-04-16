import option_delta_tools as odt
import pandas as pd
from unittest.mock import patch


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


class _FakeIndexLoader:
    def __init__(self, with_iv=True):
        self.with_iv = with_iv

    def get_underlying_spot(self, underlying_code):
        return {
            "ts_code": underlying_code,
            "trade_date": "20260412",
            "close_price": 4020.0,
        }

    def get_latest_iv(self, underlying_code):
        if not self.with_iv:
            return None
        return {
            "etf_code": underlying_code,
            "trade_date": "20260412",
            "iv": 18.0,
        }

    def find_option_contract(self, underlying_code, option_flag, strike, month, as_of_yyyymmdd):
        if option_flag == "c":
            return {
                "status": "ok",
                "ts_code": "IO2404-C-4000",
                "delist_date": "20260424",
                "exercise_price": 4000.0,
                "is_exact_strike": True,
                "trade_date": "20260412",
                "close": 120.0,
                "vol": 1800,
                "oi": 32000,
            }
        return None

    def get_contract_by_ts_code(self, ts_code, as_of_yyyymmdd):
        if ts_code == "IO2404-C-4000":
            return {
                "status": "ok",
                "ts_code": ts_code,
                "underlying": "000300.SH",
                "call_put": "C",
                "exercise_price": 4000.0,
                "delist_date": "20260424",
                "trade_date": "20260412",
                "close": 120.0,
                "vol": 1800,
                "oi": 32000,
            }
        return {"status": "missing_contract", "missing_reason": "not found", "ts_code": ts_code}


def test_parse_etf_option_legs_buy_and_sell():
    text = "我有创业板4月3.2认购买方23张，还有3.3认购卖方50张"
    legs = odt.parse_etf_option_legs(text)
    assert len(legs) == 2
    by_strike = {round(float(x["strike"]), 3): x for x in legs}
    assert by_strike[3.2]["signed_qty"] == 23
    assert by_strike[3.3]["signed_qty"] == -50
    assert by_strike[3.2]["option_flag"] == "c"
    assert by_strike[3.3]["option_flag"] == "c"
    assert by_strike[3.2]["direction_cn"] == "买认购"
    assert by_strike[3.3]["direction_cn"] == "卖认购"


def test_normalize_structured_option_legs_adds_cn_direction_labels():
    legs = odt._normalize_structured_option_legs(  # noqa: SLF001 - 测试内部标准化行为
        [
            {"cp": "call", "side": "short", "qty": 2, "strike": 3.2, "month": 4, "underlying_hint": "159915.SZ"},
            {"cp": "put", "side": "long", "qty": 1, "strike": 3.1, "month": 4, "underlying_hint": "159915.SZ"},
        ]
    )
    assert len(legs) == 2
    assert legs[0]["direction_cn"] == "卖认购"
    assert legs[1]["direction_cn"] == "买认沽"


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
    assert out["displayable"] is True
    assert out["execution_ready"] is True
    assert out["publishable"] is True
    assert out["coverage_tier"] == "full"
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
    assert out["displayable"] is True
    assert out["execution_ready"] is True
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
    assert out["displayable"] is True
    assert out["execution_ready"] is True
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
    assert out["displayable"] is False
    assert out["execution_ready"] is False
    assert out["publishable"] is False
    assert out["coverage_tier"] == "gap"
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


def test_compute_option_delta_cash_supports_index_option():
    out = odt.compute_option_delta_cash(
        user_query="IO 4月4000认购买方2张，怎么调？",
        symbol_hint="IO",
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_FakeIndexLoader(with_iv=True),
        as_of_date="20260412",
    )
    assert out["asset_class"] == "index"
    assert out["displayable"] is True
    assert out["execution_ready"] is True
    assert out["publishable"] is True
    assert out["metrics"]["coverage_ratio"] == 1.0
    assert out["legs"][0]["multiplier"] == 100.0
    assert "标的类别: `index`" in out["report"]


def test_compute_option_delta_cash_from_structured_legs():
    out = odt.compute_option_delta_cash_from_legs(
        legs=[
            {"underlying_hint": "159915.SZ", "month": 4, "strike": 3.2, "cp": "call", "side": "long", "qty": 23},
            {"underlying_hint": "159915.SZ", "month": 4, "strike": 3.3, "cp": "call", "side": "short", "qty": 50},
        ],
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_FakeLoader(with_iv=True),
        as_of_date="20260412",
    )
    assert out["is_etf"] is True
    assert len(out["legs"]) == 2
    assert out["displayable"] is True
    assert out["execution_ready"] is True
    assert out["publishable"] is True
    assert "Total Delta Cash" in out["report"]


def test_compute_option_delta_cash_index_missing_iv_not_publishable():
    out = odt.compute_option_delta_cash(
        user_query="IO 4月4000认购买方2张，怎么调？",
        symbol_hint="IO",
        trend_signal="震荡",
        risk_preference="稳健型",
        loader=_FakeIndexLoader(with_iv=False),
        as_of_date="20260412",
    )
    assert out["asset_class"] == "index"
    assert out["displayable"] is False
    assert out["execution_ready"] is False
    assert out["publishable"] is False
    assert any("IV" in x for x in out["blocking_missing_notes"])


def test_compute_option_delta_cash_supports_contract_code_only_leg():
    out = odt.compute_option_delta_cash(
        user_query="请分析我上传的期权持仓",
        symbol_hint="",
        vision_legs=[
            {"contract_code": "IO2404-C-4000", "qty": 2, "side": "long"},
        ],
        vision_domain="option",
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_FakeIndexLoader(with_iv=True),
        as_of_date="20260412",
    )
    assert out["asset_class"] == "index"
    assert out["displayable"] is True
    assert out["execution_ready"] is True
    assert out["publishable"] is True
    assert len(out["legs"]) == 1
    assert out["legs"][0]["ts_code"] == "IO2404-C-4000"


def test_get_contract_by_ts_code_uses_matched_suffix_for_price_query():
    loader = odt.ETFOptionMarketLoader(engine=object())
    seen_params = []

    def _fake_read_sql(sql, engine, params=None):
        params = params or {}
        seen_params.append(dict(params))
        sql_text = str(sql)
        if "FROM option_basic" in sql_text:
            if params.get("ts_code") == "90007162.SH":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "90007162.SH",
                            "underlying": "159915.SZ",
                            "call_put": "C",
                            "exercise_price": 3.4,
                            "delist_date": "20260424",
                        }
                    ]
                )
            return pd.DataFrame()
        if "FROM option_daily" in sql_text:
            if params.get("ts_code") == "90007162.SH":
                return pd.DataFrame([{"trade_date": "20260412", "close": 0.12, "vol": 100, "oi": 1000}])
            return pd.DataFrame()
        return pd.DataFrame()

    with patch.object(odt.pd, "read_sql", side_effect=_fake_read_sql):
        out = loader.get_contract_by_ts_code("90007162", as_of_yyyymmdd="20260412")

    assert out["status"] == "ok"
    assert out["ts_code"] == "90007162.SH"
    # 关键断言：价格查询使用了补全后的后缀代码，而不是原始裸码。
    assert any(p.get("ts_code") == "90007162.SH" for p in seen_params)


def test_compute_option_delta_cash_multi_underlying_returns_per_underlying_and_portfolio():
    class _MultiLoader:
        def get_underlying_spot(self, underlying_code):
            if underlying_code == "510050.SH":
                return {"ts_code": underlying_code, "trade_date": "20260412", "close_price": 3.02}
            if underlying_code == "159915.SZ":
                return {"ts_code": underlying_code, "trade_date": "20260412", "close_price": 3.437}
            return None

        def get_latest_iv(self, underlying_code):
            if underlying_code == "510050.SH":
                return {"etf_code": underlying_code, "trade_date": "20260412", "iv": 18.0}
            if underlying_code == "159915.SZ":
                return {"etf_code": underlying_code, "trade_date": "20260412", "iv": 22.0}
            return None

        def find_option_contract(self, underlying_code, option_flag, strike, month, as_of_yyyymmdd):
            if underlying_code == "510050.SH":
                return {
                    "status": "ok",
                    "ts_code": "10000001.SH",
                    "delist_date": "20260424",
                    "exercise_price": float(strike),
                    "is_exact_strike": True,
                    "trade_date": "20260412",
                    "close": 0.130,
                    "vol": 1500,
                    "oi": 16000,
                }
            if underlying_code == "159915.SZ":
                return {
                    "status": "ok",
                    "ts_code": "90007162.SH",
                    "delist_date": "20260424",
                    "exercise_price": float(strike),
                    "is_exact_strike": True,
                    "trade_date": "20260412",
                    "close": 0.1228,
                    "vol": 1200,
                    "oi": 18000,
                }
            return None

    out = odt.compute_option_delta_cash(
        user_query="请分析上传截图",
        symbol_hint="",
        vision_legs=[
            {"underlying_hint": "510050.SH", "month": 4, "strike": 3.0, "cp": "call", "side": "long", "qty": 2},
            {"underlying_hint": "159915.SZ", "month": 4, "strike": 3.4, "cp": "call", "side": "short", "qty": 10},
        ],
        trend_signal="看涨",
        trend_map={"510050.SH": "震荡", "159915.SZ": "看涨"},
        risk_preference="稳健型",
        loader=_MultiLoader(),
        as_of_date="20260412",
    )
    assert out["displayable"] is True
    assert out["execution_ready"] is True
    assert out["publishable"] is True
    assert out["asset_class"] == "multi"
    assert set((out.get("per_underlying") or {}).keys()) == {"510050.SH", "159915.SZ"}
    assert "portfolio_summary" in out
    assert "risk_contribution_ranking" in out
    assert "#### 调仓优先队列（风险贡献最大腿优先）" in out["report"]
    assert "组合 Total Delta Cash" in out["report"]


def test_compute_option_delta_cash_multi_underlying_partial_gap_displayable_but_not_execution_ready():
    class _PartialGapLoader:
        def get_underlying_spot(self, underlying_code):
            mapping = {
                "510050.SH": {"ts_code": "510050.SH", "trade_date": "20260412", "close_price": 3.02},
                "159915.SZ": {"ts_code": "159915.SZ", "trade_date": "20260412", "close_price": 3.437},
            }
            return mapping.get(underlying_code)

        def get_latest_iv(self, underlying_code):
            if underlying_code == "510050.SH":
                return {"etf_code": "510050.SH", "trade_date": "20260412", "iv": 18.0}
            if underlying_code == "159915.SZ":
                return None
            return None

        def find_option_contract(self, underlying_code, option_flag, strike, month, as_of_yyyymmdd):
            return {
                "status": "ok",
                "ts_code": f"{underlying_code}-C",
                "delist_date": "20260424",
                "exercise_price": float(strike),
                "is_exact_strike": True,
                "trade_date": "20260412",
                "close": 0.100,
                "vol": 1500,
                "oi": 16000,
            }

    out = odt.compute_option_delta_cash(
        user_query="请分析上传截图",
        symbol_hint="",
        vision_legs=[
            {"underlying_hint": "510050.SH", "month": 4, "strike": 3.0, "cp": "call", "side": "long", "qty": 2},
            {"underlying_hint": "159915.SZ", "month": 4, "strike": 3.4, "cp": "call", "side": "short", "qty": 10},
        ],
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_PartialGapLoader(),
        as_of_date="20260412",
    )
    assert out["displayable"] is True
    assert out["execution_ready"] is False
    assert out["publishable"] is False
    assert out["coverage_tier"] == "partial"
    assert any("159915.SZ" in x for x in out["missing_notes"])
    assert "覆盖层级: `partial`" in out["report"]
    assert "暂不输出金额级调整量" in out["report"]


def test_compute_option_delta_cash_multi_risk_contribution_priority_not_bias_small_leg():
    class _RankLoader:
        def get_underlying_spot(self, underlying_code):
            mapping = {
                "510500.SH": {"ts_code": "510500.SH", "trade_date": "20260412", "close_price": 8.0},
                "159915.SZ": {"ts_code": "159915.SZ", "trade_date": "20260412", "close_price": 3.4},
            }
            return mapping.get(underlying_code)

        def get_latest_iv(self, underlying_code):
            return {"etf_code": underlying_code, "trade_date": "20260412", "iv": 20.0}

        def find_option_contract(self, underlying_code, option_flag, strike, month, as_of_yyyymmdd):
            return {
                "status": "ok",
                "ts_code": f"{underlying_code}-{option_flag}",
                "delist_date": "20260424",
                "exercise_price": float(strike),
                "is_exact_strike": True,
                "trade_date": "20260412",
                "close": 0.1,
                "vol": 2000,
                "oi": 20000,
            }

    out = odt.compute_option_delta_cash(
        user_query="请分析上传截图",
        symbol_hint="",
        vision_legs=[
            {"underlying_hint": "510500.SH", "month": 4, "strike": 8.0, "cp": "call", "side": "short", "qty": 50},
            {"underlying_hint": "159915.SZ", "month": 4, "strike": 3.4, "cp": "call", "side": "short", "qty": 2},
        ],
        trend_signal="看涨",
        trend_map={"510500.SH": "看涨", "159915.SZ": "震荡"},
        risk_preference="稳健型",
        loader=_RankLoader(),
        as_of_date="20260412",
    )
    ranking = out.get("risk_contribution_ranking") or []
    assert ranking
    assert ranking[0]["underlying_code"] == "510500.SH"


def test_compute_option_delta_cash_normalizes_contract_code_ss_suffix():
    class _CodeNormLoader(_FakeIndexLoader):
        def get_contract_by_ts_code(self, ts_code, as_of_yyyymmdd):
            assert ts_code == "90007162.SH"
            return {
                "status": "ok",
                "ts_code": "90007162.SH",
                "underlying": "159915.SZ",
                "call_put": "C",
                "exercise_price": 3.4,
                "delist_date": "20260424",
                "trade_date": "20260412",
                "close": 0.1228,
                "vol": 100,
                "oi": 1000,
            }

        def get_underlying_spot(self, underlying_code):
            return {"ts_code": underlying_code, "trade_date": "20260412", "close_price": 3.437}

        def get_latest_iv(self, underlying_code):
            return {"etf_code": underlying_code, "trade_date": "20260412", "iv": 22.0}

        def find_option_contract(self, underlying_code, option_flag, strike, month, as_of_yyyymmdd):
            return {
                "status": "ok",
                "ts_code": "90007162.SH",
                "delist_date": "20260424",
                "exercise_price": 3.4,
                "is_exact_strike": True,
                "trade_date": "20260412",
                "close": 0.1228,
                "vol": 100,
                "oi": 1000,
            }

    out = odt.compute_option_delta_cash(
        user_query="请分析上传期权持仓",
        symbol_hint="",
        vision_legs=[{"contract_code": "90007162.SS", "qty": 10, "side": "short", "cp": "call", "strike": 3.4, "month": 4, "underlying_hint": "159915.SZ"}],
        vision_domain="option",
        trend_signal="看涨",
        risk_preference="稳健型",
        loader=_CodeNormLoader(with_iv=True),
        as_of_date="20260412",
    )
    assert out["displayable"] is True
    assert out["execution_ready"] is True
    assert out["publishable"] is True
    assert len(out["legs"]) == 1
    assert out["legs"][0]["ts_code"] == "90007162.SH"


def test_classify_delta_coverage_tiers():
    gap = odt._classify_delta_coverage(coverage_ratio=0.0, has_legs=True)  # noqa: SLF001
    partial = odt._classify_delta_coverage(coverage_ratio=0.59, has_legs=True)  # noqa: SLF001
    full = odt._classify_delta_coverage(coverage_ratio=0.60, has_legs=True)  # noqa: SLF001
    assert gap == {"coverage_tier": "gap", "displayable": False, "execution_ready": False}
    assert partial == {"coverage_tier": "partial", "displayable": True, "execution_ready": False}
    assert full == {"coverage_tier": "full", "displayable": True, "execution_ready": True}


def test_fetch_underlying_spot_map_returns_multi_underlying_quotes():
    class _SpotLoader:
        def get_underlying_spot(self, underlying_code):
            mapping = {
                "510500.SH": {"ts_code": "510500.SH", "trade_date": "20260415", "close_price": 8.111},
                "159915.SZ": {"ts_code": "159915.SZ", "trade_date": "20260415", "close_price": 3.542},
                "000300.SH": {"ts_code": "000300.SH", "trade_date": "20260415", "close_price": 4012.20},
            }
            return mapping.get(underlying_code)

    out = odt.fetch_underlying_spot_map(
        underlyings=["510500", "159915.SZ", "IO"],
        loader=_SpotLoader(),
    )
    assert set(out.keys()) == {"510500.SH", "159915.SZ", "000300.SH"}
    assert out["510500.SH"]["close_price"] == 8.111
    assert out["159915.SZ"]["trade_date"] == "20260415"
    assert out["000300.SH"]["source"] == "index_price"
    assert out["000300.SH"]["missing"] is False


def test_fetch_underlying_spot_map_marks_missing_without_guess():
    class _SpotLoader:
        def get_underlying_spot(self, underlying_code):
            if underlying_code == "510500.SH":
                return {"ts_code": "510500.SH", "trade_date": "20260415", "close_price": 8.111}
            return None

    out = odt.fetch_underlying_spot_map(
        underlyings=["510500.SH", "159915.SZ"],
        loader=_SpotLoader(),
    )
    assert out["510500.SH"]["missing"] is False
    assert out["159915.SZ"]["missing"] is True
    assert out["159915.SZ"]["close_price"] is None
    assert "missing_reason" in out["159915.SZ"]
