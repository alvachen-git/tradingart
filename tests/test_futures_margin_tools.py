import pandas as pd

import futures_fund_flow_tools as fft


def test_margin_profile_from_fees_info(monkeypatch):
    fees_df = pd.DataFrame(
        [
            {
                "合约代码": "RB2610",
                "品种代码": "RB",
                "合约乘数": 10,
                "做多保证金率": 0.10,
                "做空保证金率": 0.12,
                "做多保证金/手": 3500,
                "做空保证金/手": 4200,
                "最新价": 3500,
                "更新时间": "2026-04-01 10:00:00",
                "持仓量": 120000,
            }
        ]
    )
    monkeypatch.setattr(fft, "_get_fees_info_cached", lambda ttl_seconds=300: fees_df)
    monkeypatch.setattr(fft, "_get_rule_cached", lambda ttl_seconds=300: pd.DataFrame())
    monkeypatch.setattr(fft, "_get_latest_price_for_margin_estimation", lambda code, month=None: (None, "", ""))

    out = fft.get_futures_margin_profile.invoke(
        {"query": "螺纹钢2610", "lots": 2, "side": "long", "broker_margin_factor": 1.1}
    )
    assert "数据来源: ak.futures_fees_info" in out
    assert "合约乘数: 10" in out
    assert "多头保证金率: 10.00%" in out
    assert "期货公司上浮系数(统一): 1.20x" in out
    assert "估算总保证金(多头, 已含上浮): 8,400.00 元" in out


def test_margin_profile_applies_short_side_and_factor(monkeypatch):
    fees_df = pd.DataFrame(
        [
            {
                "合约代码": "AG2606",
                "品种代码": "AG",
                "合约乘数": 15,
                "做多保证金率": 0.09,
                "做空保证金率": 0.11,
                "做多保证金/手": 12000,
                "做空保证金/手": 15000,
                "最新价": 7800,
                "更新时间": "2026-04-01 10:00:00",
                "持仓量": 50000,
            }
        ]
    )
    monkeypatch.setattr(fft, "_get_fees_info_cached", lambda ttl_seconds=300: fees_df)
    monkeypatch.setattr(fft, "_get_rule_cached", lambda ttl_seconds=300: pd.DataFrame())
    monkeypatch.setattr(fft, "_get_latest_price_for_margin_estimation", lambda code, month=None: (None, "", ""))

    out = fft.get_futures_margin_profile.invoke(
        {"query": "AG2606", "lots": 3, "side": "short", "broker_margin_factor": 1.5}
    )
    assert "计算方向: 空头" in out
    assert "期货公司上浮系数(统一): 1.20x" in out
    assert "估算总保证金(空头, 已含上浮): 54,000.00 元" in out


def test_margin_profile_falls_back_to_rule(monkeypatch):
    rule_df = pd.DataFrame(
        [
            {
                "代码": "RB",
                "交易保证金比例": 10.0,
                "合约乘数": 10,
            }
        ]
    )
    monkeypatch.setattr(fft, "_get_fees_info_cached", lambda ttl_seconds=300: pd.DataFrame())
    monkeypatch.setattr(fft, "_get_rule_cached", lambda ttl_seconds=300: rule_df)
    monkeypatch.setattr(fft, "_get_latest_price_for_margin_estimation", lambda code, month=None: (3600, "RB2610", "20260401"))

    out = fft.get_futures_margin_profile.invoke({"query": "RB"})
    assert "数据来源: ak.futures_rule" in out
    assert "估算每手保证金(多头): 3,600.00 元" in out


def test_margin_profile_falls_back_to_static_mapping(monkeypatch):
    monkeypatch.setattr(fft, "_get_fees_info_cached", lambda ttl_seconds=300: pd.DataFrame())
    monkeypatch.setattr(fft, "_get_rule_cached", lambda ttl_seconds=300: pd.DataFrame())
    monkeypatch.setattr(fft, "_get_latest_price_for_margin_estimation", lambda code, month=None: (3600, "RB2610", "20260401"))

    out = fft.get_futures_margin_profile.invoke({"query": "螺纹钢", "lots": 2})
    assert "数据来源: static_mapping" in out
    assert "估算总保证金(多头, 已含上浮): 8,640.00 元" in out
