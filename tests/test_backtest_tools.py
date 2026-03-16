import pandas as pd

import backtest_tools as bt


def _stub_etf_result(start_date: str, end_date: str, strategy: str = "double_sell"):
    trades = pd.DataFrame({"net_pnl": [100.0, -50.0, 80.0, -40.0]})
    return {
        "summary": {
            "symbol": "510500",
            "strategy": strategy,
            "start_date": start_date,
            "end_date": end_date,
            "trades": 4,
            "total_pnl": 90.0,
            "annualized_pnl": 30.0,
            "annualized_return_pct": 0.12,
            "max_drawdown": -120.0,
            "max_drawdown_pct": -0.08,
            "win_rate": 0.5,
            "avg_return": 22.5,
            "avg_margin": 15000.0,
        },
        "trades": trades,
    }


def test_run_option_strategy_backtest_explicit_dates(monkeypatch):
    monkeypatch.setattr(bt, "_latest_trade_day_for_underlying", lambda _: "20260210")
    monkeypatch.setattr(bt, "align_to_prev_trade_day", lambda d, underlying=None: d)
    monkeypatch.setattr(
        bt,
        "run_etf_roll_backtest",
        lambda **kwargs: _stub_etf_result(kwargs["start_date"], kwargs["end_date"], kwargs["strategy"]),
    )

    out = bt.run_option_strategy_backtest.invoke(
        {
            "symbol": "510500",
            "strategy": "双卖",
            "start_date": "20250101",
            "end_date": "20251231",
        }
    )
    assert "生效区间: 20250101 ~ 20251231" in out
    assert "盈亏比(单笔均值比): 2.00" in out


def test_run_option_strategy_backtest_time_expr(monkeypatch):
    monkeypatch.setattr(bt, "_latest_trade_day_for_underlying", lambda _: "20260210")
    monkeypatch.setattr(bt, "align_to_prev_trade_day", lambda d, underlying=None: d)
    monkeypatch.setattr(
        bt,
        "run_etf_roll_backtest",
        lambda **kwargs: _stub_etf_result(kwargs["start_date"], kwargs["end_date"], kwargs["strategy"]),
    )

    out = bt.run_option_strategy_backtest.invoke(
        {
            "symbol": "510500",
            "strategy": "牛市价差",
            "time_expr": "2024Q3",
        }
    )
    assert "策略: bull_spread" in out
    assert "生效区间: 20240701 ~ 20240930" in out
    assert "requested_time_expr: 2024Q3" in out


def test_run_option_strategy_backtest_unparsable_time_expr():
    out = bt.run_option_strategy_backtest.invoke(
        {
            "symbol": "510500",
            "strategy": "双卖",
            "time_expr": "最近几段时间",
        }
    )
    assert "时间参数错误" in out


def test_run_option_backtest_compat_max_oi(monkeypatch):
    monkeypatch.setattr(
        bt,
        "run_max_oi_backtest",
        lambda **kwargs: {
            "summary": {
                "symbol": "510500",
                "strategy": "max_oi_call",
                "start_date": kwargs.get("start_date", "20250101"),
                "end_date": kwargs.get("end_date", "20251231"),
                "trades": 2,
                "total_return": 0.1,
                "annualized_return": 0.08,
                "max_drawdown": -0.05,
                "win_rate": 0.5,
                "avg_return": 0.05,
            },
            "trades": pd.DataFrame({"ret": [0.1, -0.02]}),
        },
    )
    out = bt.run_option_backtest.invoke(
        {
            "symbol": "510500",
            "strategy": "max_oi_call",
            "start_date": "20250101",
            "end_date": "20251231",
        }
    )
    assert "策略: max_oi_call" in out
