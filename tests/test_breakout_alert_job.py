import pandas as pd
import types

from breakout_alert_job import (
    _signal_hash,
    build_prefilter_candidates,
    compose_grouped_brief,
    compose_grouped_html,
    compose_grouped_summary,
    fetch_realtime_prices,
    parse_llm_json,
    run_job,
    run_test_push,
)


def _mock_history_df(rows: int = 90, start: float = 100.0) -> pd.DataFrame:
    """趋势样本：用于验证非横盘场景。"""
    data = []
    for i in range(rows):
        base = start + i * 0.2
        data.append(
            {
                "trade_date": f"2025{i//30+1:02d}{(i%28)+1:02d}",
                "open_price": base,
                "high_price": base + 1.0,
                "low_price": base - 1.0,
                "close_price": base + 0.2,
            }
        )
    return pd.DataFrame(data)


def _mock_sideways_df(rows: int = 90, base: float = 100.0) -> pd.DataFrame:
    """横盘样本：用于验证平台突破与8-bar约束。"""
    data = []
    for i in range(rows):
        offset = (i % 4 - 1.5) * 0.15
        close = base + offset
        data.append(
            {
                "trade_date": f"2025{i//30+1:02d}{(i%28)+1:02d}",
                "open_price": close - 0.1,
                "high_price": close + 0.5,
                "low_price": close - 0.5,
                "close_price": close,
            }
        )
    return pd.DataFrame(data)


def test_fetch_realtime_prefers_main_contract(monkeypatch):
    import breakout_alert_job as job

    class _Resp:
        def __init__(self, text):
            self.text = text

    class _Sess:
        def get(self, url, timeout=2):
            text = (
                'var hq_str_CFF_RE_IF2602="IF2602,0,0,4711.2,0,0,2026-03-18,15:00:00";'
                'var hq_str_CFF_RE_IF2603="IF2603,0,0,4651.4,0,0,2026-03-18,15:00:00";'
            )
            return _Resp(text)

    monkeypatch.setattr(job, "_build_sina_session", lambda: _Sess())
    got = fetch_realtime_prices(["IF"], preferred_contracts={"IF": "IF2603"}, target_trade_date="20260318")
    assert got["IF"] is not None
    assert got["IF"]["contract_code"] == "IF2603"


def test_fetch_realtime_skips_stale_quote(monkeypatch):
    import breakout_alert_job as job

    class _Resp:
        def __init__(self, text):
            self.text = text

    class _Sess:
        def get(self, url, timeout=2):
            text = (
                'var hq_str_CFF_RE_IH2602="IH2602,0,0,3042.6,0,0,2026-02-24,15:00:00";'
                'var hq_str_CFF_RE_IH2603="IH2603,0,0,2958.6,0,0,2026-03-18,15:00:00";'
            )
            return _Resp(text)

    monkeypatch.setattr(job, "_build_sina_session", lambda: _Sess())
    monkeypatch.setattr(job, "_candidate_contracts", lambda code: ["CFF_RE_IH2602", "CFF_RE_IH2603"])
    got = fetch_realtime_prices(["IH"], preferred_contracts={}, target_trade_date="20260318")
    assert got["IH"] is not None
    assert got["IH"]["contract_code"] == "IH2603"


def test_fetch_realtime_fallback_to_minute_when_sina_empty(monkeypatch):
    import breakout_alert_job as job

    class _Resp:
        def __init__(self, text):
            self.text = text

    class _Sess:
        def get(self, url, timeout=2):
            return _Resp('var hq_str_nf_cu2605="";')

    monkeypatch.setattr(job, "_build_sina_session", lambda: _Sess())
    monkeypatch.setattr(
        job,
        "_fetch_minute_close_fallback",
        lambda contract_code: {
            "price": 94420.0,
            "quote_date": "20260319",
            "name": "CU2605",
            "contract_code": "CU2605",
            "source": "ak_minute",
        },
    )

    got = fetch_realtime_prices(["CU"], preferred_contracts={"CU": "CU2605"}, target_trade_date="20260319")
    assert got["CU"] is not None
    assert got["CU"]["contract_code"] == "CU2605"
    assert abs(float(got["CU"]["price"]) - 94420.0) < 1e-9
    assert got["CU"]["source"] == "ak_minute"


def test_fetch_realtime_fallback_respects_trade_date(monkeypatch):
    import breakout_alert_job as job

    class _Resp:
        def __init__(self, text):
            self.text = text

    class _Sess:
        def get(self, url, timeout=2):
            return _Resp('var hq_str_nf_cu2605="";')

    monkeypatch.setattr(job, "_build_sina_session", lambda: _Sess())
    monkeypatch.setattr(
        job,
        "_fetch_minute_close_fallback",
        lambda contract_code: {
            "price": 94420.0,
            "quote_date": "20260318",
            "name": "CU2605",
            "contract_code": "CU2605",
            "source": "ak_minute",
        },
    )

    got = fetch_realtime_prices(["CU"], preferred_contracts={"CU": "CU2605"}, target_trade_date="20260319")
    assert got["CU"] is None


def test_prefilter_hits_up_and_down(monkeypatch):
    import breakout_alert_job as job

    hist = _mock_sideways_df()
    history_map = {"RB": hist, "AG": hist}
    rt_map = {
        "RB": {"price": float(hist["high_price"].tail(10).max() + 1.2), "contract_code": "RB2605", "name": "螺纹"},
        "AG": {"price": float(hist["low_price"].tail(10).min() - 1.2), "contract_code": "AG2606", "name": "白银"},
    }

    def _fake_kline(df):
        if float(df.iloc[-1]["close_price"]) > float(df.iloc[-2]["close_price"]):
            return {"patterns": ["【上升三法】(中继再涨，多头持续上攻！)"], "trends": [], "score": 50}
        return {"patterns": ["【下降三法】(中继再跌，多头持续溃逃)"], "trends": [], "score": 50}

    monkeypatch.setattr(job, "_calc_kline_signals", _fake_kline)
    candidates = job.build_prefilter_candidates(history_map, rt_map, threshold_atr=0.30, max_candidates=20)

    assert len(candidates) == 2
    directions = {x["symbol"]: x["direction"] for x in candidates}
    assert directions["RB"] == "up"
    assert directions["AG"] == "down"


def test_prefilter_respects_max_candidates(monkeypatch):
    import breakout_alert_job as job

    hist = _mock_sideways_df()
    history_map = {"RB": hist, "AG": hist, "CU": hist}
    rt_map = {
        "RB": {"price": float(hist["high_price"].tail(10).max() + 0.8), "contract_code": "RB2605", "name": "螺纹"},
        "AG": {"price": float(hist["high_price"].tail(10).max() + 0.9), "contract_code": "AG2606", "name": "白银"},
        "CU": {"price": float(hist["high_price"].tail(10).max() + 1.5), "contract_code": "CU2606", "name": "铜"},
    }

    monkeypatch.setattr(job, "SCAN_PERIODS", (10, 20, 30, 60))
    monkeypatch.setattr(job, "_calc_kline_signals", lambda df: {"patterns": ["10日平台突破"], "trends": [], "score": 60})
    candidates = job.build_prefilter_candidates(history_map, rt_map, threshold_atr=0.30, max_candidates=2)

    assert len(candidates) == 2
    assert candidates[0]["strength"] >= candidates[1]["strength"]


def test_prefilter_requires_pattern_whitelist(monkeypatch):
    import breakout_alert_job as job

    hist = _mock_sideways_df()
    history_map = {"RB": hist}
    rt_map = {
        "RB": {"price": float(hist["high_price"].tail(10).max() + 1.0), "contract_code": "RB2605", "name": "螺纹"},
    }
    monkeypatch.setattr(job, "_calc_kline_signals", lambda df: {"patterns": ["多头吞噬"], "trends": [], "score": 80})
    candidates = job.build_prefilter_candidates(history_map, rt_map, threshold_atr=0.30, max_candidates=20)
    assert candidates == []


def test_prefilter_platform_breakout_requires_period_at_least_8(monkeypatch):
    import breakout_alert_job as job

    hist = _mock_history_df()
    history_map = {"RB": hist}
    rt_map = {
        "RB": {"price": float(hist["high_price"].tail(10).max() + 1.2), "contract_code": "RB2605", "name": "螺纹"},
    }
    monkeypatch.setattr(job, "SCAN_PERIODS", (5,))
    monkeypatch.setattr(job, "_calc_kline_signals", lambda df: {"patterns": ["10日平台突破"], "trends": [], "score": 80})
    candidates = job.build_prefilter_candidates(history_map, rt_map, threshold_atr=0.30, max_candidates=20)
    assert candidates == []


def test_parse_llm_json_with_fence():
    raw = """```json
    [
      {"symbol":"RB","direction":"up","is_breakout":true,"reason_simple":"上破箱体","confidence":0.81},
      {"symbol":"AG","direction":"none","is_breakout":false,"reason_simple":"无突破","confidence":0.42}
    ]
    ```"""

    parsed = parse_llm_json(raw)

    assert len(parsed) == 2
    assert parsed[0]["symbol"] == "RB"
    assert parsed[0]["is_breakout"] is True
    assert abs(parsed[0]["confidence"] - 0.81) < 1e-9


def test_parse_llm_json_bool_string_safe():
    raw = """[
      {"symbol":"RB","direction":"up","is_breakout":"false","reason_simple":"无效","confidence":0.9},
      {"symbol":"AG","direction":"down","is_breakout":"true","reason_simple":"有效","confidence":0.7}
    ]"""

    parsed = parse_llm_json(raw)
    assert parsed[0]["is_breakout"] is False
    assert parsed[1]["is_breakout"] is True


def test_compose_summary_empty_and_grouped():
    empty_text = compose_grouped_summary("20260318", 76, 5, [])
    assert "未发现有效突破信号" in empty_text

    signals = [
        {
            "symbol": "RB",
            "contract_code": "RB2605",
            "direction": "up",
            "period": 10,
            "strength": 0.9,
            "realtime_price": 3400,
            "reason_simple": "上破10日箱体",
        },
        {
            "symbol": "AG",
            "contract_code": "AG2606",
            "direction": "down",
            "period": 5,
            "strength": 0.8,
            "realtime_price": 7800,
            "reason_simple": "下破5日箱体",
        },
    ]
    txt = compose_grouped_summary("20260318", 76, 5, signals)
    assert "【上破组】" in txt
    assert "【下破组】" in txt
    assert "RB(RB2605)" in txt
    assert "AG(AG2606)" in txt


def test_compose_html_and_brief_grouped():
    signals = [
        {
            "symbol": "RB",
            "contract_code": "RB2605",
            "direction": "up",
            "period": 20,
            "strength": 1.2,
            "strength_raw": 1.2,
            "atr_ratio": 2.1,
            "realtime_price": 3500.0,
            "reason_simple": "上破20日箱体",
        },
        {
            "symbol": "AG",
            "contract_code": "AG2606",
            "direction": "down",
            "period": 10,
            "strength": 0.9,
            "strength_raw": 0.9,
            "atr_ratio": 1.8,
            "realtime_price": 7800.0,
            "reason_simple": "下破10日箱体",
        },
    ]
    html = compose_grouped_html("20260318", 79, 6, signals)
    brief = compose_grouped_brief("20260318", 79, 6, signals)

    assert "14:25 技术突破提醒" in html
    assert "RB(RB2605)" in html
    assert "AG(AG2606)" in html
    assert "上破组" in html
    assert "下破组" in html
    assert "扫描79 | 预筛6 | 确认2 | 上破1 | 下破1" in brief


def test_signal_hash_stable_for_order():
    a = [
        {"symbol": "RB", "direction": "up", "period": 10, "strength": 0.61},
        {"symbol": "AG", "direction": "down", "period": 5, "strength": 0.72},
    ]
    b = [a[1], a[0]]

    assert _signal_hash("20260318", a) == _signal_hash("20260318", b)


def test_run_job_no_signal_no_push(monkeypatch):
    import breakout_alert_job as job

    latest = "20260318"
    hist = _mock_history_df()

    fake_data_engine = types.SimpleNamespace(
        PRODUCT_MAP={"RB": {}, "AG": {}},
        engine=object(),
        get_latest_data_date=lambda: latest,
    )
    monkeypatch.setitem(__import__("sys").modules, "data_engine", fake_data_engine)

    monkeypatch.setattr(job, "_load_scan_symbols", lambda **kwargs: ["RB"])
    monkeypatch.setattr(
        job,
        "fetch_realtime_prices",
        lambda symbols, preferred_contracts=None, target_trade_date="": {
            "RB": {"price": 123.0, "contract_code": "RB2605", "name": "螺纹"}
        },
    )
    monkeypatch.setattr(job, "_fetch_history_df", lambda engine, symbol, end_date, bars=90: hist.copy())
    monkeypatch.setattr(job, "llm_review_candidates", lambda candidates, model_name, min_confidence: ([], ""))
    monkeypatch.setattr(job, "_load_state", lambda path: {})
    monkeypatch.setattr(job, "_resolve_email_recipients", lambda channel_code, email_to_override="": [])

    called = {"publish": 0, "webhook": 0}

    def _pub(*args, **kwargs):
        called["publish"] += 1
        return True, "ok"

    def _hook(*args, **kwargs):
        called["webhook"] += 1
        return True, "ok"

    monkeypatch.setattr(job, "_publish_station", _pub)
    monkeypatch.setattr(job, "_send_email", _hook)

    result = run_job(trade_date_arg=latest, limit=0, symbols_arg="", dry_run=False)
    assert result["status"] == "success"
    assert result["signal_count"] == 0
    assert called["publish"] == 0
    assert called["webhook"] == 0


def test_run_job_same_day_dedup_skip_push(monkeypatch):
    import breakout_alert_job as job

    latest = "20260318"
    hist = _mock_history_df()
    approved = [
        {
            "symbol": "RB",
            "contract_code": "RB2605",
            "direction": "up",
            "period": 10,
            "strength": 0.9,
            "realtime_price": 3500.0,
            "reason_simple": "上破10日箱体",
        }
    ]

    fake_data_engine = types.SimpleNamespace(
        PRODUCT_MAP={"RB": {}},
        engine=object(),
        get_latest_data_date=lambda: latest,
    )
    monkeypatch.setitem(__import__("sys").modules, "data_engine", fake_data_engine)

    monkeypatch.setattr(job, "_load_scan_symbols", lambda **kwargs: ["RB"])
    monkeypatch.setattr(
        job,
        "fetch_realtime_prices",
        lambda symbols, preferred_contracts=None, target_trade_date="": {
            "RB": {"price": 123.0, "contract_code": "RB2605", "name": "螺纹"}
        },
    )
    monkeypatch.setattr(job, "_fetch_history_df", lambda engine, symbol, end_date, bars=90: hist.copy())
    monkeypatch.setattr(job, "build_prefilter_candidates", lambda **kwargs: approved.copy())
    monkeypatch.setattr(job, "llm_review_candidates", lambda candidates, model_name, min_confidence: (approved.copy(), ""))
    monkeypatch.setattr(job, "_resolve_email_recipients", lambda channel_code, email_to_override="": [])

    dedupe = _signal_hash(latest, approved)
    monkeypatch.setattr(job, "_load_state", lambda path: {"last_trade_date": latest, "last_signal_hash": dedupe})

    called = {"publish": 0, "webhook": 0, "save": 0}

    def _pub(*args, **kwargs):
        called["publish"] += 1
        return True, "ok"

    def _hook(*args, **kwargs):
        called["webhook"] += 1
        return True, "ok"

    def _save(*args, **kwargs):
        called["save"] += 1

    monkeypatch.setattr(job, "_publish_station", _pub)
    monkeypatch.setattr(job, "_send_email", _hook)
    monkeypatch.setattr(job, "_save_state", _save)

    result = run_job(trade_date_arg=latest, limit=0, symbols_arg="", dry_run=False)
    assert result["status"] == "success"
    assert result["sent_before"] is True
    assert result["publish_msg"] == "duplicate-signal-skip"
    assert result["email_msg"] == "duplicate-signal-skip"
    assert called["publish"] == 0
    assert called["webhook"] == 0
    assert called["save"] == 0


def test_run_job_dry_run_does_not_save_state(monkeypatch):
    import breakout_alert_job as job

    latest = "20260318"
    hist = _mock_history_df()
    approved = [
        {
            "symbol": "RB",
            "contract_code": "RB2605",
            "direction": "up",
            "period": 10,
            "strength": 0.9,
            "realtime_price": 3500.0,
            "reason_simple": "上破10日箱体",
        }
    ]

    fake_data_engine = types.SimpleNamespace(
        PRODUCT_MAP={"RB": {}},
        engine=object(),
        get_latest_data_date=lambda: latest,
    )
    monkeypatch.setitem(__import__("sys").modules, "data_engine", fake_data_engine)

    monkeypatch.setattr(job, "_load_scan_symbols", lambda **kwargs: ["RB"])
    monkeypatch.setattr(
        job,
        "fetch_realtime_prices",
        lambda symbols, preferred_contracts=None, target_trade_date="": {
            "RB": {"price": 123.0, "contract_code": "RB2605", "name": "螺纹"}
        },
    )
    monkeypatch.setattr(job, "_fetch_history_df", lambda engine, symbol, end_date, bars=90: hist.copy())
    monkeypatch.setattr(job, "build_prefilter_candidates", lambda **kwargs: approved.copy())
    monkeypatch.setattr(job, "llm_review_candidates", lambda candidates, model_name, min_confidence: (approved.copy(), ""))
    monkeypatch.setattr(job, "_resolve_email_recipients", lambda channel_code, email_to_override="": ["foo@example.com"])
    monkeypatch.setattr(job, "_load_state", lambda path: {})

    called = {"save": 0}
    monkeypatch.setattr(job, "_publish_station", lambda *args, **kwargs: (True, "dry-run"))
    monkeypatch.setattr(job, "_send_email", lambda *args, **kwargs: (True, "dry-run recipients=1"))
    monkeypatch.setattr(job, "_save_state", lambda *args, **kwargs: called.__setitem__("save", called["save"] + 1))

    result = run_job(trade_date_arg=latest, limit=0, symbols_arg="", dry_run=True)
    assert result["status"] == "success"
    assert result["signal_count"] == 1
    assert called["save"] == 0


def test_run_job_publishes_html_content_and_brief_summary(monkeypatch):
    import breakout_alert_job as job

    latest = "20260318"
    hist = _mock_history_df()
    approved = [
        {
            "symbol": "RB",
            "contract_code": "RB2605",
            "direction": "up",
            "period": 10,
            "strength": 0.9,
            "strength_raw": 0.9,
            "atr_ratio": 2.3,
            "realtime_price": 3500.0,
            "reason_simple": "上破10日箱体",
        }
    ]

    fake_data_engine = types.SimpleNamespace(
        PRODUCT_MAP={"RB": {}},
        engine=object(),
        get_latest_data_date=lambda: latest,
    )
    monkeypatch.setitem(__import__("sys").modules, "data_engine", fake_data_engine)

    monkeypatch.setattr(job, "_load_scan_symbols", lambda **kwargs: ["RB"])
    monkeypatch.setattr(
        job,
        "fetch_realtime_prices",
        lambda symbols, preferred_contracts=None, target_trade_date="": {
            "RB": {"price": 123.0, "contract_code": "RB2605", "name": "螺纹"}
        },
    )
    monkeypatch.setattr(job, "_fetch_history_df", lambda engine, symbol, end_date, bars=90: hist.copy())
    monkeypatch.setattr(job, "build_prefilter_candidates", lambda **kwargs: approved.copy())
    monkeypatch.setattr(job, "llm_review_candidates", lambda candidates, model_name, min_confidence: (approved.copy(), ""))
    monkeypatch.setattr(job, "_resolve_email_recipients", lambda channel_code, email_to_override="": ["foo@example.com"])
    monkeypatch.setattr(job, "_load_state", lambda path: {})

    captured = {"publish": None}
    monkeypatch.setattr(
        job,
        "_publish_station",
        lambda *args, **kwargs: (captured.__setitem__("publish", {"args": args, "kwargs": kwargs}) or True, "ok"),
    )
    monkeypatch.setattr(job, "_send_email", lambda *args, **kwargs: (True, "ok"))
    monkeypatch.setattr(job, "_save_state", lambda *args, **kwargs: None)

    result = run_job(trade_date_arg=latest, limit=0, symbols_arg="", dry_run=False)
    assert result["status"] == "success"
    assert result["signal_count"] == 1
    assert captured["publish"] is not None

    publish_kwargs = captured["publish"]["kwargs"]
    assert "<div style=" in publish_kwargs["content"]
    assert "上破组" in publish_kwargs["content"]
    assert "扫描1 | 预筛1 | 确认1 | 上破1 | 下破0" in publish_kwargs["summary"]


def test_run_test_push_success(monkeypatch):
    import breakout_alert_job as job

    monkeypatch.setenv("BREAKOUT_CHANNEL_CODE", "trade_signal")
    monkeypatch.setenv("BREAKOUT_ALERT_EMAIL_TO", "foo@example.com")
    monkeypatch.setattr(job, "_resolve_email_recipients", lambda channel_code, email_to_override="": ["foo@example.com"])
    monkeypatch.setattr(job, "_publish_station", lambda *args, **kwargs: (True, "site-ok"))
    monkeypatch.setattr(job, "_send_email", lambda *args, **kwargs: (True, "email-ok"))

    result = run_test_push(dry_run=False, test_message="联调测试", email_to_arg="")
    assert result["status"] == "success"
    assert result["mode"] == "test-push"
    assert result["publish_ok"] is True
    assert result["email_ok"] is True


def test_run_test_push_error_when_all_failed(monkeypatch):
    import breakout_alert_job as job

    monkeypatch.setenv("BREAKOUT_CHANNEL_CODE", "trade_signal")
    monkeypatch.delenv("BREAKOUT_ALERT_EMAIL_TO", raising=False)
    monkeypatch.delenv("ALERT_EMAIL_TO", raising=False)
    monkeypatch.setattr(job, "_resolve_email_recipients", lambda channel_code, email_to_override="": [])
    monkeypatch.setattr(job, "_publish_station", lambda *args, **kwargs: (False, "site-fail"))
    monkeypatch.setattr(job, "_send_email", lambda *args, **kwargs: (False, "email empty"))

    result = run_test_push(dry_run=False, test_message="", email_to_arg="")
    assert result["status"] == "error"
    assert result["publish_ok"] is False
    assert result["email_ok"] is False
