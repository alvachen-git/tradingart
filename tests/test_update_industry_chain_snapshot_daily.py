import update_industry_chain_snapshot_daily as job


def _mock_snapshot(sector: str):
    return {
        "meta": {
            "sector": sector,
            "fund_trade_date": "20260324",
            "screener_trade_date": "20260324",
            "warnings": [],
        },
        "stages": [],
        "edges": [],
    }


def test_run_update_dry_run_does_not_write(monkeypatch):
    writes = {"n": 0}

    monkeypatch.setattr(job, "get_db_engine", lambda: object())
    monkeypatch.setattr(job, "get_tushare_pro", lambda: object())
    monkeypatch.setattr(job, "ensure_industry_chain_snapshot_cache_table", lambda engine: True)
    monkeypatch.setattr(job, "load_chain_templates", lambda: {"半导体": {}, "AI服务器": {}, "AI算力": {}})
    monkeypatch.setattr(job, "load_chain_snapshot_cache", lambda **kwargs: None)
    monkeypatch.setattr(job, "get_chain_snapshot", lambda **kwargs: _mock_snapshot(kwargs["sector_name"]))
    monkeypatch.setattr(
        job,
        "save_chain_snapshot_cache",
        lambda **kwargs: writes.__setitem__("n", writes["n"] + 1) or True,
    )

    count = job.run_update(
        trade_date="20260324",
        flow_window="5D",
        sectors=["半导体", "AI服务器"],
        dry_run=True,
        force=False,
    )
    assert count == 2
    assert writes["n"] == 0


def test_run_update_skip_existing_when_not_force(monkeypatch):
    calls = {"build": 0, "save": 0}

    monkeypatch.setattr(job, "get_db_engine", lambda: object())
    monkeypatch.setattr(job, "get_tushare_pro", lambda: object())
    monkeypatch.setattr(job, "ensure_industry_chain_snapshot_cache_table", lambda engine: True)
    monkeypatch.setattr(job, "load_chain_templates", lambda: {"半导体": {}})
    monkeypatch.setattr(job, "load_chain_snapshot_cache", lambda **kwargs: {"meta": {}})
    monkeypatch.setattr(
        job,
        "get_chain_snapshot",
        lambda **kwargs: calls.__setitem__("build", calls["build"] + 1) or _mock_snapshot("半导体"),
    )
    monkeypatch.setattr(
        job,
        "save_chain_snapshot_cache",
        lambda **kwargs: calls.__setitem__("save", calls["save"] + 1) or True,
    )

    count = job.run_update(
        trade_date="20260324",
        sectors=["半导体"],
        dry_run=False,
        force=False,
    )
    assert count == 0
    assert calls["build"] == 0
    assert calls["save"] == 0


def test_run_update_force_overwrites_existing(monkeypatch):
    calls = {"build": 0, "save": 0}

    monkeypatch.setattr(job, "get_db_engine", lambda: object())
    monkeypatch.setattr(job, "get_tushare_pro", lambda: object())
    monkeypatch.setattr(job, "ensure_industry_chain_snapshot_cache_table", lambda engine: True)
    monkeypatch.setattr(job, "load_chain_templates", lambda: {"半导体": {}})
    monkeypatch.setattr(job, "load_chain_snapshot_cache", lambda **kwargs: {"meta": {}})
    monkeypatch.setattr(
        job,
        "get_chain_snapshot",
        lambda **kwargs: calls.__setitem__("build", calls["build"] + 1) or _mock_snapshot("半导体"),
    )
    monkeypatch.setattr(
        job,
        "save_chain_snapshot_cache",
        lambda **kwargs: calls.__setitem__("save", calls["save"] + 1) or True,
    )

    count = job.run_update(
        trade_date="20260324",
        sectors=["半导体"],
        dry_run=False,
        force=True,
    )
    assert count == 1
    assert calls["build"] == 1
    assert calls["save"] == 1
