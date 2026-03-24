import pandas as pd
from sqlalchemy import create_engine, text

import industry_chain_tools as tools
from industry_chain_tools import (
    _match_index_codes_by_keywords,
    _apply_company_stage_cap,
    _build_flow_edges,
    _score_stage_relevance,
    _sort_stage_companies,
    calc_fund_signal,
    fetch_stage_members_from_tushare,
    get_chain_snapshot,
    scale_flow_width,
    split_net_flow,
)


def _seed_sqlite(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE daily_stock_screener (
                    trade_date TEXT,
                    ts_code TEXT,
                    name TEXT,
                    industry TEXT,
                    pattern TEXT,
                    ma_trend TEXT,
                    score INTEGER
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE stock_moneyflow_daily (
                    trade_date TEXT,
                    ts_code TEXT,
                    net_mf_amount REAL,
                    main_net_amount REAL,
                    small_mid_net_amount REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE stock_company_profile_cache (
                    ts_code TEXT,
                    company_name TEXT,
                    main_business TEXT,
                    business_scope TEXT,
                    domain_tags TEXT,
                    tags_updated_at TEXT,
                    domain_insight_text TEXT,
                    insight_updated_at TEXT,
                    tech_highlights TEXT,
                    customer_profile TEXT,
                    moat_note TEXT,
                    boundary_risk TEXT
                )
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT INTO daily_stock_screener
                (trade_date, ts_code, name, industry, pattern, ma_trend, score)
                VALUES
                ('20260319','000001.SZ','甲公司','半导体','平台突破','多头',90),
                ('20260319','000002.SZ','乙公司','半导体','上升三法','多头',80),
                ('20260319','000003.SZ','丙公司','半导体','','震荡',70)
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT INTO stock_moneyflow_daily
                (trade_date, ts_code, net_mf_amount, main_net_amount, small_mid_net_amount)
                VALUES
                ('20260319','000001.SZ',0,100,0),
                ('20260318','000001.SZ',0,50,0),
                ('20260319','000002.SZ',0,80,0),
                ('20260318','000002.SZ',0,-20,0),
                ('20260319','000003.SZ',0,-10,0),
                ('20260318','000003.SZ',0,-30,0)
                """
            )
        )

        conn.execute(
            text(
                """
                INSERT INTO stock_company_profile_cache
                (
                    ts_code, company_name, main_business, business_scope, domain_tags, tags_updated_at,
                    domain_insight_text, insight_updated_at, tech_highlights, customer_profile, moat_note, boundary_risk
                )
                VALUES
                (
                    '000001.SZ','甲公司','主营芯片设计','经营范围A','芯片设计|AI算力','2026-03-19 18:00:00',
                    '甲公司聚焦高算力芯片设计，服务云侧与行业客户，依托IP与量产经验形成护城河，需关注先进制程依赖风险。',
                    '2026-03-19 18:30:00',
                    '高算力SoC设计|先进封装协同',
                    '云计算与行业头部客户',
                    'IP复用和量产验证壁垒',
                    '需关注制程与客户集中度'
                ),
                (
                    '000002.SZ','乙公司','主营封测','经营范围B','封装测试','2026-03-19 18:00:00',
                    '',
                    '',
                    '',
                    '',
                    '',
                    ''
                )
                """
            )
        )


def test_calc_fund_signal():
    assert calc_fund_signal(10, 20) == "持续流入"
    assert calc_fund_signal(-1, 20) == "短线分歧"
    assert calc_fund_signal(1, -2) == "反抽修复"
    assert calc_fund_signal(-1, -2) == "持续流出"


def test_get_chain_snapshot_merges_pattern_fund_domain_tags():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _seed_sqlite(engine)

    templates = {
        "半导体": {
            "display_name": "半导体产业链",
            "stages": [
                {"id": "mid_design", "name": "中游-IC设计"},
                {"id": "mid_pack", "name": "中游-封测"},
            ],
            "edges": [["mid_design", "mid_pack"]],
        }
    }
    stage_member_map = {
        "mid_design": [{"ts_code": "000001.SZ", "name": "甲公司"}, {"ts_code": "000003.SZ", "name": "丙公司"}],
        "mid_pack": [{"ts_code": "000002.SZ", "name": "乙公司"}],
    }

    snap = get_chain_snapshot(
        sector_name="半导体",
        limit_per_stage=10,
        engine=engine,
        pro=None,
        templates=templates,
        stage_member_map=stage_member_map,
    )

    assert snap["meta"]["screener_trade_date"] == "20260319"
    assert snap["meta"]["fund_trade_date"] == "20260319"
    assert len(snap["stages"]) == 2

    first_stage = snap["stages"][0]
    assert first_stage["name"] == "中游-IC设计"
    assert len(first_stage["companies"]) == 2

    top = first_stage["companies"][0]
    assert top["ts_code"] == "000001.SZ"
    assert top["pattern"] == "平台突破"
    assert top["main_net_amount_1d"] == 100.0
    assert top["main_net_amount_5d"] == 150.0
    assert top["domain_tags_text"] == "芯片设计 / AI算力"
    assert "domain_insight_text" in top
    assert top["domain_insight_text"].startswith("甲公司聚焦高算力芯片设计")
    assert top["tech_highlights"] == ["高算力SoC设计", "先进封装协同"]
    assert top["customer_profile"] == "云计算与行业头部客户"
    assert top["moat_note"] == "IP复用和量产验证壁垒"
    assert top["boundary_risk"] == "需关注制程与客户集中度"
    assert "insight_updated_at" in top
    assert "fund_signal" in top
    assert "net_flow_5d" in first_stage
    assert "net_flow_5d_history" in first_stage
    assert isinstance(first_stage["net_flow_5d_history"], list)
    assert "flow_in_external" in first_stage
    assert "flow_out_external" in first_stage
    assert "flow_window" in snap["meta"]
    assert "flow_semantics" in snap["meta"]
    assert "fund_history_dates" in snap["meta"]


def test_sort_prefers_market_cap_then_five_day_flow_then_score():
    rows = [
        {"ts_code": "A", "market_cap": 1000, "main_net_amount_5d": 10, "score": 90, "main_net_amount_1d": 1},
        {"ts_code": "B", "market_cap": 1200, "main_net_amount_5d": -100, "score": 10, "main_net_amount_1d": 1},
        {"ts_code": "C", "market_cap": 1000, "main_net_amount_5d": 30, "score": 30, "main_net_amount_1d": 1},
        {"ts_code": "D", "market_cap": 1000, "main_net_amount_5d": 30, "score": 80, "main_net_amount_1d": 1},
    ]
    out = _sort_stage_companies(rows, limit_per_stage=10)
    # B 市值最高，应优先
    assert out[0]["ts_code"] == "B"
    # C 与 D 市值/5D相同，按 score 排
    assert out[1]["ts_code"] == "D"
    assert out[2]["ts_code"] == "C"


def test_split_net_flow_decomposition():
    p, o = split_net_flow(12.3)
    assert p == 12.3
    assert o == 0.0

    p, o = split_net_flow(-8.8)
    assert p == 0.0
    assert o == 8.8

    p, o = split_net_flow(0)
    assert p == 0.0
    assert o == 0.0


def test_internal_flow_allocation_sums_to_positive_inflow():
    stage_results = [
        {"id": "a", "net_flow_5d": 100.0, "net_flow_1d": 0.0},
        {"id": "b", "net_flow_5d": 60.0, "net_flow_1d": 0.0},
        {"id": "c", "net_flow_5d": 20.0, "net_flow_1d": 0.0},
    ]
    edges = [["a", "b"], ["a", "c"]]
    flow_edges, _, _ = _build_flow_edges(stage_results, edges, flow_window="5D")
    internal = [x for x in flow_edges if x["flow_type"] == "internal" and x["source"] == "a"]
    assert len(internal) == 2
    total = sum(float(x["flow_value_abs"]) for x in internal)
    assert abs(total - 100.0) < 1e-6


def test_external_balance_logic():
    stage_results = [
        {"id": "a", "net_flow_5d": 100.0, "net_flow_1d": 0.0},
        {"id": "b", "net_flow_5d": -30.0, "net_flow_1d": 0.0},
        {"id": "c", "net_flow_5d": 10.0, "net_flow_1d": 0.0},
    ]
    edges = [["a", "b"], ["a", "c"]]
    flow_edges, flow_in_external, flow_out_external = _build_flow_edges(
        stage_results, edges, flow_window="5D"
    )
    assert flow_out_external["b"] == 30.0
    # c 只收到 a 的内部流，链外流入应为 0
    assert abs(flow_in_external["c"]) < 1e-6
    # b 自身净流出，不应有链外流入
    assert abs(flow_in_external["b"]) < 1e-6
    # a 无上游承接，净流入应归入链外流入
    assert abs(flow_in_external["a"] - 100.0) < 1e-6
    # 存在 b -> 链外流出 的边
    assert any(
        x["flow_type"] == "external_out" and x["source"] == "b"
        for x in flow_edges
    )


def test_scale_flow_width_log_monotonic():
    w1 = scale_flow_width(10, mode="log")
    w2 = scale_flow_width(100, mode="log")
    w3 = scale_flow_width(1000, mode="log")
    assert w1 < w2 < w3


def test_no_internal_allocation_when_downstream_not_positive():
    stage_results = [
        {"id": "up", "net_flow_5d": 120.0, "net_flow_1d": 0.0},
        {"id": "mid", "net_flow_5d": -50.0, "net_flow_1d": 0.0},
    ]
    edges = [["up", "mid"]]
    flow_edges, flow_in_external, _ = _build_flow_edges(stage_results, edges, flow_window="5D")
    internal = [x for x in flow_edges if x["flow_type"] == "internal"]
    assert len(internal) == 1
    assert abs(float(internal[0]["flow_value_abs"])) < 1e-6
    # 上游净流入无法被下游承接，应体现在链外流入
    assert abs(flow_in_external["up"] - 120.0) < 1e-6


def test_snapshot_contains_three_day_fund_history_metadata():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _seed_sqlite(engine)
    templates = {
        "半导体": {
            "display_name": "半导体产业链",
            "stages": [
                {"id": "up", "name": "上游"},
            ],
            "edges": [],
        }
    }
    stage_member_map = {
        "up": [{"ts_code": "000001.SZ", "name": "甲公司"}],
    }
    snap = get_chain_snapshot(
        sector_name="半导体",
        limit_per_stage=10,
        engine=engine,
        pro=None,
        templates=templates,
        stage_member_map=stage_member_map,
    )
    hist_dates = snap["meta"].get("fund_history_dates", [])
    assert isinstance(hist_dates, list)
    assert hist_dates[0] == "20260319"
    stage_hist = snap["stages"][0].get("net_flow_5d_history", [])
    assert isinstance(stage_hist, list)
    assert stage_hist
    assert stage_hist[0]["trade_date"] == "20260319"


def test_match_index_codes_by_keywords_hits_expected_codes():
    catalog = pd.DataFrame(
        [
            {"ts_code": "990001.TI", "name": "AI服务器"},
            {"ts_code": "990002.TI", "name": "液冷温控"},
            {"ts_code": "990003.TI", "name": "光模块"},
        ]
    )
    matched = _match_index_codes_by_keywords(catalog, ["液冷", "服务器"])
    assert matched == ["990001.TI", "990002.TI"]


def test_match_index_codes_by_keywords_supports_exclude():
    catalog = pd.DataFrame(
        [
            {"ts_code": "990001.TI", "name": "服务器PCB"},
            {"ts_code": "990002.TI", "name": "GPU芯片"},
            {"ts_code": "990003.TI", "name": "CPO高速连接"},
        ]
    )
    matched = _match_index_codes_by_keywords(
        catalog,
        include_keywords=["PCB", "CPO", "GPU"],
        exclude_keywords=["GPU"],
    )
    assert matched == ["990001.TI", "990003.TI"]


def test_dynamic_stage_uses_whitelist_when_keyword_miss(monkeypatch):
    rules = {
        "测试板块": {
            "stage_a": {
                "keywords": ["不会命中"],
                "whitelist_codes": ["990010.TI"],
            }
        }
    }
    monkeypatch.setattr(tools, "AI_CHAIN_DYNAMIC_RULES", rules)

    class FakePro:
        def ths_index(self, **kwargs):
            return pd.DataFrame([{"ts_code": "990001.TI", "name": "AI服务器"}])

        def ths_member(self, ts_code):
            if ts_code == "990010.TI":
                return pd.DataFrame(
                    [{"con_code": "000001.SZ", "con_name": "白名单公司"}]
                )
            return pd.DataFrame(columns=["con_code", "con_name"])

    members, warnings, dynamic_info, _ = fetch_stage_members_from_tushare(
        stages=[{"id": "stage_a", "name": "阶段A", "ths_index_codes": []}],
        pro=FakePro(),
        sector_name="测试板块",
        collect_meta=True,
    )
    assert members["stage_a"][0]["ts_code"] == "000001.SZ"
    assert dynamic_info["stage_a"]["source_mode"] == "whitelist"
    assert any("动态筛选无命中" in w for w in warnings)


def test_stage_relevance_score_filters_chip_from_pcb_stage():
    rule = tools.AI_CHAIN_DYNAMIC_RULES["AI服务器"]["up_pcb_connect"]
    chip_company = {
        "domain_tags_text": "GPU / 芯片设计",
        "domain_insight_text": "公司聚焦AI芯片与加速器设计",
        "main_business": "高性能GPU研发",
        "business_scope": "芯片设计",
        "industry": "半导体",
        "name": "芯片公司",
    }
    pcb_company = {
        "domain_tags_text": "PCB / 连接器",
        "domain_insight_text": "公司深耕服务器主板PCB与高速连接器，并参与CPO配套",
        "main_business": "主板PCB与高速铜缆",
        "business_scope": "连接器",
        "industry": "电子元件",
        "name": "PCB公司",
    }
    assert _score_stage_relevance(chip_company, rule) < 1
    assert _score_stage_relevance(pcb_company, rule) >= 1


def test_company_stage_cap_keeps_at_most_two_stages():
    stage_company_map = {
        "s1": [{"ts_code": "000001.SZ", "stage_relevance_score": 10, "market_cap": 100}],
        "s2": [{"ts_code": "000001.SZ", "stage_relevance_score": 8, "market_cap": 90}],
        "s3": [{"ts_code": "000001.SZ", "stage_relevance_score": 5, "market_cap": 80}],
    }
    removed = _apply_company_stage_cap(stage_company_map, keep_max=2)
    remain = sum(
        1 for rows in stage_company_map.values() for x in rows if x.get("ts_code") == "000001.SZ"
    )
    assert remain == 2
    assert sum(int(v) for v in removed.values()) == 1


def test_ai_sector_snapshot_stable_when_dynamic_source_empty():
    class EmptyPro:
        def ths_index(self, **kwargs):
            return pd.DataFrame(columns=["ts_code", "name"])

        def ths_member(self, ts_code):
            return pd.DataFrame(columns=["con_code", "con_name"])

    snap = get_chain_snapshot(
        sector_name="AI服务器",
        limit_per_stage=10,
        engine=None,
        pro=EmptyPro(),
    )
    assert len(snap["stages"]) == 6
    assert snap["meta"]["member_source_mode"] == "mixed"
    assert isinstance(snap["meta"].get("dynamic_match_info"), dict)
    assert "index_hit_count" in snap["meta"]["dynamic_match_info"].get("up_chip_storage", {})
    assert "candidate_company_count" in snap["meta"]["dynamic_match_info"].get("up_chip_storage", {})
    assert "filtered_company_count" in snap["meta"]["dynamic_match_info"].get("up_chip_storage", {})
    assert "fallback_company_count" in snap["meta"]["dynamic_match_info"].get("up_chip_storage", {})
    assert any("动态筛选无命中" in w for w in snap["meta"].get("warnings", []))
    for stage in snap["stages"]:
        assert "companies" in stage
        assert isinstance(stage["companies"], list)


def test_stage_member_second_level_cache_by_sector_and_trade_date(monkeypatch):
    tools._STAGE_MEMBER_CACHE.clear()
    calls = {"n": 0}

    def _fake_fetch(stages, pro, sector_name="", collect_meta=False):
        calls["n"] += 1
        return (
            {"s1": [{"ts_code": "000001.SZ", "name": "A", "match_source": "dynamic"}]},
            [],
            {"s1": {"source_mode": "dynamic"}},
            "mixed",
        )

    monkeypatch.setattr(tools, "fetch_stage_members_from_tushare", _fake_fetch)
    stages = [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]

    m1, w1, d1, s1 = tools._get_stage_members_cached(stages, pro=object(), sector_name="AI服务器", screener_trade_date="20260324")
    m2, w2, d2, s2 = tools._get_stage_members_cached(stages, pro=object(), sector_name="AI服务器", screener_trade_date="20260324")

    assert calls["n"] == 1
    assert m1 == m2 and w1 == w2 and d1 == d2 and s1 == s2
