from datetime import datetime, timedelta

import pandas as pd

import update_stock_company_profile_tags as profile_tags_mod
from update_stock_company_profile_tags import (
    EvidenceItem,
    compute_profile_hash,
    dedupe_evidences,
    evidence_hash,
    fallback_build_insight,
    fallback_extract_domain_tags,
    is_valid_insight,
    normalize_domain_tags,
    parse_insight_output,
    pick_candidates,
    should_refresh,
    should_refresh_insight,
    load_sector_component_codes,
)


def test_normalize_domain_tags_limits_and_dedup():
    tags = [" 芯片 设计 ", "芯片设计", "功率器件", "超长标签abcdefghi"]
    out = normalize_domain_tags(tags)
    assert out[0] == "芯片设计"
    assert "功率器件" in out
    assert len(out) <= 3


def test_fallback_extract_domain_tags_hits_keywords():
    text_value = "公司主营IGBT和MOSFET功率半导体器件，并提供汽车电子解决方案"
    tags = fallback_extract_domain_tags(text_value)
    assert any(t in tags for t in ["功率器件", "汽车电子"])


def test_should_refresh_rules():
    now_dt = datetime(2026, 3, 20)
    h1 = compute_profile_hash("a", "b")
    h2 = compute_profile_hash("a", "c")

    assert should_refresh(None, h1, now_dt, True, False, 180)

    old = {
        "profile_hash": h1,
        "domain_tags": "芯片设计",
        "tags_updated_at": (now_dt - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S"),
    }
    assert not should_refresh(old, h1, now_dt, True, False, 180)
    assert should_refresh(old, h2, now_dt, True, False, 180)


def test_pick_candidates_with_missing_and_expired():
    profiles = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "main_business": "芯片设计",
                "business_scope": "IC产品",
                "com_name": "A",
            },
            {
                "ts_code": "000002.SZ",
                "main_business": "晶圆制造",
                "business_scope": "代工",
                "com_name": "B",
            },
        ]
    )
    existing = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "domain_tags": "",
                "profile_hash": compute_profile_hash("芯片设计", "IC产品"),
                "tags_updated_at": "2025-01-01 00:00:00",
            }
        ]
    )

    out = pick_candidates(
        profiles_df=profiles,
        existing_df=existing,
        refresh_missing=True,
        refresh_expired=False,
        expire_days=180,
    )
    assert set(out["ts_code"].tolist()) == {"000001.SZ", "000002.SZ"}


def test_dedupe_evidence_by_url_and_title():
    items = [
        EvidenceItem(
            url="https://finance.sina.com.cn/a/1",
            title="标题A",
            domain="finance.sina.com.cn",
            published_at="2026-01-01",
            fetched_at="2026-03-20 10:00:00",
            snippet="x" * 220,
            source_type="media",
        ),
        EvidenceItem(
            url="https://finance.sina.com.cn/a/1",
            title="标题B",
            domain="finance.sina.com.cn",
            published_at="2026-01-01",
            fetched_at="2026-03-20 10:00:00",
            snippet="y" * 220,
            source_type="media",
        ),
        EvidenceItem(
            url="https://cninfo.com.cn/a/2",
            title="标题A",
            domain="cninfo.com.cn",
            published_at="2026-01-02",
            fetched_at="2026-03-20 10:00:00",
            snippet="z" * 220,
            source_type="official",
        ),
    ]

    out = dedupe_evidences(items)
    assert len(out) == 1
    assert out[0].url == "https://finance.sina.com.cn/a/1"


def test_parse_insight_output_quality_check():
    raw = (
        "{"
        "\"domain_tags\":[\"晶圆制造\",\"先进工艺\",\"车规\"],"
        "\"tech_highlights\":[\"成熟节点良率优化\",\"特色工艺平台\"],"
        "\"customer_profile\":\"服务IDM与车规客户\","
        "\"moat_note\":\"长期验证与产能协同\","
        "\"boundary_risk\":\"景气回落与资本开支波动\","
        "\"domain_insight_text\":\"公司在晶圆制造与特色工艺平台上持续投入，技术侧强调成熟节点良率优化、工艺配方迭代与产线协同，能够在多品类产品中保持稳定交付；客户覆盖IDM、车规与工业长期订单，需求结构相对稳健；护城河来自验证周期、质量体系与产能爬坡经验；边界上需关注行业景气波动、资本开支节奏及价格周期对盈利弹性的影响。\","
        "\"confidence\":0.82"
        "}"
    )

    parsed = parse_insight_output(raw)
    assert parsed["domain_tags"][:2] == ["晶圆制造", "先进工艺"]
    assert parsed["tech_highlights"][0] == "成熟节点良率优化"
    assert is_valid_insight(parsed)


def test_fallback_insight_has_length_and_fields():
    out = fallback_build_insight(
        company_name="测试公司",
        main_business="主营晶圆制造与封装测试",
        business_scope="服务车规和工业客户",
        tags=["晶圆制造", "封装测试"],
        evidences=[],
    )

    text_value = out["domain_insight_text"]
    assert 120 <= len(text_value) <= 180
    assert out["customer_profile"]
    assert out["moat_note"]
    assert out["boundary_risk"]


def test_should_refresh_insight_with_days_and_hash():
    now_dt = datetime(2026, 3, 21)
    profile_hash = compute_profile_hash("a", "b")
    item = EvidenceItem(
        url="https://cninfo.com.cn/a/1",
        title="标题A",
        domain="cninfo.com.cn",
        published_at="2026-01-01",
        fetched_at="2026-03-20 10:00:00",
        snippet="x" * 220,
        source_type="official",
    )
    ih = evidence_hash(profile_hash, [item])

    old = {
        "profile_hash": profile_hash,
        "insight_hash": ih,
        "domain_insight_text": "x" * 130,
        "insight_updated_at": "2026-03-19 10:00:00",
    }

    assert not should_refresh_insight(old, profile_hash, ih, now_dt, 90, False)
    assert should_refresh_insight(old, profile_hash, "other", now_dt, 90, False)
    assert should_refresh_insight(old, profile_hash, ih, now_dt, 1, False)
    assert should_refresh_insight(old, profile_hash, ih, now_dt, 90, True)


def test_load_sector_component_codes_supports_dynamic_resolver(monkeypatch):
    templates = {
        "AI服务器": {
            "stages": [
                {"id": "s1", "name": "阶段1", "ths_index_codes": []},
                {"id": "s2", "name": "阶段2", "ths_index_codes": []},
            ]
        }
    }
    monkeypatch.setattr(profile_tags_mod, "load_chain_templates", lambda path=None: templates)
    monkeypatch.setattr(
        profile_tags_mod,
        "fetch_stage_members_from_tushare",
        lambda stages, pro, sector_name, collect_meta=False: (
            {
                "s1": [{"ts_code": "000001.SZ", "name": "A"}],
                "s2": [{"ts_code": "000002.SZ", "name": "B"}],
            },
            [],
        ),
    )

    codes = load_sector_component_codes(object(), "AI服务器")
    assert codes == ["000001.SZ", "000002.SZ"]


def test_load_sector_component_codes_supports_new_sectors(monkeypatch):
    templates = {
        "新能源": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "光伏": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "航天卫星": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "机器人": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "储能": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "工业母机": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "创新药": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "低空经济": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "电力": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "核电": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "军工": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
        "有色金属": {"stages": [{"id": "s1", "name": "阶段1", "ths_index_codes": []}]},
    }
    monkeypatch.setattr(profile_tags_mod, "load_chain_templates", lambda path=None: templates)
    monkeypatch.setattr(
        profile_tags_mod,
        "fetch_stage_members_from_tushare",
        lambda stages, pro, sector_name, collect_meta=False: (
            {"s1": [{"ts_code": "000333.SZ", "name": "测试公司"}]},
            [],
        ),
    )

    for sector in [
        "新能源",
        "光伏",
        "航天卫星",
        "机器人",
        "储能",
        "工业母机",
        "创新药",
        "低空经济",
        "电力",
        "核电",
        "军工",
        "有色金属",
    ]:
        codes = load_sector_component_codes(object(), sector)
        assert codes == ["000333.SZ"]
