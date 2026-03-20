from datetime import datetime, timedelta

import pandas as pd

from update_stock_company_profile_tags import (
    compute_profile_hash,
    fallback_extract_domain_tags,
    normalize_domain_tags,
    pick_candidates,
    should_refresh,
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
            {"ts_code": "000001.SZ", "main_business": "芯片设计", "business_scope": "IC产品", "com_name": "A"},
            {"ts_code": "000002.SZ", "main_business": "晶圆制造", "business_scope": "代工", "com_name": "B"},
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
