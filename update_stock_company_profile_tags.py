from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from llm_compat import ChatTongyiCompat

load_dotenv(override=True)


def get_db_engine():
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME")
    if not all([user, pwd, host, name]):
        return None
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}"
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def get_tushare_pro():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return None
    ts.set_token(token)
    return ts.pro_api()


def build_tag_llm():
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        return None
    model = os.getenv("DOMAIN_TAG_MODEL", "qwen-plus")
    try:
        return ChatTongyiCompat(model=model, api_key=api_key, temperature=0.0, streaming=False)
    except Exception:
        return None


def ensure_stock_company_profile_cache_table(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stock_company_profile_cache (
                  ts_code VARCHAR(16) NOT NULL,
                  company_name VARCHAR(128) DEFAULT '',
                  exchange VARCHAR(16) DEFAULT '',
                  main_business TEXT,
                  business_scope LONGTEXT,
                  domain_tags VARCHAR(255) DEFAULT '',
                  tags_model VARCHAR(64) DEFAULT '',
                  profile_hash VARCHAR(64) DEFAULT '',
                  source_updated_at DATETIME NULL,
                  tags_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY (ts_code),
                  KEY idx_tags_updated_at (tags_updated_at)
                ) DEFAULT CHARSET=utf8mb4
                """
            )
        )


def normalize_domain_tags(tags: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for tag in tags:
        t = str(tag or "").strip()
        t = re.sub(r"[\s\-_/]+", "", t)
        t = re.sub(r"[，。,；;、|]+", "", t)
        if not t:
            continue
        if len(t) < 2:
            continue
        if len(t) > 8:
            t = t[:8]
        if t not in seen:
            out.append(t)
            seen.add(t)
        if len(out) >= 3:
            break
    return out


def fallback_extract_domain_tags(text_value: str) -> List[str]:
    text_l = str(text_value or "").lower()

    mapping = [
        ("芯片设计", ["芯片设计", "ic设计", "soc", "eda", "ip核", "数字芯片", "模拟芯片"]),
        ("晶圆制造", ["晶圆", "代工", "wafer", "fab"]),
        ("封装测试", ["封装", "封测", "测试服务"]),
        ("半导体材料", ["光刻胶", "硅片", "靶材", "材料", "电子特气", "抛光液"]),
        ("半导体设备", ["刻蚀", "薄膜", "清洗", "检测设备", "设备"]),
        ("功率器件", ["igbt", "mosfet", "功率半导体", "功率器件"]),
        ("存储芯片", ["dram", "nand", "存储"]),
        ("传感器", ["传感器", "cmos", "光学"]),
        ("汽车电子", ["车规", "汽车电子", "新能源车"]),
        ("消费电子", ["消费电子", "手机", "pc", "可穿戴"]),
        ("工业控制", ["工业控制", "工控", "自动化"]),
        ("AI算力", ["ai", "算力", "服务器", "数据中心"]),
    ]

    hit = [tag for tag, kws in mapping if any(k in text_l for k in kws)]
    hit = normalize_domain_tags(hit)
    if hit:
        return hit

    # 回退词抽取：抓中文词组
    words = re.findall(r"[\u4e00-\u9fa5]{2,8}", str(text_value or ""))
    words = [w for w in words if w not in {"公司", "业务", "产品", "技术", "服务", "客户", "市场", "以及"}]
    return normalize_domain_tags(words[:6]) or ["综合业务"]


def parse_llm_tags(content: str) -> List[str]:
    s = str(content or "").strip()
    if not s:
        return []

    # 优先 JSON
    if "[" in s and "]" in s:
        try:
            arr = json.loads(s[s.find("[") : s.rfind("]") + 1])
            if isinstance(arr, list):
                return normalize_domain_tags([str(x) for x in arr])
        except Exception:
            pass

    for sep in ["|", ",", "，", "、", "/", ";", "；", "\n"]:
        s = s.replace(sep, "|")
    return normalize_domain_tags([x.strip() for x in s.split("|") if x.strip()])


def ai_extract_domain_tags(text_value: str, llm) -> List[str]:
    if llm is None:
        return []
    prompt = (
        "请根据以下公司主营描述，提取1-3个中文短标签表示其擅长业务领域。"
        "要求：每个标签2-8字，不要解释，不要编号，只输出JSON数组。\n\n"
        f"描述：{text_value[:1500]}"
    )
    try:
        resp = llm.invoke(prompt)
        content = getattr(resp, "content", resp)
        return parse_llm_tags(str(content))
    except Exception:
        return []


def compute_profile_hash(main_business: str, business_scope: str) -> str:
    raw = f"{main_business or ''}\n{business_scope or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_company_profiles(pro) -> pd.DataFrame:
    all_rows = []
    for ex in ["SSE", "SZSE"]:
        df = pro.stock_company(exchange=ex)
        if df is None or df.empty:
            continue
        df = df.copy()
        df["ts_code"] = df["ts_code"].astype(str).str.upper().str.strip()
        df = df[df["ts_code"].str.endswith((".SH", ".SZ"))]
        all_rows.append(df)

    if not all_rows:
        return pd.DataFrame(
            columns=["ts_code", "com_name", "main_business", "business_scope", "exchange"]
        )

    out = pd.concat(all_rows, ignore_index=True)
    out = out.drop_duplicates(subset=["ts_code"], keep="last")
    return out


def load_existing_cache(engine, ts_codes: List[str]) -> pd.DataFrame:
    if not ts_codes:
        return pd.DataFrame(
            columns=["ts_code", "domain_tags", "profile_hash", "tags_updated_at"]
        )

    params = {f"c{i}": code for i, code in enumerate(ts_codes)}
    placeholders = ",".join(f":c{i}" for i in range(len(ts_codes)))
    sql = text(
        f"""
        SELECT ts_code, domain_tags, profile_hash, tags_updated_at
        FROM stock_company_profile_cache
        WHERE ts_code IN ({placeholders})
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def _safe_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("T", " ")
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d"]:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    # 兼容数据库返回带毫秒场景
    try:
        return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def should_refresh(
    existing_row: Optional[Dict[str, Any]],
    profile_hash: str,
    now_dt: datetime,
    refresh_missing: bool,
    refresh_expired: bool,
    expire_days: int,
) -> bool:
    if existing_row is None:
        return True

    old_hash = str(existing_row.get("profile_hash") or "").strip()
    if old_hash != profile_hash:
        return True

    old_tags = str(existing_row.get("domain_tags") or "").strip()
    if refresh_missing and not old_tags:
        return True

    if refresh_expired:
        old_dt = _safe_datetime(existing_row.get("tags_updated_at"))
        if old_dt is None:
            return True
        if (now_dt - old_dt) >= timedelta(days=max(1, int(expire_days))):
            return True

    return False


def pick_candidates(
    profiles_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    refresh_missing: bool,
    refresh_expired: bool,
    expire_days: int,
) -> pd.DataFrame:
    if profiles_df.empty:
        return profiles_df

    existing_map = {}
    if not existing_df.empty:
        for _, row in existing_df.iterrows():
            existing_map[str(row.get("ts_code") or "").strip().upper()] = row.to_dict()

    now_dt = datetime.now()
    selected = []
    for _, row in profiles_df.iterrows():
        code = str(row.get("ts_code") or "").strip().upper()
        mb = str(row.get("main_business") or "")
        bs = str(row.get("business_scope") or "")
        profile_hash = compute_profile_hash(mb, bs)
        old = existing_map.get(code)
        if should_refresh(
            existing_row=old,
            profile_hash=profile_hash,
            now_dt=now_dt,
            refresh_missing=refresh_missing,
            refresh_expired=refresh_expired,
            expire_days=expire_days,
        ):
            r = row.copy()
            r["profile_hash"] = profile_hash
            selected.append(r)

    if not selected:
        return profiles_df.head(0).copy()
    return pd.DataFrame(selected)


def generate_domain_tags(main_business: str, business_scope: str, llm) -> Tuple[List[str], str]:
    raw = (main_business or "") + "\n" + (business_scope or "")
    ai_tags = ai_extract_domain_tags(raw, llm)
    if ai_tags:
        return ai_tags, os.getenv("DOMAIN_TAG_MODEL", "qwen-plus")

    fb = fallback_extract_domain_tags(raw)
    return fb, "fallback-rules"


def upsert_profiles(engine, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    sql = text(
        """
        INSERT INTO stock_company_profile_cache (
            ts_code, company_name, exchange, main_business, business_scope,
            domain_tags, tags_model, profile_hash, source_updated_at, tags_updated_at
        ) VALUES (
            :ts_code, :company_name, :exchange, :main_business, :business_scope,
            :domain_tags, :tags_model, :profile_hash, :source_updated_at, :tags_updated_at
        )
        ON DUPLICATE KEY UPDATE
            company_name=VALUES(company_name),
            exchange=VALUES(exchange),
            main_business=VALUES(main_business),
            business_scope=VALUES(business_scope),
            domain_tags=VALUES(domain_tags),
            tags_model=VALUES(tags_model),
            profile_hash=VALUES(profile_hash),
            source_updated_at=VALUES(source_updated_at),
            tags_updated_at=VALUES(tags_updated_at)
        """
    )

    with engine.begin() as conn:
        for row in rows:
            conn.execute(sql, row)


def run_update(
    refresh_missing: bool,
    refresh_expired: bool,
    expire_days: int,
    limit: int,
) -> int:
    if not refresh_missing and not refresh_expired:
        refresh_missing = True

    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库配置缺失")

    pro = get_tushare_pro()
    if pro is None:
        raise RuntimeError("TUSHARE_TOKEN 缺失")

    ensure_stock_company_profile_cache_table(engine)

    print(
        f"🚀 更新公司业务标签缓存 | refresh_missing={refresh_missing} "
        f"refresh_expired={refresh_expired} expire_days={expire_days} limit={limit}"
    )

    profiles = fetch_company_profiles(pro)
    if profiles.empty:
        raise RuntimeError("未拉取到 stock_company 数据")

    existing = load_existing_cache(engine, profiles["ts_code"].astype(str).tolist())
    candidates = pick_candidates(
        profiles_df=profiles,
        existing_df=existing,
        refresh_missing=refresh_missing,
        refresh_expired=refresh_expired,
        expire_days=expire_days,
    )

    if limit > 0:
        candidates = candidates.head(limit)

    print(f"📊 全量公司={len(profiles)} | 待刷新={len(candidates)}")
    if candidates.empty:
        print("✅ 没有需要刷新的公司")
        return 0

    llm = build_tag_llm()
    if llm is None:
        print("⚠️ 未配置 DASHSCOPE_API_KEY 或模型不可用，将使用规则提取标签")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    upsert_rows: List[Dict[str, Any]] = []
    ai_cnt = 0
    fb_cnt = 0

    for i, (_, row) in enumerate(candidates.iterrows(), 1):
        ts_code = str(row.get("ts_code") or "").strip().upper()
        company_name = str(row.get("com_name") or "").strip()
        exchange = str(row.get("exchange") or "").strip()
        main_business = str(row.get("main_business") or "").strip()
        business_scope = str(row.get("business_scope") or "").strip()
        profile_hash = str(row.get("profile_hash") or compute_profile_hash(main_business, business_scope))

        tags, model_name = generate_domain_tags(main_business, business_scope, llm=llm)
        if model_name == "fallback-rules":
            fb_cnt += 1
        else:
            ai_cnt += 1

        upsert_rows.append(
            {
                "ts_code": ts_code,
                "company_name": company_name,
                "exchange": exchange,
                "main_business": main_business,
                "business_scope": business_scope,
                "domain_tags": "|".join(tags),
                "tags_model": model_name,
                "profile_hash": profile_hash,
                "source_updated_at": now_str,
                "tags_updated_at": now_str,
            }
        )

        if i % 100 == 0:
            print(f"... 已处理 {i}/{len(candidates)}")
            time.sleep(0.1)

    upsert_profiles(engine, upsert_rows)
    print(f"✅ 完成写入 {len(upsert_rows)} 条 | AI={ai_cnt} | fallback={fb_cnt}")
    return len(upsert_rows)


def main():
    parser = argparse.ArgumentParser(description="更新 A 股公司主营信息及业务领域标签缓存")
    parser.add_argument("--refresh-missing", action="store_true", help="刷新缺失标签的公司")
    parser.add_argument("--refresh-expired", action="store_true", help="刷新过期标签的公司")
    parser.add_argument("--days", type=int, default=180, help="过期阈值天数，默认180")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量，0=不限制")
    args = parser.parse_args()

    run_update(
        refresh_missing=args.refresh_missing,
        refresh_expired=args.refresh_expired,
        expire_days=args.days,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
