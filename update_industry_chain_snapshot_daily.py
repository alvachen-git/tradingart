from __future__ import annotations

import argparse
from datetime import datetime
from typing import List, Optional

from sqlalchemy import text

from industry_chain_tools import (
    ensure_industry_chain_snapshot_cache_table,
    get_chain_snapshot,
    get_db_engine,
    get_tushare_pro,
    load_chain_snapshot_cache,
    load_chain_templates,
    save_chain_snapshot_cache,
)

DEFAULT_SECTORS = [
    "半导体",
    "AI服务器",
    "AI算力",
    "新能源",
    "光伏",
    "航天卫星",
    "机器人",
    "储能",
    "工业母机",
    "创新药",
    "低空经济",
]
SNAPSHOT_BUILD_LIMIT = 20


def normalize_trade_date(raw: Optional[str]) -> str:
    if not raw:
        return ""
    cleaned = str(raw).strip().replace("-", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        raise ValueError(f"非法交易日格式: {raw}, 期望 YYYYMMDD")
    return cleaned


def parse_sectors(raw: Optional[str], templates: dict) -> List[str]:
    if not raw:
        return [x for x in DEFAULT_SECTORS if x in templates]
    tokens = [x.strip() for x in str(raw).replace("，", ",").split(",") if x.strip()]
    seen = set()
    out: List[str] = []
    for s in tokens:
        if s not in templates:
            raise ValueError(f"未配置板块: {s}")
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def detect_trade_date(engine, preferred: str = "") -> str:
    if preferred:
        return preferred
    with engine.connect() as conn:
        fund_date = conn.execute(text("SELECT MAX(trade_date) FROM stock_moneyflow_daily")).scalar()
        if fund_date:
            return str(fund_date).replace("-", "")
        screener_date = conn.execute(text("SELECT MAX(trade_date) FROM daily_stock_screener")).scalar()
        if screener_date:
            return str(screener_date).replace("-", "")
    return datetime.now().strftime("%Y%m%d")


def run_update(
    trade_date: str,
    flow_window: str = "5D",
    sectors: Optional[List[str]] = None,
    dry_run: bool = False,
    force: bool = False,
) -> int:
    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库配置缺失，无法建立连接")

    pro = get_tushare_pro()
    templates = load_chain_templates()
    target_sectors = sectors or [x for x in DEFAULT_SECTORS if x in templates]
    if not target_sectors:
        raise RuntimeError("无可更新板块")

    if not ensure_industry_chain_snapshot_cache_table(engine):
        raise RuntimeError("快照缓存表创建/校验失败")

    actual_trade_date = detect_trade_date(engine=engine, preferred=trade_date)
    print(
        f"🚀 开始更新产业链快照 | trade_date={actual_trade_date} "
        f"flow_window={flow_window} dry_run={dry_run} force={force} sectors={','.join(target_sectors)}"
    )

    success_count = 0
    for sector in target_sectors:
        existing = load_chain_snapshot_cache(
            sector_name=sector,
            flow_window=flow_window,
            trade_date=actual_trade_date,
            engine=engine,
        )
        if existing and not force:
            print(f"⏭️  {sector} 已有 {actual_trade_date} 快照，跳过（可加 --force 覆盖）")
            continue

        snapshot = get_chain_snapshot(
            sector_name=sector,
            limit_per_stage=SNAPSHOT_BUILD_LIMIT,
            engine=engine,
            pro=pro,
            templates=templates,
            flow_window=flow_window,
        )
        meta = snapshot.get("meta") or {}
        snapshot_trade_date = (
            actual_trade_date
            or str(meta.get("fund_trade_date") or "").replace("-", "")
            or str(meta.get("screener_trade_date") or "").replace("-", "")
            or datetime.now().strftime("%Y%m%d")
        )

        if dry_run:
            print(
                f"🧪 [dry-run] {sector} 快照可生成 | trade_date={snapshot_trade_date} "
                f"fund={meta.get('fund_trade_date')} screener={meta.get('screener_trade_date')} "
                f"stages={len(snapshot.get('stages') or [])}"
            )
            success_count += 1
            continue

        ok = save_chain_snapshot_cache(
            trade_date=snapshot_trade_date,
            sector_name=sector,
            flow_window=flow_window,
            snapshot=snapshot,
            engine=engine,
        )
        if not ok:
            raise RuntimeError(f"{sector} 快照写入失败")
        print(
            f"✅ {sector} 快照写入完成 | trade_date={snapshot_trade_date} "
            f"fund={meta.get('fund_trade_date')} screener={meta.get('screener_trade_date')}"
        )
        success_count += 1

    print(f"🏁 产业链快照更新完成，成功板块数: {success_count}/{len(target_sectors)}")
    return success_count


def main() -> None:
    parser = argparse.ArgumentParser(description="每日构建产业链快照（sector + trade_date + flow_window）")
    parser.add_argument("--trade-date", type=str, default=None, help="交易日 YYYYMMDD（默认自动探测）")
    parser.add_argument("--flow-window", type=str, default="5D", help="资金窗口，默认 5D")
    parser.add_argument(
        "--sectors",
        type=str,
        default="半导体,AI服务器,AI算力,新能源,光伏,航天卫星,机器人,储能,工业母机,创新药,低空经济",
        help=(
            "板块列表，逗号分隔，默认 "
            "半导体,AI服务器,AI算力,新能源,光伏,航天卫星,机器人,储能,工业母机,创新药,低空经济"
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="仅构建验证，不写库")
    parser.add_argument("--force", action="store_true", help="覆盖已存在快照")
    args = parser.parse_args()

    templates = load_chain_templates()
    trade_date = normalize_trade_date(args.trade_date)
    sectors = parse_sectors(args.sectors, templates)
    run_update(
        trade_date=trade_date,
        flow_window=args.flow_window,
        sectors=sectors,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
