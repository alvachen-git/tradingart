import argparse

import data_engine as de


def _normalize_date(value):
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else None


def main():
    parser = argparse.ArgumentParser(description="更新跨资产IV温度指数（日更）")
    parser.add_argument("--date", dest="trade_date", default=None, help="指定交易日，格式 YYYYMMDD / YYYY-MM-DD")
    parser.add_argument("--lookback", type=int, default=252, help="IV Rank 回看窗口（日）")
    parser.add_argument("--smooth-span", type=int, default=5, help="EWMA 平滑参数 span")
    parser.add_argument("--backfill-days", type=int, default=0, help="回填历史天数（>0 时执行历史回填）")
    parser.add_argument("--backfill-start", default=None, help="回填起始日期（YYYYMMDD / YYYY-MM-DD）")
    parser.add_argument("--only-missing", action="store_true", help="回填时仅补缺失日期，不重算已存在日期")
    parser.add_argument("--force", action="store_true", help="即使当日已存在记录也强制重算")
    parser.add_argument("--no-persist", action="store_true", help="仅计算不落表")
    args = parser.parse_args()

    target_date = _normalize_date(args.trade_date)
    if target_date is None:
        latest_data_date = _normalize_date(de.get_latest_data_date())
        if latest_data_date:
            target_date = latest_data_date
    if target_date is None:
        print("❌ 更新失败：未找到可用交易日")
        raise SystemExit(1)

    backfill_start = _normalize_date(args.backfill_start)
    if (args.backfill_days and args.backfill_days > 0) or backfill_start:
        stats = de.backfill_cross_asset_iv_index_history(
            end_date=target_date,
            days=max(args.backfill_days, 1) if args.backfill_days else 252,
            start_date=backfill_start,
            lookback=max(args.lookback, 1),
            smooth_span=max(args.smooth_span, 1),
            only_missing=args.only_missing,
        )
        print(
            "📚 历史回填完成 requested={requested} computed={computed} skipped={skipped} range={first}->{last}".format(
                requested=stats.get("requested", 0),
                computed=stats.get("computed", 0),
                skipped=stats.get("skipped", 0),
                first=stats.get("first_date"),
                last=stats.get("last_date"),
            )
        )

    # 性能优化：日更模式下，若当日已存在则直接跳过（除非 --force）
    if not args.no_persist and not args.force:
        existing = de.get_cross_asset_iv_index(end_date=target_date, auto_compute=False)
        if existing.get("trade_date") == target_date:
            print(f"⏭️ 当日({target_date})跨资产IV温度指数已存在，跳过重算")
            raise SystemExit(0)

    payload = de.refresh_cross_asset_iv_index_for_date(
        trade_date=target_date,
        lookback=max(args.lookback, 1),
        smooth_span=max(args.smooth_span, 1),
        persist=not args.no_persist,
    )

    if not payload.get("trade_date"):
        print("❌ 跨资产IV温度指数更新失败：未找到可用交易日数据")
        raise SystemExit(1)

    print("✅ 跨资产IV温度指数更新完成")
    print(
        "trade_date={trade_date} raw={raw:.2f} ewma5={ewma:.2f} coverage={coverage:.1f}% available_weight={weight:.1f} regime={regime}".format(
            trade_date=payload["trade_date"],
            raw=float(payload["index_raw"] or 0.0),
            ewma=float(payload["index_ewma5"] or 0.0),
            coverage=float(payload["coverage_pct"] or 0.0),
            weight=float(payload["available_weight"] or 0.0),
            regime=payload.get("regime") or "无数据",
        )
    )


if __name__ == "__main__":
    main()
