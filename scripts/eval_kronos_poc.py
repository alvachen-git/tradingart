#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.kronos_poc.eval import EvalConfig, build_suitability_markdown, evaluate_all_assets, evaluate_asset


def main():
    parser = argparse.ArgumentParser(description="Walk-forward evaluation for Kronos PoC interval forecast.")
    parser.add_argument("--symbol", help="只评估单个试点标的（如 沪深300指数/黄金）")
    parser.add_argument("--lookback", type=int, default=120)
    parser.add_argument("--horizon", type=int, default=3)
    parser.add_argument("--max-origins", type=int, default=None, help="限制回测滚动起点数量，加快验证")
    parser.add_argument("--out-dir", default="docs/kronos_poc_eval")
    args = parser.parse_args()

    cfg = EvalConfig(lookback_window=args.lookback, horizon=args.horizon, max_origins=args.max_origins)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.out_dir) / ts
    out_root.mkdir(parents=True, exist_ok=True)

    if args.symbol:
        detail_df, summary = evaluate_asset(args.symbol, config=cfg)
        summary_df = detail_df.groupby("asset_label", as_index=False).agg(
            coverage_p10_p90=("covered_p10_p90", "mean"),
            mae_p50=("abs_err_p50", "mean"),
            avg_band_width=("band_width", "mean"),
        )
        summary_df["asset_label"] = summary["asset_label"]
    else:
        detail_df, summary_df = evaluate_all_assets(cfg)

    detail_path = out_root / "detail.csv"
    summary_path = out_root / "summary.csv"
    detail_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")

    md = build_suitability_markdown(summary_df)
    md_path = out_root / "suitability.md"
    md_path.write_text(md, encoding="utf-8")

    print(f"[OK] 评估完成")
    print(f"- 明细: {detail_path}")
    print(f"- 汇总: {summary_path}")
    print(f"- 结论: {md_path}")


if __name__ == "__main__":
    main()
