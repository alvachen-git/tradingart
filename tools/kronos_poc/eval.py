from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

from .config import DEFAULT_LOOKBACK, DEFAULT_QUANTILES, PILOT_ASSETS, resolve_pilot_asset
from .data_source import fetch_history
from .engine import KronosAdapter


@dataclass
class EvalConfig:
    lookback_window: int = DEFAULT_LOOKBACK
    horizon: int = 3
    quantiles: Tuple[float, float, float] = DEFAULT_QUANTILES
    max_origins: int | None = None


def _pkey(q: float) -> str:
    return f"p{int(float(q) * 100):02d}_close"


def _safe_mape(actual: np.ndarray, pred: np.ndarray) -> float:
    mask = actual != 0
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs((actual[mask] - pred[mask]) / actual[mask])))


def evaluate_asset(asset_input: str, config: EvalConfig, engine: KronosAdapter | None = None) -> tuple[pd.DataFrame, dict]:
    asset = resolve_pilot_asset(asset_input)
    engine = engine or KronosAdapter()
    # Pull a generous window for rolling evaluation.
    df = fetch_history(asset, lookback_window=3000, safety_margin=0)
    if df.empty or len(df) < config.lookback_window + config.horizon + 20:
        raise RuntimeError(f"{asset.label} 历史数据不足，无法评估（当前 {len(df)} 条）")

    df = df.sort_values("trade_date").reset_index(drop=True)
    origins = list(range(config.lookback_window, len(df) - config.horizon + 1))
    if config.max_origins is not None:
        origins = origins[-int(config.max_origins):]
    rows: List[dict] = []
    q10_key = _pkey(config.quantiles[0])
    q50_key = _pkey(config.quantiles[1])
    q90_key = _pkey(config.quantiles[2])

    for origin in origins:
        context = df.iloc[:origin].tail(config.lookback_window).reset_index(drop=True)
        preds, _, debug = engine.predict(context, horizon=config.horizon, quantiles=config.quantiles)
        engine_mode = (((debug or {}).get("engine_mode")) or ((debug or {}).get("engine"))) or "unknown"
        for step in range(1, config.horizon + 1):
            pred_row = preds[step - 1]
            actual_idx = origin + step - 1
            actual_close = float(df.iloc[actual_idx]["close_price"])
            p10 = float(pred_row[q10_key])
            p50 = float(pred_row[q50_key])
            p90 = float(pred_row[q90_key])
            rows.append(
                {
                    "asset_key": asset.key,
                    "asset_label": asset.label,
                    "asset_type": asset.asset_type,
                    "origin_trade_date": str(df.iloc[origin - 1]["trade_date"]),
                    "target_trade_date": str(df.iloc[actual_idx]["trade_date"]),
                    "step": step,
                    "actual_close": actual_close,
                    "p10_close": p10,
                    "p50_close": p50,
                    "p90_close": p90,
                    "covered_p10_p90": int(p10 <= actual_close <= p90),
                    "abs_err_p50": abs(p50 - actual_close),
                    "band_width": max(0.0, p90 - p10),
                    "engine_mode": engine_mode,
                }
            )

    detail_df = pd.DataFrame(rows)
    actual = detail_df["actual_close"].to_numpy(dtype=float)
    p50 = detail_df["p50_close"].to_numpy(dtype=float)
    summary = {
        "asset_key": asset.key,
        "asset_label": asset.label,
        "asset_type": asset.asset_type,
        "rows": int(len(detail_df)),
        "horizon": config.horizon,
        "lookback_window": config.lookback_window,
        "coverage_p10_p90": float(detail_df["covered_p10_p90"].mean()),
        "mae_p50": float(detail_df["abs_err_p50"].mean()),
        "mape_p50": _safe_mape(actual, p50),
        "avg_band_width": float(detail_df["band_width"].mean()),
        "engine_modes": ",".join(sorted(set(detail_df["engine_mode"].astype(str).tolist()))),
    }
    return detail_df, summary


def evaluate_all_assets(config: EvalConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    detail_frames: List[pd.DataFrame] = []
    summary_rows: List[dict] = []
    engine = KronosAdapter()
    for asset in PILOT_ASSETS:
        detail_df, summary = evaluate_asset(asset.label, config=config, engine=engine)
        detail_frames.append(detail_df)
        summary_rows.append(summary)
    detail_all = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame()
    summary_df = pd.DataFrame(summary_rows)
    return detail_all, summary_df


def build_suitability_markdown(summary_df: pd.DataFrame) -> str:
    if summary_df.empty:
        return "# Kronos PoC Suitability\n\n无数据。\n"
    lines = [
        "# Kronos PoC Suitability",
        "",
        "说明：首期以 `P10-P90` 覆盖率为主，辅以 `P50 MAE` 与区间宽度。",
        "",
        "| 标的 | 覆盖率(P10-P90) | P50 MAE | 平均区间宽度 | 结论建议 |",
        "|---|---:|---:|---:|---|",
    ]
    for _, row in summary_df.sort_values("coverage_p10_p90", ascending=False).iterrows():
        cov = float(row["coverage_p10_p90"])
        if cov >= 0.65:
            verdict = "较适合作为辅助视角"
        elif cov >= 0.5:
            verdict = "可试用，但需谨慎解读"
        else:
            verdict = "不稳定，优先作为压力测试样本"
        lines.append(
            f"| {row['asset_label']} | {cov:.2%} | {float(row['mae_p50']):.4f} | {float(row['avg_band_width']):.4f} | {verdict} |"
        )
    return "\n".join(lines) + "\n"

