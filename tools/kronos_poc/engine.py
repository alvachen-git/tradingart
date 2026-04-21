from __future__ import annotations

import math
import os
import statistics
import sys
from functools import lru_cache
from typing import Iterable, List, Tuple

import numpy as np
import pandas as pd


def _quantile_keys(quantiles: Iterable[float]) -> List[float]:
    return [round(float(q), 4) for q in quantiles]


def _normal_z(q: float) -> float:
    return statistics.NormalDist().inv_cdf(float(q))


def _safe_float(v) -> float:
    try:
        return float(v)
    except Exception:
        return float("nan")


def _core_interval_mass() -> float:
    try:
        mass = float(os.getenv("KRONOS_CORE_MASS", "0.6"))
    except ValueError:
        mass = 0.6
    return min(max(mass, 0.2), 0.9)


def _shortest_sample_interval(values, mass: float | None = None) -> Tuple[float, float]:
    clean = np.asarray(values, dtype=float)
    clean = np.sort(clean[np.isfinite(clean)])
    if clean.size == 0:
        return float("nan"), float("nan")
    if clean.size == 1:
        val = float(clean[0])
        return val, val

    target_mass = _core_interval_mass() if mass is None else min(max(float(mass), 0.2), 0.9)
    count = max(1, int(math.ceil(clean.size * target_mass)))
    if count >= clean.size:
        return float(clean[0]), float(clean[-1])

    widths = clean[count - 1 :] - clean[: clean.size - count + 1]
    start = int(np.argmin(widths))
    end = start + count - 1
    return float(clean[start]), float(clean[end])


class FallbackIntervalEngine:
    """Fallback statistical interval predictor when Kronos is unavailable."""

    name = "fallback-lognormal"

    def predict(self, df: pd.DataFrame, horizon: int, quantiles: Iterable[float]) -> Tuple[List[dict], list[str], dict]:
        q_list = _quantile_keys(quantiles)
        closes = pd.to_numeric(df["close_price"], errors="coerce").dropna()
        if len(closes) < 30:
            raise ValueError("历史收盘价不足，无法进行区间预测")

        last_close = float(closes.iloc[-1])
        log_ret = np.log(closes / closes.shift(1)).dropna()
        mu = float(log_ret.tail(60).mean()) if len(log_ret) else 0.0
        sigma = float(log_ret.tail(60).std(ddof=1)) if len(log_ret) > 1 else 0.0
        sigma = max(sigma, 1e-4)

        preds: List[dict] = []
        for step in range(1, int(horizon) + 1):
            step_mu = mu * step
            step_sigma = sigma * math.sqrt(step)
            row = {"step": step, "target_trade_date": None}
            for q in q_list:
                z = _normal_z(q)
                price_q = last_close * math.exp(step_mu + z * step_sigma)
                row[f"p{int(q * 100):02d}_close"] = round(float(price_q), 6)
            row["core_low_close"] = round(float(last_close * math.exp(step_mu + _normal_z(0.2) * step_sigma)), 6)
            row["core_high_close"] = round(float(last_close * math.exp(step_mu + _normal_z(0.8) * step_sigma)), 6)
            preds.append(row)

        warnings = ["Kronos 未就绪，当前结果为统计学兜底区间（仅用于页面联调与流程验收）。"]
        debug = {"engine_mode": self.name, "mu": mu, "sigma": sigma}
        return preds, warnings, debug


class KronosAdapter:
    """
    Best-effort Kronos adapter.
    Falls back if the repo/package is not available or the runtime API differs.
    """

    name = "kronos-pretrained"

    def __init__(self):
        self._fallback = FallbackIntervalEngine()

    @lru_cache(maxsize=1)
    def _load_pipeline(self):
        repo_path = os.getenv("KRONOS_REPO_PATH")
        if repo_path and repo_path not in sys.path:
            sys.path.insert(0, repo_path)

        last_error = None
        for import_path in [
            ("model.pipeline", "ForecastingPipeline"),
            ("kronos.model.pipeline", "ForecastingPipeline"),
            ("pipeline", "ForecastingPipeline"),
        ]:
            mod_name, cls_name = import_path
            try:
                mod = __import__(mod_name, fromlist=[cls_name])
                cls = getattr(mod, cls_name)
                model_name = os.getenv("KRONOS_MODEL_NAME", "NeoQuasar/Kronos-small")
                try:
                    pipeline = cls.from_pretrained(model_name)
                except TypeError:
                    pipeline = cls.from_pretrained(model_name, device_map="cpu")
                return pipeline
            except Exception as e:
                last_error = e
                continue
        raise RuntimeError(f"无法加载 Kronos ForecastingPipeline: {last_error}")

    @lru_cache(maxsize=1)
    def _load_classic_predictor(self):
        repo_path = os.getenv("KRONOS_REPO_PATH")
        if repo_path and repo_path not in sys.path:
            sys.path.insert(0, repo_path)

        try:
            from model import Kronos, KronosTokenizer, KronosPredictor  # type: ignore
        except Exception as e:
            raise RuntimeError(f"无法导入 Kronos/KronosTokenizer/KronosPredictor: {e}")

        model_name = os.getenv("KRONOS_MODEL_NAME", "NeoQuasar/Kronos-small")
        tokenizer_name = os.getenv("KRONOS_TOKENIZER_NAME", "NeoQuasar/Kronos-Tokenizer-base")
        device = os.getenv("KRONOS_DEVICE")
        max_context = int(os.getenv("KRONOS_MAX_CONTEXT", "512"))
        try:
            tokenizer = KronosTokenizer.from_pretrained(tokenizer_name)
            model = Kronos.from_pretrained(model_name)
            kwargs = {"max_context": max_context}
            if device:
                kwargs["device"] = device
            predictor = KronosPredictor(model, tokenizer, **kwargs)
        except Exception as e:
            raise RuntimeError(f"初始化 KronosPredictor 失败: {e}")
        return predictor

    def _parse_kronos_output(self, raw, horizon: int, quantiles: Iterable[float], last_close: float) -> List[dict]:
        q_list = _quantile_keys(quantiles)
        preds: List[dict] = [{"step": i + 1, "target_trade_date": None} for i in range(int(horizon))]

        # Case A: DataFrame with quantile columns
        if hasattr(raw, "columns") and hasattr(raw, "iloc"):
            df = raw
            for i in range(min(len(df), horizon)):
                for q in q_list:
                    key = f"p{int(q * 100):02d}_close"
                    q_candidates = [q, str(q), f"q{q}", f"p{int(q*100)}"]
                    val = None
                    for c in q_candidates:
                        if c in df.columns:
                            val = df.iloc[i][c]
                            break
                    if val is not None:
                        preds[i][key] = round(_safe_float(val), 6)
            return preds

        # Case B: dict with quantiles list or mapping
        if isinstance(raw, dict):
            if "predictions" in raw and isinstance(raw["predictions"], list):
                rows = raw["predictions"][:horizon]
                for i, r in enumerate(rows):
                    for q in q_list:
                        src_keys = [f"p{int(q*100):02d}_close", str(q), f"q{q}"]
                        for sk in src_keys:
                            if sk in r:
                                preds[i][f"p{int(q * 100):02d}_close"] = round(_safe_float(r[sk]), 6)
                                break
                return preds
            if "quantiles" in raw:
                q_map = raw["quantiles"]
                if isinstance(q_map, dict):
                    for q in q_list:
                        values = q_map.get(q) or q_map.get(str(q))
                        if values is None:
                            continue
                        for i in range(min(horizon, len(values))):
                            preds[i][f"p{int(q * 100):02d}_close"] = round(_safe_float(values[i]), 6)
                    return preds

        # Case C: ndarray/tensor samples => derive quantiles
        if hasattr(raw, "shape") or isinstance(raw, (list, tuple)):
            arr = np.asarray(raw, dtype=float)
            if arr.ndim == 1 and len(arr) >= horizon:
                arr = arr[:horizon][None, :]
            if arr.ndim >= 2:
                # Expect sample x horizon or horizon x sample; pick likely arrangement.
                if arr.shape[-1] == horizon:
                    samples = arr.reshape(-1, horizon)
                elif arr.shape[0] == horizon:
                    samples = arr.T
                else:
                    raise ValueError(f"无法解析 Kronos 输出 shape={arr.shape}")
                for step in range(horizon):
                    step_samples = samples[:, step]
                    for q in q_list:
                        preds[step][f"p{int(q * 100):02d}_close"] = round(float(np.quantile(step_samples, q)), 6)
                    core_low, core_high = _shortest_sample_interval(step_samples)
                    preds[step]["core_low_close"] = round(core_low, 6)
                    preds[step]["core_high_close"] = round(core_high, 6)
                return preds

        raise ValueError(f"无法解析 Kronos 输出类型: {type(raw)}")

    def _future_timestamps(self, last_trade_date: str, horizon: int) -> pd.Series:
        last_ts = pd.to_datetime(str(last_trade_date))
        # Use business-day cadence as a stable default for EOD PoC.
        idx = pd.bdate_range(start=last_ts + pd.Timedelta(days=1), periods=horizon, freq="B")
        return pd.Series(idx)

    def _predict_with_classic_predictor(self, df: pd.DataFrame, horizon: int, quantiles: Iterable[float]) -> Tuple[List[dict], list[str], dict]:
        predictor = self._load_classic_predictor()
        q_list = _quantile_keys(quantiles)
        draws = max(5, int(os.getenv("KRONOS_QUANTILE_DRAWS", "31")))
        temp = float(os.getenv("KRONOS_TEMPERATURE", "1.0"))
        top_p = float(os.getenv("KRONOS_TOP_P", "0.9"))

        local = df.copy()
        if "trade_date" not in local.columns:
            raise ValueError("缺少 trade_date 列，无法构造 Kronos 时间戳")
        x_timestamp = pd.Series(pd.to_datetime(local["trade_date"].astype(str)))
        y_timestamp = self._future_timestamps(str(local["trade_date"].iloc[-1]), horizon)

        x_df = pd.DataFrame(
            {
                "open": pd.to_numeric(local.get("open_price"), errors="coerce"),
                "high": pd.to_numeric(local.get("high_price"), errors="coerce"),
                "low": pd.to_numeric(local.get("low_price"), errors="coerce"),
                "close": pd.to_numeric(local.get("close_price"), errors="coerce"),
                "volume": pd.to_numeric(local.get("vol"), errors="coerce"),
                "amount": pd.to_numeric(local.get("amount"), errors="coerce"),
            }
        )
        x_df["volume"] = x_df["volume"].fillna(0.0)
        # If amount is missing, KronosPredictor can infer from volume; keep zeros to avoid NaN rejection.
        x_df["amount"] = x_df["amount"].fillna(x_df["volume"] * x_df["close"].ffill().fillna(0.0))
        if x_df[["open", "high", "low", "close"]].isnull().any().any():
            raise ValueError("OHLC 存在缺失值，无法进行 Kronos 预测")

        close_draws = []
        for _ in range(draws):
            pred_df = predictor.predict(
                df=x_df,
                x_timestamp=x_timestamp,
                y_timestamp=y_timestamp,
                pred_len=horizon,
                T=temp,
                top_p=top_p,
                sample_count=1,
                verbose=False,
            )
            if "close" not in pred_df.columns:
                raise ValueError("KronosPredictor 输出缺少 close 列")
            close_draws.append(pd.to_numeric(pred_df["close"], errors="coerce").to_numpy(dtype=float))

        samples = np.asarray(close_draws, dtype=float)  # (draws, horizon)
        if samples.ndim != 2 or samples.shape[1] < horizon:
            raise ValueError(f"Kronos 样本形状异常: {samples.shape}")

        preds: List[dict] = []
        for step in range(horizon):
            row = {
                "step": step + 1,
                "target_trade_date": pd.Timestamp(y_timestamp.iloc[step]).strftime("%Y%m%d"),
            }
            step_samples = samples[:, step]
            for q in q_list:
                row[f"p{int(q * 100):02d}_close"] = round(float(np.quantile(step_samples, q)), 6)
            core_low, core_high = _shortest_sample_interval(step_samples)
            row["core_low_close"] = round(core_low, 6)
            row["core_high_close"] = round(core_high, 6)
            preds.append(row)

        warnings = [
            "实验性功能：模型预测区间仅作为辅助视角，不构成交易建议。",
            f"Kronos 分位数由 {draws} 次随机采样近似得到（非模型原生分位数输出）。",
        ]
        debug = {
            "engine_mode": self.name,
            "adapter_input": "ohlcva",
            "backend": "classic_kronos_predictor",
            "draws": draws,
            "core_interval_mass": _core_interval_mass(),
            "temperature": temp,
            "top_p": top_p,
        }
        return preds, warnings, debug

    def predict(self, df: pd.DataFrame, horizon: int, quantiles: Iterable[float]) -> Tuple[List[dict], list[str], dict]:
        last_close = float(pd.to_numeric(df["close_price"], errors="coerce").dropna().iloc[-1])
        try:
            pipeline = self._load_pipeline()
            close_series = pd.Series(pd.to_numeric(df["close_price"], errors="coerce").dropna().tolist())
            # Best-effort invocation across possible APIs.
            raw = None
            if hasattr(pipeline, "predict"):
                try:
                    raw = pipeline.predict(close_series, prediction_length=horizon, quantiles=list(quantiles))
                except TypeError:
                    try:
                        raw = pipeline.predict(close_series, horizon=horizon, quantiles=list(quantiles))
                    except TypeError:
                        raw = pipeline.predict(close_series, horizon)
            elif callable(pipeline):
                raw = pipeline(close_series, horizon=horizon)
            if raw is None:
                raise RuntimeError("Kronos pipeline 返回空结果")

            preds = self._parse_kronos_output(raw, horizon=horizon, quantiles=quantiles, last_close=last_close)
            warnings = ["实验性功能：模型预测区间仅作为辅助视角，不构成交易建议。"]
            debug = {"engine_mode": self.name, "adapter_input": "close_only"}
            return preds, warnings, debug
        except Exception as e:
            pipeline_error = str(e)
            try:
                preds, warnings, debug = self._predict_with_classic_predictor(df, horizon, quantiles)
                warnings.insert(0, f"未匹配到 ForecastingPipeline，已切换到你本地仓库的 KronosPredictor 接口。")
                debug["pipeline_error"] = pipeline_error
                return preds, warnings, debug
            except Exception as e2:
                preds, warnings, debug = self._fallback.predict(df, horizon, quantiles)
                warnings.insert(0, f"Kronos 调用失败，已降级为统计学兜底：{e2}")
                debug["kronos_error"] = str(e2)
                debug["pipeline_error"] = pipeline_error
                return preds, warnings, debug
