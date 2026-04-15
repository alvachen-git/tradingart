#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Direct SQLi regression runner (bypass async chat pipeline).

Why:
- /api/chat depends on async task queue and can timeout even when SQL fixes are correct.
- This script calls data_engine tools/functions directly to verify SQL safety quickly.
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import sys
import types
from pathlib import Path
from typing import Any, Dict, List


SQL_ERROR_PATTERNS = [
    "sql syntax",
    "sqlalchemy.exc",
    "pymysql",
    "programmingerror",
    "operationalerror",
    "traceback",
    "you have an error in your sql",
]


def _norm(s: str) -> str:
    s = str(s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _sim(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, _norm(a), _norm(b)).ratio()


def _has_sql_error(text: str) -> str:
    t = _norm(text)
    for p in SQL_ERROR_PATTERNS:
        if p in t:
            return p
    return ""


def _call_tool(obj: Any, kwargs: Dict[str, Any]) -> str:
    if hasattr(obj, "invoke"):
        out = obj.invoke(kwargs)
    else:
        out = obj(**kwargs)
    return str(out)


def _ensure_project_root_on_syspath() -> Path:
    """
    Make project root importable when running:
      py -3 scripts/sqli_direct_runner.py
    """
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return project_root


def _load_env_fallback(project_root: Path) -> None:
    """
    Best-effort .env loader for thin environments without python-dotenv.
    data_engine expects:
      - parent/.env first
      - fallback current/.env
    """
    candidates = [project_root.parent / ".env", project_root / ".env"]
    loaded = False
    for p in candidates:
        if not p.exists():
            continue
        try:
            for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                key = k.strip()
                val = v.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
            loaded = True
            print(f"[INFO] loaded env fallback from: {p}")
            break
        except Exception as e:
            print(f"[WARN] failed to load env fallback from {p}: {e}")
    if not loaded:
        print("[WARN] no .env found for fallback loader.")


def _install_lightweight_stubs() -> None:
    """
    Install minimal runtime stubs for optional dependencies so this script
    can run in a thin venv.
    """
    # scipy -> provide stats.percentileofscore used by data_engine valuation path
    try:
        import scipy  # noqa: F401
    except ModuleNotFoundError:
        scipy_mod = types.ModuleType("scipy")
        stats_mod = types.ModuleType("scipy.stats")

        def _percentileofscore(a, score, *args, **kwargs):
            try:
                arr = [float(x) for x in list(a) if x is not None]
                if not arr:
                    return 50.0
                less = sum(1 for x in arr if x < float(score))
                equal = sum(1 for x in arr if x == float(score))
                return (less + 0.5 * equal) / len(arr) * 100.0
            except Exception:
                return 50.0

        stats_mod.percentileofscore = _percentileofscore
        scipy_mod.stats = stats_mod
        sys.modules["scipy"] = scipy_mod
        sys.modules["scipy.stats"] = stats_mod
        print("[WARN] scipy not found; using lightweight test stub.")

    # streamlit -> provide cache decorators used at import time
    try:
        import streamlit  # noqa: F401
    except ModuleNotFoundError:
        st_mod = types.ModuleType("streamlit")

        def _decorator_passthrough(fn=None, *dargs, **dkwargs):
            if callable(fn):
                return fn

            def deco(f):
                return f

            return deco

        st_mod.cache_data = _decorator_passthrough
        st_mod.cache_resource = _decorator_passthrough
        st_mod.session_state = {}
        sys.modules["streamlit"] = st_mod
        print("[WARN] streamlit not found; using lightweight test stub.")

    # python-dotenv -> no-op load_dotenv
    try:
        import dotenv  # noqa: F401
    except ModuleNotFoundError:
        dotenv_mod = types.ModuleType("dotenv")
        dotenv_mod.load_dotenv = lambda *a, **k: None
        sys.modules["dotenv"] = dotenv_mod
        print("[WARN] python-dotenv not found; using lightweight test stub.")

    # langchain_core.tools / langchain_core.messages
    try:
        import langchain_core  # noqa: F401
    except ModuleNotFoundError:
        lc_core = types.ModuleType("langchain_core")
        lc_tools = types.ModuleType("langchain_core.tools")
        lc_msgs = types.ModuleType("langchain_core.messages")

        def _tool_decorator(fn=None, *dargs, **dkwargs):
            if callable(fn):
                return fn

            def deco(f):
                return f

            return deco

        class _HumanMessage:
            def __init__(self, content: str = ""):
                self.content = content

        lc_tools.tool = _tool_decorator
        lc_msgs.HumanMessage = _HumanMessage
        lc_core.tools = lc_tools
        lc_core.messages = lc_msgs
        sys.modules["langchain_core"] = lc_core
        sys.modules["langchain_core.tools"] = lc_tools
        sys.modules["langchain_core.messages"] = lc_msgs
        print("[WARN] langchain_core not found; using lightweight test stub.")

    # langchain.agents.create_agent
    try:
        import langchain  # noqa: F401
    except ModuleNotFoundError:
        lc = types.ModuleType("langchain")
        lc_agents = types.ModuleType("langchain.agents")

        class _DummyAgent:
            def invoke(self, *args, **kwargs):
                return {"output": ""}

        def _create_agent(*args, **kwargs):
            return _DummyAgent()

        lc_agents.create_agent = _create_agent
        lc.agents = lc_agents
        sys.modules["langchain"] = lc
        sys.modules["langchain.agents"] = lc_agents
        print("[WARN] langchain not found; using lightweight test stub.")

    # langchain_community.chat_models.ChatTongyi
    try:
        import langchain_community  # noqa: F401
    except ModuleNotFoundError:
        lcc = types.ModuleType("langchain_community")
        lcc_chat_models = types.ModuleType("langchain_community.chat_models")

        class _ChatTongyi:
            def __init__(self, *args, **kwargs):
                pass

            def invoke(self, *args, **kwargs):
                return ""

        lcc_chat_models.ChatTongyi = _ChatTongyi
        lcc.chat_models = lcc_chat_models
        sys.modules["langchain_community"] = lcc
        sys.modules["langchain_community.chat_models"] = lcc_chat_models
        print("[WARN] langchain_community not found; using lightweight test stub.")

    # tushare
    try:
        import tushare  # noqa: F401
    except ModuleNotFoundError:
        ts_mod = types.ModuleType("tushare")
        ts_mod.set_token = lambda *a, **k: None
        ts_mod.pro_api = lambda *a, **k: None
        sys.modules["tushare"] = ts_mod
        print("[WARN] tushare not found; using lightweight test stub.")

    # kline_tools.analyze_kline_pattern
    if "kline_tools" not in sys.modules:
        kl_mod = types.ModuleType("kline_tools")

        class _DummyPatternTool:
            def invoke(self, *args, **kwargs):
                return ""

            def __call__(self, *args, **kwargs):
                return ""

        kl_mod.analyze_kline_pattern = _DummyPatternTool()
        sys.modules["kline_tools"] = kl_mod
        print("[WARN] kline_tools not preloaded; using lightweight test stub.")

    # cross_asset_iv_index symbols imported by data_engine (not used in this test)
    if "cross_asset_iv_index" not in sys.modules:
        cai_mod = types.ModuleType("cross_asset_iv_index")
        cai_mod.CROSS_ASSET_IV_BASKET_VERSION = "stub"
        cai_mod.CROSS_ASSET_IV_MIN_COVERAGE_PCT = 0.0
        cai_mod.backfill_cross_asset_iv_index_history = lambda *a, **k: {}
        cai_mod.get_cross_asset_iv_components = lambda *a, **k: {}
        cai_mod.get_cross_asset_iv_index = lambda *a, **k: {}
        cai_mod.get_cross_asset_iv_index_history = lambda *a, **k: {}
        cai_mod.refresh_cross_asset_iv_index_for_date = lambda *a, **k: {}
        sys.modules["cross_asset_iv_index"] = cai_mod
        print("[WARN] cross_asset_iv_index not preloaded; using lightweight test stub.")

    # risk_index_service symbols imported by data_engine (not used in this test)
    if "risk_index_service" not in sys.modules:
        ris_mod = types.ModuleType("risk_index_service")
        ris_mod.get_geopolitical_risk_history = lambda *a, **k: {}
        ris_mod.get_latest_geopolitical_risk_snapshot = lambda *a, **k: {}
        ris_mod.get_recent_geopolitical_risk_snapshots = lambda *a, **k: {}
        ris_mod.refresh_geopolitical_risk_snapshot = lambda *a, **k: {}
        sys.modules["risk_index_service"] = ris_mod
        print("[WARN] risk_index_service not preloaded; using lightweight test stub.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Direct SQLi regression (no async chat).")
    parser.add_argument("--out-json", default="reports/ai_regression/sqli_direct_result.json")
    parser.add_argument("--pair-sim-min", type=float, default=0.65)
    args = parser.parse_args()

    project_root = _ensure_project_root_on_syspath()
    _install_lightweight_stubs()
    _load_env_fallback(project_root)
    try:
        import data_engine as de  # relies on your local .env DB config
    except ModuleNotFoundError as e:
        print(f"[ERROR] import failed: {e}")
        print(f"[HINT] project_root added to sys.path: {project_root}")
        missing = str(e).split("'")[-2] if "'" in str(e) else ""
        if missing:
            print(f"[HINT] missing dependency: {missing}")
            print(f"[HINT] install with: py -3 -m pip install {missing}")
        print("[HINT] run from repo root and ensure `data_engine.py` exists.")
        return 2

    pairs = [
        {
            "id": "iv_union",
            "normal": (de.get_iv_range_stats, {"symbol": "510300", "start_date": "20260101", "end_date": "20260115"}),
            "attack": (
                de.get_iv_range_stats,
                {"symbol": "510300' UNION SELECT 1,2,3 --", "start_date": "20260101", "end_date": "20260115"},
            ),
            "sim_min": 0.60,
        },
        {
            "id": "broker_or_true",
            "normal": (
                de.search_broker_holdings_on_date,
                {"broker_name": "中信期货", "date": "20260115", "symbol": "RB"},
            ),
            "attack": (
                de.search_broker_holdings_on_date,
                {"broker_name": "中信期货' OR '1'='1", "date": "20260115", "symbol": "RB"},
            ),
            "sim_min": 0.70,
        },
        {
            "id": "holding_drop",
            "normal": (de.tool_analyze_position_change, {"symbol": "RB", "start_date": "20260110", "end_date": "20260115"}),
            "attack": (
                de.tool_analyze_position_change,
                {"symbol": "RB'; DROP TABLE futures_holding; --", "start_date": "20260110", "end_date": "20260115"},
            ),
            "sim_min": 0.45,
        },
    ]

    results: List[Dict[str, Any]] = []
    failed = 0

    for pair in pairs:
        pid = pair["id"]
        sim_min = float(pair.get("sim_min", args.pair_sim_min))

        fn_n, kwargs_n = pair["normal"]
        fn_a, kwargs_a = pair["attack"]

        normal_text = _call_tool(fn_n, kwargs_n)
        attack_text = _call_tool(fn_a, kwargs_a)

        normal_err = _has_sql_error(normal_text)
        attack_err = _has_sql_error(attack_text)
        sim = _sim(normal_text, attack_text)

        passed = (not normal_err) and (not attack_err) and (sim >= sim_min)
        if not passed:
            failed += 1

        results.append(
            {
                "id": pid,
                "normal_kwargs": kwargs_n,
                "attack_kwargs": kwargs_a,
                "normal_sql_error_pattern": normal_err,
                "attack_sql_error_pattern": attack_err,
                "similarity": round(sim, 4),
                "similarity_min": sim_min,
                "passed": passed,
                "normal_preview": normal_text[:500],
                "attack_preview": attack_text[:500],
            }
        )
        print(
            f"[{pid}] pass={passed} sim={sim:.3f} "
            f"normal_sql_err={bool(normal_err)} attack_sql_err={bool(attack_err)}"
        )

    out = {"total": len(results), "failed": failed, "results": results}
    out_path = args.out_json
    import os
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[DONE] {out_path}")

    if failed > 0:
        print(f"[FAIL] failed={failed}/{len(results)}")
        return 2
    print("[PASS] all direct SQLi checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
