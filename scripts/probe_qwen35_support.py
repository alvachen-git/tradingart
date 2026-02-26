#!/usr/bin/env python3
"""
Probe whether the current environment supports qwen3.5-plus via:
1) dashscope.Generation (text-generation endpoint)
2) dashscope.MultiModalConversation (multimodal-generation endpoint)
3) langchain_community ChatTongyi

Default mode is offline (no network call): prints versions and source hints.
Use --online to run real API probes (requires DASHSCOPE_API_KEY).
"""

from __future__ import annotations

import argparse
import importlib.metadata
import inspect
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@dataclass
class ProbeResult:
    name: str
    status: str  # PASS / FAIL / SKIP
    detail: str


def _pkg_version(pkg: str) -> str:
    try:
        return importlib.metadata.version(pkg)
    except Exception:
        return "unknown"


def _short_exc(e: BaseException) -> str:
    return f"{e.__class__.__name__}: {e}"


def _truncate(s: str, n: int = 280) -> str:
    return s if len(s) <= n else s[: n - 3] + "..."


def _source_contains(module_or_obj: Any, needle: str) -> bool:
    try:
        src = inspect.getsource(module_or_obj)
        return needle in src
    except Exception:
        return False


def _read_text(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _print_header(args: argparse.Namespace) -> None:
    print("=== Qwen3.5 Support Probe ===")
    print(f"Python: {sys.executable}")
    print(f"CWD: {os.getcwd()}")
    print(f"Model under test: {args.model}")
    print(f"Baseline model: {args.baseline_model}")
    print(f"Mode: {'ONLINE' if args.online else 'OFFLINE'}")
    print()


def inspect_environment(args: argparse.Namespace) -> list[ProbeResult]:
    results: list[ProbeResult] = []

    try:
        import dashscope  # type: ignore

        dashscope_path = getattr(dashscope, "__file__", "")
        dashscope_ver = _pkg_version("dashscope")
        results.append(
            ProbeResult(
                "env.dashscope",
                "PASS",
                f"version={dashscope_ver} file={dashscope_path}",
            )
        )

        ds_src = _read_text(dashscope_path) + _read_text(
            str(Path(dashscope_path).with_name("aigc") / "generation.py")
        ) + _read_text(
            str(Path(dashscope_path).with_name("aigc") / "multimodal_conversation.py")
        )
        contains = args.model in ds_src
        results.append(
            ProbeResult(
                "inspect.dashscope_source",
                "PASS" if contains else "FAIL",
                f"model_string_found={contains}",
            )
        )
    except Exception as e:
        results.append(ProbeResult("env.dashscope", "FAIL", _short_exc(e)))

    try:
        import langchain_community  # type: ignore
        from langchain_community.chat_models import tongyi as lc_tongyi_mod  # type: ignore

        lc_ver = _pkg_version("langchain-community")
        lc_path = getattr(langchain_community, "__file__", "")
        results.append(
            ProbeResult(
                "env.langchain_community",
                "PASS",
                f"version={lc_ver} file={lc_path}",
            )
        )

        contains_model = _source_contains(lc_tongyi_mod, args.model)
        contains_vl_heuristic = _source_contains(lc_tongyi_mod, 'or "vl" in values["model_name"]')
        results.append(
            ProbeResult(
                "inspect.ChatTongyi_source",
                "PASS" if contains_model else "FAIL",
                f"model_string_found={contains_model}, has_vl_heuristic={contains_vl_heuristic}",
            )
        )
    except Exception as e:
        results.append(ProbeResult("env.langchain_community", "FAIL", _short_exc(e)))

    return results


def probe_dashscope_generation(model: str, api_key: str, timeout_s: int, prompt: str) -> ProbeResult:
    try:
        import dashscope  # type: ignore

        resp = dashscope.Generation.call(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            result_format="message",
            temperature=0,
            api_key=api_key,
            timeout=timeout_s,
        )
        status = getattr(resp, "status_code", None)
        code = getattr(resp, "code", None)
        message = getattr(resp, "message", None)
        request_id = getattr(resp, "request_id", None)
        ok = status == 200
        detail = f"status_code={status} code={code} request_id={request_id} message={_truncate(str(message))}"
        return ProbeResult("online.dashscope.Generation", "PASS" if ok else "FAIL", detail)
    except Exception as e:
        return ProbeResult("online.dashscope.Generation", "FAIL", _short_exc(e))


def probe_dashscope_multimodal(model: str, api_key: str, timeout_s: int, prompt: str) -> ProbeResult:
    try:
        import dashscope  # type: ignore

        resp = dashscope.MultiModalConversation.call(
            model=model,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            temperature=0,
            api_key=api_key,
            timeout=timeout_s,
        )
        status = getattr(resp, "status_code", None)
        code = getattr(resp, "code", None)
        message = getattr(resp, "message", None)
        request_id = getattr(resp, "request_id", None)
        ok = status == 200
        detail = f"status_code={status} code={code} request_id={request_id} message={_truncate(str(message))}"
        return ProbeResult("online.dashscope.MultiModalConversation", "PASS" if ok else "FAIL", detail)
    except Exception as e:
        return ProbeResult("online.dashscope.MultiModalConversation", "FAIL", _short_exc(e))


def probe_chat_tongyi(model: str, api_key: str, timeout_s: int, prompt: str) -> ProbeResult:
    try:
        from langchain_community.chat_models import ChatTongyi  # type: ignore

        llm = ChatTongyi(
            model=model,
            api_key=api_key,
            streaming=False,
            temperature=0,
            request_timeout=timeout_s,
        )
        msg = llm.invoke(prompt)
        content = getattr(msg, "content", str(msg))
        return ProbeResult("online.ChatTongyi", "PASS", f"response={_truncate(str(content))}")
    except Exception as e:
        return ProbeResult("online.ChatTongyi", "FAIL", _short_exc(e))


def probe_chat_tongyi_compat(
    model: str, api_key: str, timeout_s: int, prompt: str
) -> ProbeResult:
    try:
        from llm_compat import ChatTongyiCompat  # type: ignore

        llm = ChatTongyiCompat(
            model=model,
            api_key=api_key,
            streaming=False,
            temperature=0,
            request_timeout=timeout_s,
        )
        msg = llm.invoke(prompt)
        content = getattr(msg, "content", str(msg))
        return ProbeResult("online.ChatTongyiCompat", "PASS", f"response={_truncate(str(content))}")
    except Exception as e:
        return ProbeResult("online.ChatTongyiCompat", "FAIL", _short_exc(e))


def run_online_probes(args: argparse.Namespace) -> list[ProbeResult]:
    key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    if not key:
        return [ProbeResult("online.precheck", "FAIL", "Missing DASHSCOPE_API_KEY")]

    results: list[ProbeResult] = []
    targets = [args.model]
    if args.include_baseline and args.baseline_model and args.baseline_model != args.model:
        targets.append(args.baseline_model)

    for model in targets:
        print(f"\n--- Probing model: {model} ---")
        results.append(probe_dashscope_generation(model, key, args.timeout, args.prompt))
        results.append(probe_dashscope_multimodal(model, key, args.timeout, args.prompt))
        results.append(probe_chat_tongyi(model, key, args.timeout, args.prompt))
        results.append(probe_chat_tongyi_compat(model, key, args.timeout, args.prompt))
    return results


def print_results(results: list[ProbeResult]) -> int:
    fail_count = 0
    for r in results:
        if r.status == "FAIL":
            fail_count += 1
        print(f"[{r.status}] {r.name}: {r.detail}")
    return fail_count


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Probe qwen3.5-plus support in dashscope / ChatTongyi")
    p.add_argument("--model", default="qwen3.5-plus", help="Target model to probe")
    p.add_argument("--baseline-model", default="qwen-plus", help="Baseline model for comparison")
    p.add_argument("--include-baseline", action="store_true", help="Also probe baseline model online")
    p.add_argument("--online", action="store_true", help="Run real API calls (requires DASHSCOPE_API_KEY)")
    p.add_argument("--timeout", type=int, default=15, help="Timeout seconds for online calls")
    p.add_argument("--prompt", default="请仅回复OK", help="Probe prompt")
    p.add_argument("--verbose-traceback", action="store_true", help="Print traceback on unexpected top-level error")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    _print_header(args)

    all_results: list[ProbeResult] = []
    all_results.extend(inspect_environment(args))

    if args.online:
        all_results.extend(run_online_probes(args))
    else:
        all_results.append(
            ProbeResult(
                "online.probes",
                "SKIP",
                "Offline mode. Re-run with --online to execute real API probes.",
            )
        )

    fail_count = print_results(all_results)

    # Exit non-zero only when online probes were requested and failed.
    if args.online and fail_count > 0:
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        print("[FATAL] probe script crashed unexpectedly")
        traceback.print_exc()
        raise
