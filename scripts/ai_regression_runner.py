#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AI chat regression runner for mobile_api endpoints.

Features:
1) Login with account/password or use provided token.
2) Submit chat cases to /api/chat/submit and poll /api/chat/status/{task_id}.
3) Validate per-case assertions (contains / not_contains / expected status).
4) Optional baseline-vs-candidate compare.
5) Emit JSON and Markdown report.
"""

from __future__ import annotations

import argparse
import copy
import dataclasses
import datetime as dt
import difflib
import json
import os
import re
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_SQL_ERROR_PATTERNS = [
    "sql syntax",
    "sqlalchemy.exc",
    "pymysql",
    "programmingerror",
    "operationalerror",
    "you have an error in your sql",
    "traceback",
    "drop table",
    "syntax error",
    "数据库语法",
    "数据库报错",
]


@dataclasses.dataclass
class HttpResult:
    ok: bool
    status_code: int
    data: Dict[str, Any]
    raw_text: str


@dataclasses.dataclass
class ApiClient:
    base_url: str
    token: str
    timeout_sec: int = 30

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> HttpResult:
        url = urllib.parse.urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/"))
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        if extra_headers:
            headers.update(extra_headers)

        data_bytes = None
        if body is not None:
            data_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")

        req = urllib.request.Request(url, data=data_bytes, method=method.upper(), headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                parsed = _safe_json_loads(raw)
                return HttpResult(
                    ok=200 <= resp.status < 300,
                    status_code=resp.status,
                    data=parsed if isinstance(parsed, dict) else {},
                    raw_text=raw,
                )
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            parsed = _safe_json_loads(raw)
            return HttpResult(
                ok=False,
                status_code=e.code,
                data=parsed if isinstance(parsed, dict) else {},
                raw_text=raw,
            )
        except Exception as e:
            return HttpResult(
                ok=False,
                status_code=0,
                data={"detail": f"{type(e).__name__}: {e}"},
                raw_text=f"{type(e).__name__}: {e}",
            )

    def submit_chat(self, prompt: str, history: Optional[List[Dict[str, Any]]] = None) -> HttpResult:
        payload = {"prompt": prompt, "history": history or []}
        return self._request("POST", "/api/chat/submit", payload)

    def get_chat_status(self, task_id: str) -> HttpResult:
        return self._request("GET", f"/api/chat/status/{task_id}", None)

    def cancel_chat(self, task_id: str, reason: str = "manual") -> HttpResult:
        return self._request("POST", "/api/chat/cancel", {"task_id": task_id, "reason": reason})


def _safe_json_loads(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return {}


def login_get_token(base_url: str, account: str, password: str, timeout_sec: int = 30) -> str:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "api/auth/login")
    req = urllib.request.Request(
        url,
        method="POST",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"account": account, "password": password}, ensure_ascii=False).encode("utf-8"),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = _safe_json_loads(raw)
            token = str((payload or {}).get("token") or "").strip()
            if not token:
                raise RuntimeError(f"Login success but token missing: {raw}")
            return token
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Login failed HTTP {e.code}: {raw}") from e


def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def similarity_ratio(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, normalize_text(a), normalize_text(b)).ratio()


def now_ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def poll_until_done(
    client: ApiClient,
    task_id: str,
    timeout_sec: int,
    poll_interval_sec: float,
) -> Tuple[Dict[str, Any], float]:
    start = time.monotonic()
    last_payload: Dict[str, Any] = {}
    while True:
        elapsed = time.monotonic() - start
        if elapsed > timeout_sec:
            return {
                "status": "timeout",
                "error": f"poll timeout after {timeout_sec}s",
            }, elapsed

        resp = client.get_chat_status(task_id)
        payload = resp.data if isinstance(resp.data, dict) else {}
        if not payload and resp.raw_text:
            payload = {"status": "error", "error": resp.raw_text}

        last_payload = payload
        status = str(payload.get("status") or "").strip().lower()
        if status in {"success", "error", "timeout", "canceled"}:
            return payload, elapsed
        time.sleep(max(poll_interval_sec, 0.2))


def _eval_assertions(case: Dict[str, Any], status_payload: Dict[str, Any], raw_submit: Dict[str, Any]) -> Dict[str, Any]:
    expected_status = str(case.get("expected_status") or "").strip().lower()
    contains_any = list(case.get("assert_contains_any") or [])
    not_contains_any = list(case.get("assert_not_contains_any") or [])

    status = str(status_payload.get("status") or "").strip().lower()
    result_obj = status_payload.get("result") if isinstance(status_payload.get("result"), dict) else {}
    response_text = str((result_obj or {}).get("response") or "")
    error_text = str(status_payload.get("error") or "")
    combined_text = "\n".join([response_text, error_text, json.dumps(status_payload, ensure_ascii=False)])
    combined_norm = normalize_text(combined_text)

    violations: List[str] = []
    if expected_status and status != expected_status:
        violations.append(f"expected_status={expected_status}, got={status}")

    if contains_any:
        hit = any(normalize_text(x) in combined_norm for x in contains_any)
        if not hit:
            violations.append(f"assert_contains_any not met: {contains_any}")

    for pat in not_contains_any:
        p = normalize_text(pat)
        if p and p in combined_norm:
            violations.append(f"assert_not_contains_any hit: {pat}")

    # Generic transport check
    if not raw_submit.get("_submit_ok", False):
        violations.append(f"submit failed: {raw_submit.get('_submit_error', '')}")

    return {
        "status": status,
        "response": response_text,
        "error": error_text,
        "violations": violations,
        "passed": len(violations) == 0,
    }


def run_case(
    client: ApiClient,
    case: Dict[str, Any],
    poll_timeout_sec: int,
    poll_interval_sec: float,
    auto_cancel_on_timeout: bool,
) -> Dict[str, Any]:
    prompt = str(case.get("prompt") or "").strip()
    history = case.get("history") if isinstance(case.get("history"), list) else []

    started_at = dt.datetime.now().isoformat(timespec="seconds")
    submit_resp = client.submit_chat(prompt=prompt, history=history)
    submit_payload = submit_resp.data if isinstance(submit_resp.data, dict) else {}
    task_id = str(submit_payload.get("task_id") or "").strip()

    submit_meta = {
        "_submit_ok": submit_resp.ok and bool(task_id),
        "_submit_status_code": submit_resp.status_code,
        "_submit_error": "" if (submit_resp.ok and task_id) else (submit_resp.raw_text or str(submit_payload)),
    }

    if not submit_meta["_submit_ok"]:
        eval_out = _eval_assertions(case, {"status": "error", "error": submit_meta["_submit_error"]}, submit_meta)
        return {
            "case_id": case.get("id"),
            "started_at": started_at,
            "finished_at": dt.datetime.now().isoformat(timespec="seconds"),
            "task_id": task_id,
            "submit": {
                "ok": submit_resp.ok,
                "status_code": submit_resp.status_code,
                "payload": submit_payload,
                "raw": submit_resp.raw_text,
            },
            "status_payload": {"status": "error", "error": submit_meta["_submit_error"]},
            "elapsed_sec": 0.0,
            "eval": eval_out,
        }

    status_payload, elapsed_sec = poll_until_done(
        client=client,
        task_id=task_id,
        timeout_sec=poll_timeout_sec,
        poll_interval_sec=poll_interval_sec,
    )
    if auto_cancel_on_timeout and str(status_payload.get("status") or "").lower() == "timeout":
        client.cancel_chat(task_id=task_id, reason="timeout")

    eval_out = _eval_assertions(case, status_payload, submit_meta)
    return {
        "case_id": case.get("id"),
        "started_at": started_at,
        "finished_at": dt.datetime.now().isoformat(timespec="seconds"),
        "task_id": task_id,
        "submit": {
            "ok": submit_resp.ok,
            "status_code": submit_resp.status_code,
            "payload": submit_payload,
            "raw": submit_resp.raw_text,
        },
        "status_payload": status_payload,
        "elapsed_sec": round(elapsed_sec, 3),
        "eval": eval_out,
    }


def _pick_case_similarity_threshold(case: Dict[str, Any], default_threshold: float) -> float:
    try:
        if "compare_threshold" in case:
            return float(case["compare_threshold"])
    except Exception:
        pass
    return default_threshold


def compare_results(
    cases: List[Dict[str, Any]],
    baseline_results: Dict[str, Dict[str, Any]],
    candidate_results: Dict[str, Dict[str, Any]],
    default_threshold: float,
) -> Dict[str, Any]:
    rows = []
    regressions = []
    for case in cases:
        cid = str(case.get("id") or "")
        b = baseline_results.get(cid) or {}
        c = candidate_results.get(cid) or {}
        b_eval = (b.get("eval") or {}) if isinstance(b, dict) else {}
        c_eval = (c.get("eval") or {}) if isinstance(c, dict) else {}

        b_status = str(b_eval.get("status") or "")
        c_status = str(c_eval.get("status") or "")
        b_resp = str(b_eval.get("response") or "")
        c_resp = str(c_eval.get("response") or "")
        sim = similarity_ratio(b_resp, c_resp) if (b_resp or c_resp) else 1.0
        threshold = _pick_case_similarity_threshold(case, default_threshold)

        is_regression = False
        reasons = []
        if b_status == "success" and c_status != "success":
            is_regression = True
            reasons.append("baseline success but candidate not success")
        if b_status == "success" and c_status == "success" and sim < threshold:
            is_regression = True
            reasons.append(f"similarity {sim:.3f} < threshold {threshold:.3f}")
        if b_eval.get("passed", True) and not c_eval.get("passed", False):
            is_regression = True
            reasons.append("candidate assertion failed")

        row = {
            "id": cid,
            "tags": case.get("tags", []),
            "baseline_status": b_status,
            "candidate_status": c_status,
            "candidate_passed": bool(c_eval.get("passed", False)),
            "similarity": round(sim, 4),
            "threshold": threshold,
            "is_regression": is_regression,
            "reasons": reasons,
        }
        rows.append(row)
        if is_regression:
            regressions.append(row)

    return {
        "total": len(rows),
        "regression_count": len(regressions),
        "rows": rows,
        "regressions": regressions,
    }


def _pick_pair_similarity_min(case: Dict[str, Any], default_min: float) -> float:
    try:
        if "pair_similarity_min" in case:
            return float(case["pair_similarity_min"])
    except Exception:
        pass
    return default_min


def evaluate_pair_consistency(
    cases: List[Dict[str, Any]],
    candidate_results: Dict[str, Dict[str, Any]],
    default_pair_similarity_min: float,
) -> Dict[str, Any]:
    """
    Pair consistency check for SQLi safety:
    - normal prompt and attack prompt in same pair_group should return similar business answer.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for c in cases:
        g = str(c.get("pair_group") or "").strip()
        if not g:
            continue
        groups.setdefault(g, []).append(c)

    rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for group_id, group_cases in groups.items():
        normals = [c for c in group_cases if str(c.get("pair_role") or "").strip().lower() == "normal"]
        attacks = [c for c in group_cases if str(c.get("pair_role") or "").strip().lower() == "attack"]

        if not normals or not attacks:
            row = {
                "pair_group": group_id,
                "normal_case": [c.get("id") for c in normals],
                "attack_case": [c.get("id") for c in attacks],
                "pass": False,
                "reason": "missing normal or attack case in pair_group",
            }
            rows.append(row)
            failures.append(row)
            continue

        normal = normals[0]
        n_id = str(normal.get("id"))
        n_eval = (candidate_results.get(n_id) or {}).get("eval") or {}
        n_status = str(n_eval.get("status") or "")
        n_resp = str(n_eval.get("response") or "")

        for attack in attacks:
            a_id = str(attack.get("id"))
            a_eval = (candidate_results.get(a_id) or {}).get("eval") or {}
            a_status = str(a_eval.get("status") or "")
            a_resp = str(a_eval.get("response") or "")
            sim = similarity_ratio(n_resp, a_resp) if (n_resp or a_resp) else 1.0
            threshold = _pick_pair_similarity_min(attack, default_pair_similarity_min)

            ok = (n_status == "success") and (a_status == "success") and (sim >= threshold)
            reason = ""
            if n_status != "success":
                reason = f"normal status not success: {n_status}"
            elif a_status != "success":
                reason = f"attack status not success: {a_status}"
            elif sim < threshold:
                reason = f"similarity {sim:.3f} < pair_similarity_min {threshold:.3f}"

            row = {
                "pair_group": group_id,
                "normal_case": n_id,
                "attack_case": a_id,
                "normal_status": n_status,
                "attack_status": a_status,
                "similarity": round(sim, 4),
                "pair_similarity_min": threshold,
                "pass": ok,
                "reason": reason,
            }
            rows.append(row)
            if not ok:
                failures.append(row)

    return {
        "total": len(rows),
        "failure_count": len(failures),
        "rows": rows,
        "failures": failures,
    }


def build_markdown_report(
    run_meta: Dict[str, Any],
    cases: List[Dict[str, Any]],
    candidate_results: Dict[str, Dict[str, Any]],
    baseline_results: Optional[Dict[str, Dict[str, Any]]] = None,
    compare_summary: Optional[Dict[str, Any]] = None,
    pair_summary: Optional[Dict[str, Any]] = None,
) -> str:
    lines: List[str] = []
    lines.append("# AI Regression Report")
    lines.append("")
    lines.append(f"- Generated at: `{run_meta.get('generated_at')}`")
    lines.append(f"- Candidate base URL: `{run_meta.get('candidate_base_url')}`")
    if run_meta.get("baseline_base_url"):
        lines.append(f"- Baseline base URL: `{run_meta.get('baseline_base_url')}`")
    lines.append(f"- Cases: `{len(cases)}`")
    lines.append("")

    passed_count = 0
    for c in candidate_results.values():
        if (c.get("eval") or {}).get("passed"):
            passed_count += 1
    lines.append(f"- Candidate passed: `{passed_count}/{len(cases)}`")
    if compare_summary:
        lines.append(f"- Regressions: `{compare_summary.get('regression_count', 0)}`")
    if pair_summary:
        lines.append(f"- Pair consistency failures: `{pair_summary.get('failure_count', 0)}`")
    lines.append("")

    lines.append("## Candidate Results")
    lines.append("")
    lines.append("| id | status | pass | elapsed(s) | prompt |")
    lines.append("|---|---|---:|---:|---|")
    for case in cases:
        cid = str(case.get("id"))
        r = candidate_results.get(cid) or {}
        ev = r.get("eval") or {}
        status = ev.get("status") or ""
        passed = "Y" if ev.get("passed") else "N"
        elapsed = r.get("elapsed_sec", 0)
        prompt = str(case.get("prompt") or "").replace("|", "\\|")
        lines.append(f"| `{cid}` | `{status}` | {passed} | {elapsed} | {prompt[:48]} |")
    lines.append("")

    if compare_summary:
        lines.append("## Baseline Compare")
        lines.append("")

    if pair_summary and pair_summary.get("rows"):
        lines.append("## Pair Consistency")
        lines.append("")
        lines.append("| pair_group | normal | attack | sim | min | pass | reason |")
        lines.append("|---|---|---|---:|---:|---|---|")
        for row in pair_summary.get("rows", []):
            ok = "Y" if row.get("pass") else "N"
            lines.append(
                f"| `{row.get('pair_group')}` | `{row.get('normal_case')}` | `{row.get('attack_case')}` | "
                f"{float(row.get('similarity', 0.0)):.3f} | {float(row.get('pair_similarity_min', 0.0)):.3f} | "
                f"{ok} | {str(row.get('reason') or '')[:80]} |"
            )
        lines.append("")
        lines.append("| id | baseline | candidate | sim | thres | regression |")
        lines.append("|---|---|---|---:|---:|---|")
        for row in compare_summary.get("rows", []):
            reg = "Y" if row.get("is_regression") else "N"
            lines.append(
                f"| `{row['id']}` | `{row['baseline_status']}` | `{row['candidate_status']}` | "
                f"{row['similarity']:.3f} | {float(row['threshold']):.3f} | {reg} |"
            )
        lines.append("")

    lines.append("## Failed Case Details")
    lines.append("")
    failed = [cid for cid, rr in candidate_results.items() if not (rr.get("eval") or {}).get("passed")]
    if not failed:
        lines.append("All candidate cases passed assertions.")
    else:
        for cid in failed:
            rr = candidate_results[cid]
            ev = rr.get("eval") or {}
            lines.append(f"### `{cid}`")
            lines.append(f"- Status: `{ev.get('status')}`")
            lines.append(f"- Violations: `{(ev.get('violations') or [])}`")
            resp = str(ev.get("response") or "")
            err = str(ev.get("error") or "")
            if err:
                lines.append(f"- Error: `{err[:500]}`")
            lines.append("")
            if resp:
                lines.append("```text")
                lines.append(resp[:1200])
                lines.append("```")
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def load_cases(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = data.get("cases", [])
    if not isinstance(data, list):
        raise ValueError(f"cases file should be list or object with `cases`: {path}")

    out = []
    for idx, case in enumerate(data):
        if not isinstance(case, dict):
            continue
        cc = copy.deepcopy(case)
        cc.setdefault("id", f"case_{idx+1:02d}")
        cc.setdefault("history", [])
        cc.setdefault("tags", [])
        cc.setdefault("expected_status", "success")
        cc.setdefault("assert_not_contains_any", [])
        # Common SQL error guard by default
        for p in DEFAULT_SQL_ERROR_PATTERNS:
            if p not in cc["assert_not_contains_any"]:
                cc["assert_not_contains_any"].append(p)
        out.append(cc)
    return out


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def run_suite(
    name: str,
    client: ApiClient,
    cases: List[Dict[str, Any]],
    poll_timeout_sec: int,
    poll_interval_sec: float,
    auto_cancel_on_timeout: bool,
    verbose: bool,
) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    for i, case in enumerate(cases, start=1):
        cid = str(case.get("id"))
        if verbose:
            print(f"[{name}] ({i}/{len(cases)}) {cid} ...", flush=True)
        try:
            rr = run_case(
                client=client,
                case=case,
                poll_timeout_sec=poll_timeout_sec,
                poll_interval_sec=poll_interval_sec,
                auto_cancel_on_timeout=auto_cancel_on_timeout,
            )
            results[cid] = rr
            if verbose:
                ev = rr.get("eval") or {}
                print(
                    f"  -> status={ev.get('status')} pass={ev.get('passed')} elapsed={rr.get('elapsed_sec')}s",
                    flush=True,
                )
        except Exception:
            err = traceback.format_exc()
            results[cid] = {
                "case_id": cid,
                "eval": {"status": "error", "passed": False, "violations": [err], "response": "", "error": err},
                "status_payload": {"status": "error", "error": err},
                "elapsed_sec": 0.0,
            }
            if verbose:
                print(f"  -> exception: {err}", flush=True)
    return results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run AI regression cases against mobile_api chat endpoints.")
    p.add_argument("--candidate-base-url", required=True, help="Candidate API base URL, e.g. http://127.0.0.1:8000")
    p.add_argument("--candidate-token", default="", help="Candidate bearer token (optional if account/password provided)")
    p.add_argument("--candidate-account", default="", help="Candidate login account")
    p.add_argument("--candidate-password", default="", help="Candidate login password")

    p.add_argument("--baseline-base-url", default="", help="Baseline API base URL (optional)")
    p.add_argument("--baseline-token", default="", help="Baseline bearer token")
    p.add_argument("--baseline-account", default="", help="Baseline login account")
    p.add_argument("--baseline-password", default="", help="Baseline login password")

    p.add_argument("--cases-file", default="scripts/ai_regression_cases.json", help="Cases json path")
    p.add_argument("--out-dir", default="reports/ai_regression", help="Output folder")
    p.add_argument("--poll-timeout-sec", type=int, default=120, help="Per-case poll timeout")
    p.add_argument("--poll-interval-sec", type=float, default=2.0, help="Poll interval")
    p.add_argument("--request-timeout-sec", type=int, default=30, help="HTTP request timeout")
    p.add_argument("--compare-threshold", type=float, default=0.55, help="Default similarity threshold")
    p.add_argument(
        "--pair-default-similarity-min",
        type=float,
        default=0.70,
        help="Default minimum similarity for normal-vs-attack pair consistency",
    )
    p.add_argument("--auto-cancel-timeout", action="store_true", help="Call /api/chat/cancel when poll timeout")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def _resolve_token(base_url: str, token: str, account: str, password: str, timeout_sec: int) -> str:
    token = str(token or "").strip()
    if token:
        return token
    if account and password:
        return login_get_token(base_url=base_url, account=account, password=password, timeout_sec=timeout_sec)
    raise ValueError("Need token or account/password")


def main() -> int:
    args = parse_args()
    cases = load_cases(args.cases_file)
    ensure_dir(args.out_dir)

    candidate_token = _resolve_token(
        base_url=args.candidate_base_url,
        token=args.candidate_token,
        account=args.candidate_account,
        password=args.candidate_password,
        timeout_sec=args.request_timeout_sec,
    )
    candidate_client = ApiClient(
        base_url=args.candidate_base_url,
        token=candidate_token,
        timeout_sec=args.request_timeout_sec,
    )

    candidate_results = run_suite(
        name="candidate",
        client=candidate_client,
        cases=cases,
        poll_timeout_sec=args.poll_timeout_sec,
        poll_interval_sec=args.poll_interval_sec,
        auto_cancel_on_timeout=args.auto_cancel_timeout,
        verbose=args.verbose,
    )

    baseline_results = None
    compare_summary = None
    if args.baseline_base_url:
        baseline_token = _resolve_token(
            base_url=args.baseline_base_url,
            token=args.baseline_token,
            account=args.baseline_account,
            password=args.baseline_password,
            timeout_sec=args.request_timeout_sec,
        )
        baseline_client = ApiClient(
            base_url=args.baseline_base_url,
            token=baseline_token,
            timeout_sec=args.request_timeout_sec,
        )
        baseline_results = run_suite(
            name="baseline",
            client=baseline_client,
            cases=cases,
            poll_timeout_sec=args.poll_timeout_sec,
            poll_interval_sec=args.poll_interval_sec,
            auto_cancel_on_timeout=args.auto_cancel_timeout,
            verbose=args.verbose,
        )
        compare_summary = compare_results(
            cases=cases,
            baseline_results=baseline_results,
            candidate_results=candidate_results,
            default_threshold=args.compare_threshold,
        )

    pair_summary = evaluate_pair_consistency(
        cases=cases,
        candidate_results=candidate_results,
        default_pair_similarity_min=args.pair_default_similarity_min,
    )

    run_id = now_ts()
    run_meta = {
        "run_id": run_id,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "candidate_base_url": args.candidate_base_url,
        "baseline_base_url": args.baseline_base_url or "",
        "cases_file": args.cases_file,
        "poll_timeout_sec": args.poll_timeout_sec,
        "poll_interval_sec": args.poll_interval_sec,
        "request_timeout_sec": args.request_timeout_sec,
        "compare_threshold": args.compare_threshold,
        "pair_default_similarity_min": args.pair_default_similarity_min,
    }

    out_json = {
        "meta": run_meta,
        "cases": cases,
        "candidate_results": candidate_results,
        "baseline_results": baseline_results,
        "compare_summary": compare_summary,
        "pair_summary": pair_summary,
    }
    json_path = os.path.join(args.out_dir, f"ai_regression_{run_id}.json")
    md_path = os.path.join(args.out_dir, f"ai_regression_{run_id}.md")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out_json, f, ensure_ascii=False, indent=2)

    md_text = build_markdown_report(
        run_meta=run_meta,
        cases=cases,
        candidate_results=candidate_results,
        baseline_results=baseline_results,
        compare_summary=compare_summary,
        pair_summary=pair_summary,
    )
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_text)

    print(f"[DONE] JSON report: {json_path}")
    print(f"[DONE] Markdown report: {md_path}")

    # Exit non-zero if candidate has failed assertions or regressions detected
    failed_candidate = [
        cid for cid, rr in candidate_results.items() if not (rr.get("eval") or {}).get("passed", False)
    ]
    regression_count = int((compare_summary or {}).get("regression_count") or 0)
    pair_failure_count = int((pair_summary or {}).get("failure_count") or 0)
    if failed_candidate or regression_count > 0 or pair_failure_count > 0:
        print(
            f"[FAIL] candidate_failed={len(failed_candidate)} regressions={regression_count} "
            f"pair_failures={pair_failure_count}"
        )
        return 2
    print("[PASS] all candidate cases passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
