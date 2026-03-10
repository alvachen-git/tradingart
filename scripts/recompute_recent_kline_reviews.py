#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Dict, List

from sqlalchemy import text

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import kline_game as kg


def _to_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def fetch_recent_games(limit: int, user_id: str = "") -> List[Dict]:
    lim = max(1, _to_int(limit, 200))
    uid = str(user_id or "").strip()
    with kg.engine.connect() as conn:
        if uid:
            rows = conn.execute(
                text(
                    """
                    SELECT id, user_id, symbol, symbol_type, game_end_time
                    FROM kline_game_records
                    WHERE status = 'finished'
                      AND end_reason = 'completed'
                      AND user_id = :uid
                    ORDER BY id DESC
                    LIMIT :lim
                    """
                ),
                {"uid": uid, "lim": lim},
            ).mappings().fetchall()
        else:
            rows = conn.execute(
                text(
                    """
                    SELECT id, user_id, symbol, symbol_type, game_end_time
                    FROM kline_game_records
                    WHERE status = 'finished'
                      AND end_reason = 'completed'
                    ORDER BY id DESC
                    LIMIT :lim
                    """
                ),
                {"lim": lim},
            ).mappings().fetchall()
    return [dict(r) for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description="Force recompute recent kline review analysis reports.")
    parser.add_argument("--limit", type=int, default=200, help="Recompute latest N completed games. Default: 200")
    parser.add_argument("--user-id", type=str, default="", help="Only recompute games of this user_id")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Sleep between games in milliseconds")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately when one game fails")
    args = parser.parse_args()

    rows = fetch_recent_games(limit=args.limit, user_id=args.user_id)
    total = len(rows)
    print(f"[REVIEW_RECOMPUTE] target_games={total} limit={args.limit} user_id={args.user_id or 'ALL'}")
    if total == 0:
        return 0

    ok_cnt = 0
    fail_cnt = 0
    skip_cnt = 0
    sleep_s = max(0.0, float(_to_int(args.sleep_ms, 0)) / 1000.0)

    for idx, row in enumerate(rows, start=1):
        gid = _to_int(row.get("id"), 0)
        uid = str(row.get("user_id") or "").strip()
        if gid <= 0 or not uid:
            skip_cnt += 1
            print(f"[REVIEW_RECOMPUTE][{idx}/{total}] skip invalid row: id={row.get('id')} user={row.get('user_id')}")
            continue

        t0 = time.time()
        out = kg.analyze_game_trades(game_id=gid, user_id=uid, force=True, generate_ai=False, force_ai=False)
        cost_ms = int((time.time() - t0) * 1000)
        if out.get("ok"):
            ok_cnt += 1
            print(
                f"[REVIEW_RECOMPUTE][{idx}/{total}] ok "
                f"game_id={gid} user={uid} symbol={row.get('symbol')} cost_ms={cost_ms}"
            )
        else:
            fail_cnt += 1
            print(
                f"[REVIEW_RECOMPUTE][{idx}/{total}] fail "
                f"game_id={gid} user={uid} msg={out.get('message', 'unknown')} cost_ms={cost_ms}"
            )
            if args.stop_on_error:
                break

        if sleep_s > 0:
            time.sleep(sleep_s)

    print(
        f"[REVIEW_RECOMPUTE] done total={total} ok={ok_cnt} fail={fail_cnt} skip={skip_cnt} "
        f"user_id={args.user_id or 'ALL'}"
    )
    return 1 if fail_cnt > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
