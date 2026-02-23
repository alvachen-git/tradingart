"""K-line card roguelike V2 rule engine.

This module is intentionally pure and deterministic when a seed is provided.
No DB or UI calls are allowed here.
"""

from __future__ import annotations

import math
import random
from typing import Any, Dict, List, Optional, Tuple


RULE_VERSION = "card_v2_2_2026_02_23"
BREAKOUT_HISTORY_WINDOW = 15


def _card(
    card_id: str,
    name: str,
    card_type: str,
    tier: int,
    desc: str,
    direction: str = "none",
    **extra: Any,
) -> Tuple[str, Dict[str, Any]]:
    out: Dict[str, Any] = {
        "card_id": card_id,
        "name": name,
        "type": card_type,
        "tier": tier,
        "tag": card_type,
        "direction": direction,
        "desc": desc,
    }
    out.update(extra)
    return card_id, out


CARD_LIBRARY: Dict[str, Dict[str, Any]] = dict(
    [
        _card(
            "short_long_novice",
            "日内短线多-新手",
            "short",
            1,
            "未来5根有1根上涨得1分，失败扣2分。",
            direction="long",
            base_reward=1,
            fail_penalty=2,
            hit_need_bars=3,
            pair_bonus=1,
            streak_conf_bonus=5,
        ),
        _card(
            "short_long_skilled",
            "日内短线多-熟练",
            "short",
            2,
            "未来5根有2根上涨得2分，失败扣1分。",
            direction="long",
            base_reward=2,
            fail_penalty=1,
            hit_need_bars=2,
            pair_bonus=3,
            streak_conf_bonus=5,
        ),
        _card(
            "short_long_veteran",
            "日内短线多-老手",
            "short",
            3,
            "未来5根有2根上涨得3分，失败扣1分。",
            direction="long",
            base_reward=3,
            fail_penalty=1,
            hit_need_bars=2,
            pair_bonus=4,
            streak_conf_bonus=5,
        ),
        _card(
            "short_long_master",
            "日内短线多-大师",
            "short",
            4,
            "未来5根有1根上涨得4分，失败扣1分。",
            direction="long",
            base_reward=4,
            fail_penalty=1,
            hit_need_bars=1,
            pair_bonus=6,
            streak_conf_bonus=10,
        ),
        _card(
            "short_short_novice",
            "日内短线空-新手",
            "short",
            1,
            "未来5根有3根下跌得1分，失败扣2分。",
            direction="short",
            base_reward=1,
            fail_penalty=2,
            hit_need_bars=3,
            pair_bonus=1,
            streak_conf_bonus=5,
        ),
        _card(
            "short_short_skilled",
            "日内短线空-熟练",
            "short",
            2,
            "未来5根有2根下跌得2分，失败扣2分。",
            direction="short",
            base_reward=2,
            fail_penalty=2,
            hit_need_bars=2,
            pair_bonus=3,
            streak_conf_bonus=5,
        ),
        _card(
            "short_short_veteran",
            "日内短线空-老手",
            "short",
            3,
            "未来5根有2根下跌得3分，失败扣1分。",
            direction="short",
            base_reward=3,
            fail_penalty=1,
            hit_need_bars=2,
            pair_bonus=4,
            streak_conf_bonus=5,
        ),
        _card(
            "short_short_master",
            "日内短线空-大师",
            "short",
            4,
            "未来5根有1根下跌得4分，失败扣1分。",
            direction="short",
            base_reward=4,
            fail_penalty=1,
            hit_need_bars=1,
            pair_bonus=6,
            streak_conf_bonus=10,
        ),
        _card(
            "trend_long_novice",
            "顺势做多-新手",
            "trend",
            1,
            "末根高于首根得3+X，失败扣4+X，且动量-2。",
            direction="long",
            base_reward=3,
            fail_penalty=4,
            momentum_gain=1,
            momentum_loss=2,
        ),
        _card(
            "trend_long_skilled",
            "顺势做多-熟练",
            "trend",
            2,
            "末根高于首根得6+X，失败扣6+X，且动量-2。",
            direction="long",
            base_reward=6,
            fail_penalty=6,
            momentum_gain=1,
            momentum_loss=2,
        ),
        _card(
            "trend_long_veteran",
            "顺势做多-老手",
            "trend",
            3,
            "末根高于首根得10+X，失败扣6+X。",
            direction="long",
            base_reward=10,
            fail_penalty=6,
            momentum_gain=2,
        ),
        _card(
            "trend_long_master",
            "顺势做多-大师",
            "trend",
            4,
            "末根高于首根得15+X，失败扣6+X。",
            direction="long",
            base_reward=15,
            fail_penalty=6,
            momentum_gain=2,
        ),
        _card(
            "trend_short_novice",
            "顺势做空-新手",
            "trend",
            1,
            "末根低于首根得3+X，失败扣4+X，且动量-2。",
            direction="short",
            base_reward=3,
            fail_penalty=4,
            momentum_gain=1,
            momentum_loss=2,
        ),
        _card(
            "trend_short_skilled",
            "顺势做空-熟练",
            "trend",
            2,
            "末根低于首根得6+X，失败扣6+X，且动量-2。",
            direction="short",
            base_reward=6,
            fail_penalty=6,
            momentum_gain=1,
            momentum_loss=2,
        ),
        _card(
            "trend_short_veteran",
            "顺势做空-老手",
            "trend",
            3,
            "末根低于首根得10+X，失败扣6+X。",
            direction="short",
            base_reward=10,
            fail_penalty=6,
            momentum_gain=2,
        ),
        _card(
            "trend_short_master",
            "顺势做空-大师",
            "trend",
            4,
            "末根低于首根得15+X，失败扣6+X。",
            direction="short",
            base_reward=15,
            fail_penalty=6,
            momentum_gain=2,
        ),
        _card(
            "breakout_long_novice",
            "突破追多-新手",
            "breakout",
            2,
            "未来5根任一收盘突破最近15根历史最高高点得20分，有动量得30分。",
            direction="long",
            hit_score=20,
            hit_score_with_momentum=30,
            fail_penalty=20,
        ),
        _card(
            "breakout_long_veteran",
            "突破追多-老手",
            "breakout",
            3,
            "未来5根任一收盘突破最近15根历史最高高点得30分，有动量得60分。",
            direction="long",
            hit_score=30,
            hit_score_with_momentum=60,
            fail_penalty=20,
        ),
        _card(
            "breakout_short_novice",
            "突破追空-新手",
            "breakout",
            2,
            "未来5根任一收盘跌破最近15根历史最低低点得20分，有动量得30分。",
            direction="short",
            hit_score=20,
            hit_score_with_momentum=30,
            fail_penalty=20,
        ),
        _card(
            "breakout_short_veteran",
            "突破追空-老手",
            "breakout",
            3,
            "未来5根任一收盘跌破最近15根历史最低低点得30分，有动量得60分。",
            direction="short",
            hit_score=30,
            hit_score_with_momentum=60,
            fail_penalty=20,
        ),
        _card(
            "tactic_quick_cancel",
            "快速撤单",
            "tactic",
            1,
            "下回合额外抽1张牌，可叠加。",
            effect="quick_cancel",
        ),
        _card(
            "tactic_scalp_cycle",
            "剥头皮循环",
            "tactic",
            2,
            "本回合短线小计*1.5，可叠加；若未来5根任一涨跌幅>3%则作废。",
            effect="scalp_cycle",
        ),
        _card(
            "tactic_leverage",
            "借钱加杠杆",
            "tactic",
            3,
            "若本回合有得分则总分*2，否则信心-40。",
            effect="leverage",
        ),
        _card(
            "tactic_risk_control",
            "风险控制",
            "tactic",
            2,
            "本回合得分时*0.6，失分时*0.5。",
            effect="risk_control",
        ),
        _card(
            "tactic_meditation",
            "冥想思考",
            "tactic",
            1,
            "恢复信心5~15。",
            effect="meditation",
        ),
        _card(
            "tactic_dynamic_adjust",
            "动态调整",
            "tactic",
            2,
            "下回合抽牌前先弃掉剩余手牌，再补抽（同回合多张只生效一次）。",
            effect="dynamic_adjust",
        ),
        _card(
            "tactic_self_confidence",
            "自信下单",
            "tactic",
            3,
            "需要信心>=80；结算到该牌时若总分>0则总分*2，否则信心-20。",
            effect="self_confidence",
            min_confidence=80,
        ),
        _card(
            "tactic_fast_stop",
            "快速止损",
            "tactic",
            3,
            "保护后两张牌最终负分；突破牌与买方期权不可保护。",
            effect="fast_stop",
            protect_next_cards=2,
        ),
        _card(
            "arb_east_novice",
            "跨期套利东-新手",
            "arbitrage",
            1,
            "套利成功得2分，失败扣2分。",
            arb_region="east",
            arb_reward=2,
            arb_fail_penalty=2,
        ),
        _card(
            "arb_east_veteran",
            "跨期套利东-老手",
            "arbitrage",
            3,
            "套利成功得3分，失败扣1分。",
            arb_region="east",
            arb_reward=3,
            arb_fail_penalty=1,
        ),
        _card(
            "arb_west_novice",
            "跨期套利西-新手",
            "arbitrage",
            1,
            "套利成功得2分，失败扣2分。",
            arb_region="west",
            arb_reward=2,
            arb_fail_penalty=2,
        ),
        _card(
            "arb_west_veteran",
            "跨期套利西-老手",
            "arbitrage",
            3,
            "套利成功得3分，失败扣1分。",
            arb_region="west",
            arb_reward=3,
            arb_fail_penalty=1,
        ),
        _card(
            "arb_south_novice",
            "跨期套利南-新手",
            "arbitrage",
            1,
            "套利成功得2分，失败扣2分。",
            arb_region="south",
            arb_reward=2,
            arb_fail_penalty=2,
        ),
        _card(
            "arb_south_veteran",
            "跨期套利南-老手",
            "arbitrage",
            3,
            "套利成功得3分，失败扣1分。",
            arb_region="south",
            arb_reward=3,
            arb_fail_penalty=1,
        ),
        _card(
            "arb_north_novice",
            "跨期套利北-新手",
            "arbitrage",
            1,
            "套利成功得2分，失败扣2分。",
            arb_region="north",
            arb_reward=2,
            arb_fail_penalty=2,
        ),
        _card(
            "arb_north_veteran",
            "跨期套利北-老手",
            "arbitrage",
            3,
            "套利成功得3分，失败扣1分。",
            arb_region="north",
            arb_reward=3,
            arb_fail_penalty=1,
        ),
        _card(
            "option_buy_call_novice",
            "买看涨做多-新手",
            "option",
            1,
            "先扣5分；成功得(Y-2)*4，暴击再*2。",
            option_style="buy",
            option_side="call",
            entry_cost=5,
            win_mult=4,
            reward_offset=2,
        ),
        _card(
            "option_buy_call_skilled",
            "买看涨做多-熟练",
            "option",
            2,
            "先扣4分；成功得(Y-2)*5，暴击再*2。",
            option_style="buy",
            option_side="call",
            entry_cost=4,
            win_mult=5,
            reward_offset=2,
        ),
        _card(
            "option_buy_call_veteran",
            "买看涨做多-老手",
            "option",
            3,
            "先扣4分；成功得(Y-2)*6，暴击再*2。",
            option_style="buy",
            option_side="call",
            entry_cost=4,
            win_mult=6,
            reward_offset=2,
        ),
        _card(
            "option_buy_call_master",
            "买看涨做多-大师",
            "option",
            4,
            "先扣3分；成功得(Y-2)*8，暴击再*2。",
            option_style="buy",
            option_side="call",
            entry_cost=3,
            win_mult=8,
            reward_offset=2,
        ),
        _card(
            "option_buy_put_novice",
            "买看跌做空-新手",
            "option",
            1,
            "先扣5分；成功得(Z-2)*4，暴击再*2。",
            option_style="buy",
            option_side="put",
            entry_cost=5,
            win_mult=4,
            reward_offset=2,
        ),
        _card(
            "option_buy_put_skilled",
            "买看跌做空-熟练",
            "option",
            2,
            "先扣4分；成功得(Z-2)*5，暴击再*2。",
            option_style="buy",
            option_side="put",
            entry_cost=4,
            win_mult=5,
            reward_offset=2,
        ),
        _card(
            "option_buy_put_veteran",
            "买看跌做空-老手",
            "option",
            3,
            "先扣4分；成功得(Z-2)*6，暴击再*2。",
            option_style="buy",
            option_side="put",
            entry_cost=4,
            win_mult=6,
            reward_offset=2,
        ),
        _card(
            "option_buy_put_master",
            "买看跌做空-大师",
            "option",
            4,
            "先扣3分；成功得(Z-2)*8，暴击再*2。",
            option_style="buy",
            option_side="put",
            entry_cost=3,
            win_mult=8,
            reward_offset=2,
        ),
        _card(
            "option_sell_call_novice",
            "卖看涨做空-新手",
            "option",
            1,
            "成功得3分；失败扣16，严重失败惩罚倍率x3。",
            option_style="sell",
            option_side="call",
            hit_score=3,
            fail_penalty=16,
            seller_fail_pct=5.0,
            seller_severe_fail_pct=10.0,
            seller_severe_fail_mult=3,
        ),
        _card(
            "option_sell_call_skilled",
            "卖看涨做空-熟练",
            "option",
            2,
            "成功得2分；失败扣10，严重失败惩罚倍率x3。",
            option_style="sell",
            option_side="call",
            hit_score=2,
            fail_penalty=10,
            seller_fail_pct=5.0,
            seller_severe_fail_pct=10.0,
            seller_severe_fail_mult=3,
        ),
        _card(
            "option_sell_call_veteran",
            "卖看涨做空-老手",
            "option",
            3,
            "成功得2分；失败扣8，严重失败惩罚倍率x2。",
            option_style="sell",
            option_side="call",
            hit_score=2,
            fail_penalty=8,
            seller_fail_pct=4.0,
            seller_severe_fail_pct=10.0,
            seller_severe_fail_mult=2,
        ),
        _card(
            "option_sell_call_master",
            "卖看涨做空-大师",
            "option",
            4,
            "成功得2分；失败扣4，严重失败惩罚倍率x2。",
            option_style="sell",
            option_side="call",
            hit_score=2,
            fail_penalty=4,
            seller_fail_pct=3.0,
            seller_severe_fail_pct=10.0,
            seller_severe_fail_mult=2,
        ),
        _card(
            "option_sell_put_novice",
            "卖看跌做多-新手",
            "option",
            1,
            "成功得3分；失败扣16，严重失败惩罚倍率x3。",
            option_style="sell",
            option_side="put",
            hit_score=3,
            fail_penalty=16,
            seller_fail_pct=5.0,
            seller_severe_fail_pct=10.0,
            seller_severe_fail_mult=3,
        ),
        _card(
            "option_sell_put_skilled",
            "卖看跌做多-熟练",
            "option",
            2,
            "成功得2分；失败扣10，严重失败惩罚倍率x3。",
            option_style="sell",
            option_side="put",
            hit_score=2,
            fail_penalty=10,
            seller_fail_pct=5.0,
            seller_severe_fail_pct=10.0,
            seller_severe_fail_mult=3,
        ),
        _card(
            "option_sell_put_veteran",
            "卖看跌做多-老手",
            "option",
            3,
            "成功得2分；失败扣8，严重失败惩罚倍率x3。",
            option_style="sell",
            option_side="put",
            hit_score=2,
            fail_penalty=8,
            seller_fail_pct=4.0,
            seller_severe_fail_pct=10.0,
            seller_severe_fail_mult=3,
        ),
        _card(
            "option_sell_put_master",
            "卖看跌做多-大师",
            "option",
            4,
            "成功得2分；失败扣4，严重失败惩罚倍率x3。",
            option_style="sell",
            option_side="put",
            hit_score=2,
            fail_penalty=4,
            seller_fail_pct=3.0,
            seller_severe_fail_pct=10.0,
            seller_severe_fail_mult=3,
        ),
    ]
)


INITIAL_DECK_TEMPLATE: List[str] = list(CARD_LIBRARY.keys())

# V2 关闭关卡强化体系，保留接口兼容。
STAGE_UPGRADES: Dict[str, Dict[str, object]] = {}


META_UPGRADES: Dict[str, Dict[str, object]] = {
    "confidence_core": {"name": "信心核心", "max_level": 3, "cost": 1},
    "hand_memory": {"name": "手牌记忆", "max_level": 2, "cost": 1},
    "draw_insight": {"name": "抽牌洞察", "max_level": 3, "cost": 1},
}


STAGE_TARGETS: Dict[int, int] = {1: 220, 2: 250, 3: 280, 4: 310, 5: 340}


def _safe_float(v: object, default: float = 0.0) -> float:
    try:
        f = float(v)
    except Exception:
        return default
    if math.isnan(f) or math.isinf(f):
        return default
    return f


def _safe_int(v: object, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _bar_close(bar: Dict[str, object]) -> float:
    return _safe_float(bar.get("close"), 0.0)


def _bar_open(bar: Dict[str, object]) -> float:
    return _safe_float(bar.get("open"), 0.0)


def _bar_high(bar: Dict[str, object]) -> float:
    return _safe_float(bar.get("high"), 0.0)


def _bar_low(bar: Dict[str, object]) -> float:
    return _safe_float(bar.get("low"), 0.0)


def get_stage_target(stage_no: int) -> int:
    return int(STAGE_TARGETS.get(int(stage_no or 1), 340))


def get_turn_threshold(
    stage_no: int,
    turn_no: int,
    run_effects: Optional[Dict[str, object]] = None,
    event_state: Optional[Dict[str, object]] = None,
) -> int:
    # V2 已弃用阈值罚分机制，返回 0 仅用于兼容旧调用方。
    _ = stage_no
    _ = turn_no
    _ = run_effects
    _ = event_state
    return 0


def build_initial_deck(seed: Optional[int] = None) -> List[str]:
    rng = random.Random(seed)
    deck = list(INITIAL_DECK_TEMPLATE)
    rng.shuffle(deck)
    return deck


def draw_cards(
    deck: List[str],
    hand: List[str],
    discard: List[str],
    draw_count: int,
    hand_limit: int,
    run_effects: Optional[Dict[str, object]] = None,
    seed: Optional[int] = None,
) -> Tuple[List[str], List[str], List[str]]:
    rng = random.Random(seed)
    effects = run_effects or {}
    quality = _safe_int(effects.get("draw_quality"), 0)
    deck_work = list(deck or [])
    hand_work = list(hand or [])
    discard_work = list(discard or [])

    for _ in range(max(0, _safe_int(draw_count, 0))):
        if len(hand_work) >= max(1, _safe_int(hand_limit, 1)):
            break
        if not deck_work and discard_work:
            rng.shuffle(discard_work)
            deck_work = discard_work
            discard_work = []
        if not deck_work:
            break

        chosen_idx = len(deck_work) - 1
        if quality > 0 and len(deck_work) > 1:
            sample_size = min(len(deck_work), 1 + quality, 4)
            candidates = rng.sample(list(range(len(deck_work))), sample_size)
            chosen_idx = max(candidates, key=lambda i: _safe_int(CARD_LIBRARY.get(deck_work[i], {}).get("tier"), 1))
        hand_work.append(deck_work.pop(chosen_idx))

    return deck_work, hand_work, discard_work


def extract_features(context_bars: List[Dict[str, object]], future_bars: List[Dict[str, object]]) -> Dict[str, float]:
    context = context_bars or []
    future = future_bars or []
    if not future:
        return {
            "future_has_up": 0.0,
            "future_has_down": 0.0,
            "future_delta_pct": 0.0,
            "future_spike_pct": 0.0,
            "history_high": 0.0,
            "history_low": 0.0,
        }

    has_up = 0.0
    has_down = 0.0
    max_spike = 0.0
    for bar in future:
        o = _bar_open(bar)
        c = _bar_close(bar)
        if c > o:
            has_up = 1.0
        elif c < o:
            has_down = 1.0
        if o > 0:
            max_spike = max(max_spike, abs(c / o - 1.0))

    first_close = _bar_close(future[0])
    last_close = _bar_close(future[-1])
    delta_pct = 0.0
    if first_close > 0:
        delta_pct = (last_close / first_close - 1.0) * 100.0

    history_window = context[-BREAKOUT_HISTORY_WINDOW:] if len(context) > BREAKOUT_HISTORY_WINDOW else context
    highs = [_bar_high(b) for b in history_window if _bar_high(b) > 0]
    lows = [_bar_low(b) for b in history_window if _bar_low(b) > 0]
    return {
        "future_has_up": has_up,
        "future_has_down": has_down,
        "future_delta_pct": delta_pct,
        "future_spike_pct": max_spike * 100.0,
        "history_high": max(highs) if highs else 0.0,
        "history_low": min(lows) if lows else 0.0,
    }


def validate_combo_direction_conflict(card_ids: List[str]) -> Dict[str, object]:
    has_trend_long = False
    has_trend_short = False
    has_breakout_long = False
    has_breakout_short = False
    has_buy_call = False
    has_sell_call = False
    has_buy_put = False
    has_sell_put = False
    arb_regions: Dict[str, str] = {}
    for cid in [str(c).strip() for c in (card_ids or []) if str(c).strip()]:
        card = CARD_LIBRARY.get(cid) or {}
        ctype = str(card.get("type", ""))
        direction = str(card.get("direction", ""))
        if ctype == "trend":
            if direction == "long":
                has_trend_long = True
            elif direction == "short":
                has_trend_short = True
        elif ctype == "breakout":
            if direction == "long":
                has_breakout_long = True
            elif direction == "short":
                has_breakout_short = True
        elif ctype == "option":
            style = str(card.get("option_style", ""))
            side = str(card.get("option_side", ""))
            if style == "buy" and side == "call":
                has_buy_call = True
            elif style == "sell" and side == "call":
                has_sell_call = True
            elif style == "buy" and side == "put":
                has_buy_put = True
            elif style == "sell" and side == "put":
                has_sell_put = True
        elif ctype == "arbitrage":
            region = str(card.get("arb_region", ""))
            if region:
                if region in arb_regions:
                    return {
                        "ok": False,
                        "error_code": "arbitrage_region_duplicate",
                        "message": "套利冲突：同一区域（东/西/南/北）的套利牌不能同回合重复执行。",
                    }
                arb_regions[region] = cid

        if has_trend_long and has_trend_short:
            return {
                "ok": False,
                "error_code": "trend_direction_conflict",
                "message": "方向冲突：顺势做多与顺势做空不能同回合同时执行。",
            }

        if (has_trend_long and has_breakout_short) or (has_trend_short and has_breakout_long):
            return {
                "ok": False,
                "error_code": "trend_breakout_direction_conflict",
                "message": "方向冲突：突破追多不能与顺势做空同回合执行，突破追空不能与顺势做多同回合执行。",
            }

        if has_breakout_long and has_breakout_short:
            return {
                "ok": False,
                "error_code": "breakout_direction_conflict",
                "message": "方向冲突：突破追多与突破追空不能同回合同时执行。",
            }
        if has_buy_call and has_sell_call:
            return {
                "ok": False,
                "error_code": "option_call_direction_conflict",
                "message": "期权冲突：买看涨与卖看涨不能同回合同时执行。",
            }
        if has_buy_put and has_sell_put:
            return {
                "ok": False,
                "error_code": "option_put_direction_conflict",
                "message": "期权冲突：买看跌与卖看跌不能同回合同时执行。",
            }
    return {"ok": True}


def _has_up_down_bars(future_bars: List[Dict[str, object]]) -> Tuple[bool, bool]:
    has_up = False
    has_down = False
    for bar in future_bars:
        o = _bar_open(bar)
        c = _bar_close(bar)
        if c > o:
            has_up = True
        elif c < o:
            has_down = True
    return has_up, has_down


def _trend_x_points(future_bars: List[Dict[str, object]]) -> int:
    if not future_bars:
        return 0
    first_close = _bar_close(future_bars[0])
    last_close = _bar_close(future_bars[-1])
    if first_close <= 0:
        return 0
    x = abs((last_close / first_close - 1.0) * 100.0)
    return max(0, int(math.floor(x)))


def _trend_multiplier(momentum_before: int) -> int:
    if momentum_before >= 10:
        return 5
    if momentum_before >= 5:
        return 3
    if momentum_before >= 3:
        return 2
    return 1


def _future_has_spike_gt_3pct(future_bars: List[Dict[str, object]]) -> bool:
    for bar in future_bars:
        o = _bar_open(bar)
        c = _bar_close(bar)
        if o <= 0:
            continue
        if abs(c / o - 1.0) > 0.03:
            return True
    return False


def _future_has_spike_gt_pct(future_bars: List[Dict[str, object]], pct: float) -> bool:
    threshold = max(0.0, float(pct)) / 100.0
    for bar in future_bars:
        o = _bar_open(bar)
        c = _bar_close(bar)
        if o <= 0:
            continue
        if abs(c / o - 1.0) > threshold:
            return True
    return False


def _future_up_down_counts(future_bars: List[Dict[str, object]]) -> Tuple[int, int]:
    up_count = 0
    down_count = 0
    for bar in future_bars:
        o = _bar_open(bar)
        c = _bar_close(bar)
        if c > o:
            up_count += 1
        elif c < o:
            down_count += 1
    return up_count, down_count


def _history_extrema(context_bars: List[Dict[str, object]]) -> Tuple[Optional[float], Optional[float]]:
    history_window = context_bars[-BREAKOUT_HISTORY_WINDOW:] if len(context_bars) > BREAKOUT_HISTORY_WINDOW else list(context_bars)
    highs = [_bar_high(x) for x in history_window if _bar_high(x) > 0]
    lows = [_bar_low(x) for x in history_window if _bar_low(x) > 0]
    if not highs or not lows:
        return None, None
    return max(highs), min(lows)


def _close_breakout_directions(
    context_bars: List[Dict[str, object]], future_bars: List[Dict[str, object]]
) -> Dict[str, bool]:
    history_high, history_low = _history_extrema(context_bars)
    has_bull = False
    has_bear = False
    for bar in future_bars:
        c = _bar_close(bar)
        if history_high is not None and c > history_high:
            has_bull = True
        if history_low is not None and c < history_low:
            has_bear = True
    return {"bull": has_bull, "bear": has_bear, "history_high": history_high, "history_low": history_low}


def _round_half_up_int(v: float) -> int:
    if v >= 0:
        return int(math.floor(v + 0.5))
    return -int(math.floor(abs(v) + 0.5))


def _buy_option_crit(
    context_bars: List[Dict[str, object]], future_bars: List[Dict[str, object]], side: str
) -> bool:
    if len(future_bars) < 3:
        return False
    prev_context_close = _bar_close(context_bars[-1]) if context_bars else 0.0
    closes: List[float] = [_bar_close(b) for b in future_bars]
    for i in range(2, len(future_bars)):
        c1 = closes[i - 2]
        c2 = closes[i - 1]
        c3 = closes[i]
        before_c1 = prev_context_close if i - 2 == 0 else closes[i - 3]
        if side == "call":
            # 连续上涨按“收盘价连续抬高”定义，不按阳线定义。
            if not (before_c1 > 0 and c1 > before_c1 and c2 > c1 and c3 > c2):
                continue
            prev_close = c2
            if prev_close > 0 and (c3 / prev_close - 1.0) > 0.03:
                return True
        elif side == "put":
            # 连续下跌按“收盘价连续走低”定义，不按阴线定义。
            if not (before_c1 > 0 and c1 < before_c1 and c2 < c1 and c3 < c2):
                continue
            prev_close = c2
            if prev_close > 0 and (c3 / prev_close - 1.0) < -0.03:
                return True
    return False


def _arbitrage_success_multiplier(chain_count: int) -> int:
    if chain_count >= 4:
        return 3
    if chain_count == 3:
        return 2
    return 1


def _arbitrage_segments(cards: List[str]) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    current_cards: List[Tuple[int, str, str]] = []

    def flush() -> None:
        nonlocal current_cards
        if not current_cards:
            return
        dedup_regions: List[str] = []
        prev_region = ""
        for _, _, region in current_cards:
            if region != prev_region:
                dedup_regions.append(region)
            prev_region = region
        chain_count = len(dedup_regions)
        segments.append(
            {
                "start_order": current_cards[0][0],
                "end_order": current_cards[-1][0],
                "orders": [x[0] for x in current_cards],
                "card_ids": [x[1] for x in current_cards],
                "regions_raw": [x[2] for x in current_cards],
                "regions_dedup": dedup_regions,
                "chain_count": chain_count,
                "success_multiplier": _arbitrage_success_multiplier(chain_count),
            }
        )
        current_cards = []

    for i, cid in enumerate(cards):
        card = CARD_LIBRARY.get(cid) or {}
        if str(card.get("type", "")) == "arbitrage":
            current_cards.append((i + 1, cid, str(card.get("arb_region", ""))))
        else:
            flush()
    flush()
    return segments


def _apply_multiplier(contribs: List[Dict[str, Any]], mult: float, kinds: Optional[List[str]] = None) -> float:
    delta = 0.0
    allow = None if kinds is None else set(kinds)
    for item in contribs:
        if allow is not None and str(item.get("kind", "")) not in allow:
            continue
        before = float(item.get("amount", 0.0))
        after = before * mult
        item["amount"] = after
        delta += after - before
    return delta


def _sum_contribs(contribs: List[Dict[str, Any]]) -> float:
    return float(sum(float(x.get("amount", 0.0)) for x in contribs))


def _short_pair_bonus(cards: List[str]) -> Tuple[int, List[Dict[str, object]]]:
    total = 0
    details: List[Dict[str, object]] = []
    # 按队列从上到下逐对结算，允许重叠窗口：
    # 例如 A-B-C 会结算 (A,B) 与 (B,C) 两次配对。
    for i in range(len(cards) - 1):
        cid_a = cards[i]
        cid_b = cards[i + 1]
        a = CARD_LIBRARY.get(cid_a) or {}
        b = CARD_LIBRARY.get(cid_b) or {}
        if a.get("type") == "short" and b.get("type") == "short":
            da = str(a.get("direction", ""))
            db = str(b.get("direction", ""))
            if da and db and da != db:
                bonus = _safe_int(a.get("pair_bonus"), 0)
                if bonus > 0:
                    total += bonus
                    details.append(
                        {
                            "pair_start_order": i + 1,
                            "cards": [cid_a, cid_b],
                            "bonus": bonus,
                        }
                    )
    return total, details


def _short_streak_confidence_bonus(cards: List[str]) -> int:
    best = 0
    streak_cards: List[Dict[str, Any]] = []
    streak_direction = ""
    for cid in cards:
        card = CARD_LIBRARY.get(cid) or {}
        if card.get("type") == "short":
            direction = str(card.get("direction", ""))
            if streak_cards and direction != streak_direction:
                streak_cards = []
                streak_direction = ""
            streak_cards.append(card)
            streak_direction = direction
            if len(streak_cards) >= 3 and streak_direction in ("long", "short"):
                candidate = max(_safe_int(one.get("streak_conf_bonus"), 0) for one in streak_cards)
                best = max(best, candidate)
        else:
            streak_cards = []
            streak_direction = ""
    return best


def resolve_turn_combo(
    card_ids: List[str],
    context_bars: List[Dict[str, object]],
    future_bars: List[Dict[str, object]],
    stage_no: int,
    run_effects: Optional[Dict[str, object]] = None,
    event_state: Optional[Dict[str, object]] = None,
    seed: Optional[int] = None,
) -> Dict[str, object]:
    _ = stage_no
    _ = event_state
    rng = random.Random(seed)
    cards = [str(c).strip() for c in (card_ids or []) if str(c).strip()]
    run_effects = dict(run_effects or {})
    momentum_before = _safe_int(run_effects.get("momentum"), 0)
    trend_mult = _trend_multiplier(momentum_before)
    if not cards:
        return {
            "turn_score": 0,
            "detail": {"reason": "pass", "features": extract_features(context_bars, future_bars)},
            "card_results": [],
            "mechanics": {
                "rule_version": RULE_VERSION,
                "momentum_before": momentum_before,
                "momentum_after": momentum_before,
                "confidence_delta_from_cards": 0,
                "extra_draw_next_turn_gain": 0,
                "trend_multiplier": trend_mult,
                "tactic_chain": [],
                "short_pair_bonus": 0,
                "short_streak_conf_bonus": 0,
                "short_breakout_misfire_applied": False,
                "short_breakout_direction": "none",
                "arbitrage_segments": [],
                "arbitrage_volatility_gt_3pct": False,
                "arbitrage_volatility_gt_5pct": False,
                "dynamic_adjust_next_turn": False,
                "self_confidence_checks": [],
                "fast_stop_trace": [],
            },
        }

    conflict = validate_combo_direction_conflict(cards)
    if not conflict.get("ok", False):
        return {
            "ok": False,
            "error_code": conflict.get("error_code", "trend_direction_conflict"),
            "message": conflict.get("message", "方向冲突"),
        }

    run_confidence = _safe_int(run_effects.get("_confidence_current"), _safe_int(run_effects.get("confidence"), 0))
    has_up, has_down = _has_up_down_bars(future_bars)
    up_count, down_count = _future_up_down_counts(future_bars)
    trend_x = _trend_x_points(future_bars)
    breakout_info = _close_breakout_directions(context_bars, future_bars)
    history_high = breakout_info.get("history_high")
    history_low = breakout_info.get("history_low")
    volatility_gt_3 = _future_has_spike_gt_pct(future_bars, 3.0)
    volatility_gt_5 = _future_has_spike_gt_pct(future_bars, 5.0)

    future_highs = [_bar_high(bar) for bar in future_bars if _bar_high(bar) > 0]
    future_lows = [_bar_low(bar) for bar in future_bars if _bar_low(bar) > 0]
    future_closes = [_bar_close(bar) for bar in future_bars if _bar_close(bar) > 0]
    future_high = max(future_highs) if future_highs else None
    future_low = min(future_lows) if future_lows else None
    future_close_high = max(future_closes) if future_closes else None
    future_close_low = min(future_closes) if future_closes else None

    has_breakout_long_card = False
    has_breakout_short_card = False
    has_short_long = False
    has_short_short = False
    for cid in cards:
        card = CARD_LIBRARY.get(cid) or {}
        if str(card.get("type", "")) == "breakout":
            if str(card.get("direction", "")) == "long":
                has_breakout_long_card = True
            elif str(card.get("direction", "")) == "short":
                has_breakout_short_card = True
        elif str(card.get("type", "")) == "short":
            if str(card.get("direction", "")) == "long":
                has_short_long = True
            elif str(card.get("direction", "")) == "short":
                has_short_short = True

    misfire_long = bool(breakout_info.get("bear")) and has_short_long and (not has_breakout_short_card)
    misfire_short = bool(breakout_info.get("bull")) and has_short_short and (not has_breakout_long_card)
    short_misfire_applied = misfire_long or misfire_short
    if bool(breakout_info.get("bull")) and bool(breakout_info.get("bear")):
        short_breakout_direction = "both"
    elif bool(breakout_info.get("bull")):
        short_breakout_direction = "long_breakout"
    elif bool(breakout_info.get("bear")):
        short_breakout_direction = "short_breakout"
    else:
        short_breakout_direction = "none"

    arb_segments = _arbitrage_segments(cards)
    arb_segment_by_order: Dict[int, Dict[str, Any]] = {}
    for seg_idx, seg in enumerate(arb_segments):
        seg["segment_id"] = seg_idx + 1
        for order in seg["orders"]:
            arb_segment_by_order[int(order)] = seg

    contribs: List[Dict[str, Any]] = []
    card_results: List[Dict[str, object]] = []
    card_results_by_order: Dict[int, Dict[str, object]] = {}
    trend_gain = 0
    trend_loss = 0

    def add_contrib(order: int, amount: float, kind: str, card_type: str, card_id: str, **extra: Any) -> None:
        if abs(float(amount)) < 1e-12:
            return
        node: Dict[str, Any] = {
            "order": int(order),
            "amount": float(amount),
            "kind": str(kind),
            "card_type": str(card_type),
            "card_id": str(card_id),
        }
        node.update(extra)
        contribs.append(node)

    for idx, cid in enumerate(cards):
        order = idx + 1
        card = CARD_LIBRARY.get(cid) or {}
        ctype = str(card.get("type", "unknown"))
        direction = str(card.get("direction", "none"))
        one: Dict[str, object] = {
            "order": order,
            "card_id": cid,
            "card_name": card.get("name", cid),
            "card_type": ctype,
            "hit": False,
            "raw_score": 0,
        }

        if ctype == "short":
            need = max(1, _safe_int(card.get("hit_need_bars"), 1))
            hit_count = up_count if direction == "long" else down_count
            hit = hit_count >= need
            score = _safe_int(card.get("base_reward"), 0) if hit else -_safe_int(card.get("fail_penalty"), 0)
            one["hit"] = bool(hit)
            one["raw_score"] = score
            one["hit_need_bars"] = need
            one["hit_count_bars"] = hit_count
            one["short_breakout_misfire"] = bool(short_misfire_applied)
            one["short_breakout_misfire_immune"] = bool(
                (direction == "long" and has_breakout_short_card) or (direction == "short" and has_breakout_long_card)
            )
            if not short_misfire_applied:
                add_contrib(order, float(score), "short", ctype, cid)
        elif ctype == "trend":
            first_close = _bar_close(future_bars[0]) if future_bars else 0.0
            last_close = _bar_close(future_bars[-1]) if future_bars else 0.0
            hit = first_close > 0 and ((direction == "long" and last_close > first_close) or (direction == "short" and last_close < first_close))
            trend_delta_pct = (last_close / first_close - 1.0) * 100.0 if first_close > 0 else 0.0
            raw_score = 0
            if hit:
                raw_score = _safe_int(card.get("base_reward"), 0) + trend_x
                trend_gain = max(trend_gain, max(0, _safe_int(card.get("momentum_gain"), 0)))
            else:
                raw_score = -(_safe_int(card.get("fail_penalty"), 0) + trend_x)
                trend_loss = max(trend_loss, max(0, _safe_int(card.get("momentum_loss"), 1)))
            one["hit"] = bool(hit)
            one["raw_score"] = raw_score
            one["x"] = trend_x
            one["first_close"] = first_close
            one["last_close"] = last_close
            one["trend_delta_pct"] = trend_delta_pct
            add_contrib(order, float(raw_score), "trend", ctype, cid)
        elif ctype == "breakout":
            hit = False
            if direction == "long" and history_high is not None and future_close_high is not None:
                hit = future_close_high > float(history_high)
            elif direction == "short" and history_low is not None and future_close_low is not None:
                hit = future_close_low < float(history_low)
            score = _safe_int(card.get("hit_score_with_momentum"), 0) if (hit and momentum_before > 0) else (
                _safe_int(card.get("hit_score"), 0) if hit else -_safe_int(card.get("fail_penalty"), 0)
            )
            one["hit"] = bool(hit)
            one["raw_score"] = score
            one["history_high"] = history_high if history_high is not None else 0.0
            one["history_low"] = history_low if history_low is not None else 0.0
            one["future_high"] = future_high if future_high is not None else 0.0
            one["future_low"] = future_low if future_low is not None else 0.0
            one["future_close_high"] = future_close_high if future_close_high is not None else 0.0
            one["future_close_low"] = future_close_low if future_close_low is not None else 0.0
            add_contrib(order, float(score), "breakout", ctype, cid)
        elif ctype == "arbitrage":
            seg = arb_segment_by_order.get(order, {})
            chain_count = _safe_int(seg.get("chain_count"), 0)
            seg_mult = _safe_int(seg.get("success_multiplier"), 1)
            base_success = _safe_int(card.get("arb_reward"), 0)
            base_fail = _safe_int(card.get("arb_fail_penalty"), 0)
            vol_fail = volatility_gt_3
            vol_severe_fail = volatility_gt_5
            pair_ok = chain_count >= 2
            success = pair_ok and (not vol_fail)
            raw_before_mult = base_success if success else -base_fail
            score = raw_before_mult
            if success:
                score = raw_before_mult * seg_mult
            elif vol_severe_fail:
                score = raw_before_mult * 2
            one["hit"] = bool(success)
            one["raw_score"] = int(score)
            one["arb_segment_id"] = _safe_int(seg.get("segment_id"), 0)
            one["arb_chain_count"] = chain_count
            one["arb_vol_fail"] = vol_fail
            one["arb_vol_severe_fail"] = vol_severe_fail
            one["arb_score_before_multiplier"] = int(raw_before_mult)
            one["arb_pair_ok"] = pair_ok
            add_contrib(order, float(score), "arbitrage", ctype, cid)
        elif ctype == "option":
            style = str(card.get("option_style", ""))
            side = str(card.get("option_side", ""))
            one["option_style"] = style
            one["option_side"] = side
            score = 0
            metric_yz = 0
            option_success = False
            option_crit = False
            option_severe_fail = False
            if style == "buy":
                entry_cost = max(0, _safe_int(card.get("entry_cost"), 0))
                win_mult = max(0, _safe_int(card.get("win_mult"), 0))
                reward_offset = max(0, _safe_int(card.get("reward_offset"), 0))
                score -= entry_cost
                first_open = _bar_open(future_bars[0]) if future_bars else 0.0
                if first_open > 0 and future_bars:
                    if side == "call" and future_high is not None and future_high > first_open:
                        option_success = True
                        metric_yz = _round_half_up_int((float(future_high) / first_open - 1.0) * 100.0)
                    elif side == "put" and future_low is not None and future_low < first_open:
                        option_success = True
                        metric_yz = _round_half_up_int((first_open / float(future_low) - 1.0) * 100.0)
                if option_success:
                    reward_units = max(0, metric_yz - reward_offset)
                    reward_before_crit = reward_units * win_mult
                    score += reward_before_crit
                    option_crit = _buy_option_crit(context_bars, future_bars, side)
                    if option_crit:
                        score *= 2
                    one["option_reward_units"] = reward_units
                    one["option_reward_before_crit"] = reward_before_crit
                else:
                    one["option_reward_units"] = 0
                    one["option_reward_before_crit"] = 0
                one["option_entry_cost"] = entry_cost
                one["option_win_mult"] = win_mult
                one["option_reward_offset"] = reward_offset
            else:
                baseline_close = _bar_close(context_bars[-1]) if context_bars else 0.0
                fail_pct = _safe_float(card.get("seller_fail_pct"), 0.0)
                severe_pct = _safe_float(card.get("seller_severe_fail_pct"), 0.0)
                severe_mult = max(1, _safe_int(card.get("seller_severe_fail_mult"), 1))
                if baseline_close > 0 and future_bars:
                    if side == "call":
                        max_high = float(future_high or 0.0)
                        option_severe_fail = max_high > baseline_close * (1.0 + severe_pct / 100.0)
                        option_success = not (max_high > baseline_close * (1.0 + fail_pct / 100.0))
                    else:
                        min_low = float(future_low or 0.0)
                        option_severe_fail = min_low < baseline_close * (1.0 - severe_pct / 100.0)
                        option_success = not (min_low < baseline_close * (1.0 - fail_pct / 100.0))
                else:
                    option_success = False
                if option_success:
                    score = _safe_int(card.get("hit_score"), 0)
                else:
                    score = -_safe_int(card.get("fail_penalty"), 0) * (severe_mult if option_severe_fail else 1)
                one["option_seller_fail_pct"] = fail_pct
                one["option_seller_severe_fail_pct"] = severe_pct
                one["option_seller_severe_fail_mult"] = severe_mult
            one["hit"] = bool(option_success)
            one["raw_score"] = int(score)
            one["option_metric_yz"] = int(metric_yz)
            one["option_crit"] = bool(option_crit)
            one["option_success"] = bool(option_success)
            one["option_severe_fail"] = bool(option_severe_fail)
            add_contrib(order, float(score), "option", ctype, cid)

        card_results.append(one)
        card_results_by_order[order] = one

    pair_bonus = 0
    pair_detail: List[Dict[str, object]] = []
    short_streak_conf = 0
    if short_misfire_applied:
        short_card_orders = [int(cr.get("order", 0)) for cr in card_results if str(cr.get("card_type", "")) == "short"]
        contribs = [x for x in contribs if str(x.get("kind", "")) != "short"]
        if short_card_orders:
            add_contrib(short_card_orders[0], -8.0, "short_misfire", "short", "__short_breakout_misfire__")
    else:
        pair_bonus, pair_detail = _short_pair_bonus(cards)
        if pair_bonus:
            for pd in pair_detail:
                start_order = _safe_int(pd.get("pair_start_order"), 0)
                add_contrib(start_order, float(_safe_int(pd.get("bonus"), 0)), "short_pair", "short", "__short_pair__", pair_detail=pd)
        short_streak_conf = _short_streak_confidence_bonus(cards)

    trend_raw_total = _sum_contribs([x for x in contribs if str(x.get("kind")) == "trend"])
    if trend_mult != 1:
        _apply_multiplier(contribs, float(trend_mult), kinds=["trend"])
    trend_total = _sum_contribs([x for x in contribs if str(x.get("kind")) == "trend"])
    short_base_total = _sum_contribs([x for x in contribs if str(x.get("kind")) in ("short", "short_pair", "short_misfire")])
    breakout_total = _sum_contribs([x for x in contribs if str(x.get("kind")) == "breakout"])
    arbitrage_total = _sum_contribs([x for x in contribs if str(x.get("kind")) == "arbitrage"])
    option_total = _sum_contribs([x for x in contribs if str(x.get("kind")) == "option"])

    tactic_chain: List[Dict[str, object]] = []
    confidence_delta = short_streak_conf
    extra_draw_gain = 0
    dynamic_adjust_next_turn = False
    self_confidence_checks: List[Dict[str, object]] = []
    fast_stop_regs: List[Dict[str, Any]] = []
    fast_stop_trace: List[Dict[str, Any]] = []

    for idx, cid in enumerate(cards):
        order = idx + 1
        card = CARD_LIBRARY.get(cid) or {}
        if str(card.get("type")) != "tactic":
            continue
        effect = str(card.get("effect", ""))
        running_total = _sum_contribs(contribs)
        node: Dict[str, object] = {"order": order, "card_id": cid, "effect": effect}

        if effect == "quick_cancel":
            extra_draw_gain += 1
            node["extra_draw_gain"] = 1
        elif effect == "dynamic_adjust":
            if not dynamic_adjust_next_turn:
                dynamic_adjust_next_turn = True
                node["applied"] = True
            else:
                node["applied"] = False
                node["reason"] = "already_applied_this_turn"
        elif effect == "scalp_cycle":
            if volatility_gt_3:
                node["voided"] = True
                node["reason"] = "future_spike_gt_3pct"
            else:
                delta = _apply_multiplier(contribs, 1.5, kinds=["short", "short_pair", "short_misfire"])
                node["voided"] = False
                node["score_delta"] = round(delta, 4)
        elif effect == "leverage":
            if running_total > 0:
                delta = _apply_multiplier(contribs, 2.0)
                node["score_multiplier"] = 2.0
                node["score_delta"] = round(delta, 4)
            else:
                confidence_delta -= 40
                node["confidence_delta"] = -40
        elif effect == "risk_control":
            if running_total > 0:
                delta = _apply_multiplier(contribs, 0.6)
                node["score_multiplier"] = 0.6
                node["score_delta"] = round(delta, 4)
            elif running_total < 0:
                delta = _apply_multiplier(contribs, 0.5)
                node["score_multiplier"] = 0.5
                node["score_delta"] = round(delta, 4)
            else:
                node["score_multiplier"] = 1.0
        elif effect == "meditation":
            gain = rng.randint(5, 15)
            confidence_delta += gain
            node["confidence_delta"] = gain
        elif effect == "self_confidence":
            min_conf = max(0, _safe_int(card.get("min_confidence"), 80))
            usable = run_confidence >= min_conf
            check = {"order": order, "card_id": cid, "usable": usable, "confidence": run_confidence, "min_confidence": min_conf}
            if not usable:
                return {
                    "ok": False,
                    "error_code": "confidence_not_enough",
                    "message": f"信心不足：自信下单需要信心>={min_conf}",
                }
            running_total_now = _sum_contribs(contribs)
            check["running_total_before"] = round(running_total_now, 4)
            if running_total_now > 0:
                delta = _apply_multiplier(contribs, 2.0)
                node["score_multiplier"] = 2.0
                node["score_delta"] = round(delta, 4)
                check["applied_multiplier"] = 2.0
            else:
                confidence_delta -= 20
                node["confidence_delta"] = -20
                check["confidence_delta"] = -20
            self_confidence_checks.append(check)
        elif effect == "fast_stop":
            regs: List[int] = []
            for offset in (1, 2):
                target_order = order + offset
                regs.append(target_order)
                fast_stop_regs.append({"source_order": order, "target_order": target_order})
            node["targets"] = regs
        tactic_chain.append(node)

    # 结算快速止损：按最终分拦截后两张牌负分（不可保护卡仍占位）。
    for reg in fast_stop_regs:
        target_order = _safe_int(reg.get("target_order"), 0)
        source_order = _safe_int(reg.get("source_order"), 0)
        trace: Dict[str, Any] = {"source_order": source_order, "target_order": target_order}
        cr = card_results_by_order.get(target_order)
        if not cr:
            trace["status"] = "out_of_range"
            fast_stop_trace.append(trace)
            continue
        blocked_reason = ""
        target_type = str(cr.get("card_type", ""))
        if target_type == "breakout":
            blocked_reason = "breakout"
        elif target_type == "option":
            card = CARD_LIBRARY.get(str(cr.get("card_id", ""))) or {}
            if str(card.get("option_style", "")) == "buy":
                blocked_reason = "buy_option"
        if blocked_reason:
            cr["fast_stop_protected"] = False
            cr["fast_stop_blocked_reason"] = blocked_reason
            trace["status"] = "blocked"
            trace["blocked_reason"] = blocked_reason
            fast_stop_trace.append(trace)
            continue
        final_amount = _sum_contribs([x for x in contribs if _safe_int(x.get("order"), 0) == target_order])
        if final_amount < 0:
            add_contrib(target_order, -final_amount, "fast_stop_protect", target_type, "__fast_stop__", source_order=source_order)
            cr["fast_stop_protected"] = True
            cr["fast_stop_blocked_reason"] = ""
            trace["status"] = "protected"
            trace["compensation"] = round(-final_amount, 4)
        else:
            cr["fast_stop_protected"] = False
            cr["fast_stop_blocked_reason"] = ""
            trace["status"] = "no_negative_score"
        fast_stop_trace.append(trace)

    # 汇总每张牌最终分数（仅用于日志/调试展示）。
    for cr in card_results:
        order = _safe_int(cr.get("order"), 0)
        final_amount = _sum_contribs([x for x in contribs if _safe_int(x.get("order"), 0) == order])
        cr["final_score"] = int(round(final_amount))
        cr.setdefault("fast_stop_protected", False)
        cr.setdefault("fast_stop_blocked_reason", "")

    momentum_after = max(0, momentum_before + trend_gain - trend_loss)
    running_total_final = _sum_contribs(contribs)
    turn_score = int(round(running_total_final))

    return {
        "turn_score": turn_score,
        "detail": {
            "reason": "combo",
            "features": extract_features(context_bars, future_bars),
        },
        "card_results": card_results,
        "mechanics": {
            "rule_version": RULE_VERSION,
            "momentum_before": momentum_before,
            "momentum_after": momentum_after,
            "trend_multiplier": trend_mult,
            "trend_gain": trend_gain,
            "trend_loss": trend_loss,
            "short_pair_bonus": int(pair_bonus),
            "short_pair_detail": pair_detail,
            "short_streak_conf_bonus": int(short_streak_conf),
            "short_breakout_misfire_applied": bool(short_misfire_applied),
            "short_breakout_direction": short_breakout_direction,
            "confidence_delta_from_cards": int(confidence_delta),
            "extra_draw_next_turn_gain": int(extra_draw_gain),
            "dynamic_adjust_next_turn": bool(dynamic_adjust_next_turn),
            "tactic_chain": tactic_chain,
            "self_confidence_checks": self_confidence_checks,
            "fast_stop_trace": fast_stop_trace,
            "volatility_gt_3pct": bool(volatility_gt_3),
            "arbitrage_volatility_gt_3pct": bool(volatility_gt_3),
            "arbitrage_volatility_gt_5pct": bool(volatility_gt_5),
            "arbitrage_segments": [
                {
                    "segment_id": _safe_int(seg.get("segment_id"), 0),
                    "start_order": _safe_int(seg.get("start_order"), 0),
                    "end_order": _safe_int(seg.get("end_order"), 0),
                    "orders": list(seg.get("orders") or []),
                    "regions_dedup": list(seg.get("regions_dedup") or []),
                    "chain_count": _safe_int(seg.get("chain_count"), 0),
                    "success_multiplier": _safe_int(seg.get("success_multiplier"), 1),
                }
                for seg in arb_segments
            ],
            "subtotal_short": int(round(short_base_total)),
            "subtotal_trend_raw": int(round(trend_raw_total)),
            "subtotal_trend_final": int(round(trend_total)),
            "subtotal_breakout": int(round(breakout_total)),
            "subtotal_arbitrage": int(round(arbitrage_total)),
            "subtotal_option": int(round(option_total)),
        },
    }


def resolve_turn(
    card_id: Optional[str],
    context_bars: List[Dict[str, object]],
    future_bars: List[Dict[str, object]],
    stage_no: int,
    run_effects: Optional[Dict[str, object]] = None,
    event_state: Optional[Dict[str, object]] = None,
    action_type: str = "play",
    seed: Optional[int] = None,
) -> Dict[str, object]:
    if action_type == "pass" or not str(card_id or "").strip():
        return resolve_turn_combo(
            card_ids=[],
            context_bars=context_bars,
            future_bars=future_bars,
            stage_no=stage_no,
            run_effects=run_effects,
            event_state=event_state,
            seed=seed,
        )
    return resolve_turn_combo(
        card_ids=[str(card_id)],
        context_bars=context_bars,
        future_bars=future_bars,
        stage_no=stage_no,
        run_effects=run_effects,
        event_state=event_state,
        seed=seed,
    )


def roll_stage_event(seed: Optional[int], stage_no: int) -> Dict[str, object]:
    _ = seed
    _ = stage_no
    return {}


def apply_event_if_trigger(event_state: Dict[str, object], turn_no: int) -> Dict[str, object]:
    _ = turn_no
    return {
        "event_state": dict(event_state or {}),
        "triggered": False,
        "score_delta": 0,
        "confidence_delta": 0,
        "message": "",
    }


def get_stage_upgrade_choices(seed: Optional[int], stage_no: int) -> List[Dict[str, object]]:
    _ = seed
    _ = stage_no
    return []


def apply_stage_upgrade_effect(
    run_effects: Dict[str, object], deck: List[str], upgrade_code: str
) -> Tuple[Dict[str, object], List[str]]:
    _ = upgrade_code
    return dict(run_effects or {}), list(deck or [])


def compute_meta_bonuses(meta_upgrades: Optional[Dict[str, int]]) -> Dict[str, float]:
    up = meta_upgrades or {}
    confidence_lv = max(0, min(3, _safe_int(up.get("confidence_core"), 0)))
    hand_lv = max(0, min(2, _safe_int(up.get("hand_memory"), 0)))
    draw_lv = max(0, min(3, _safe_int(up.get("draw_insight"), 0)))
    return {
        "starting_confidence_bonus": confidence_lv * 5,
        "hand_limit_bonus": hand_lv,
        "draw_quality": draw_lv,
        "score_multiplier": 1.0 + draw_lv * 0.03,
    }


def compute_run_exp(total_score: int, cleared_stages: int, cleared_run: bool) -> int:
    score_part = max(0, int(total_score // 20))
    stage_part = max(0, int(cleared_stages)) * 80
    base = 120 + score_part + stage_part
    if cleared_run:
        base += 300
    return int(base)
