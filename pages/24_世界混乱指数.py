from __future__ import annotations

import math
import os
import sys
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import data_engine as de
from risk_index_config import EVENT_BASKET_V1, ONGOING_CHAOS_CLUSTERS_V1
from sidebar_navigation import show_navigation
from ui_components import inject_sidebar_toggle_style

st.set_page_config(
    page_title="爱波塔·世界混乱指数",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded",
)

with st.sidebar:
    show_navigation()

inject_sidebar_toggle_style(mode="high_contrast")


BAND_META = {
    "nothing_happens": {"label": "局势偏稳", "color": "#22c55e", "range": "0-24"},
    "something_might_happen": {"label": "混乱升温", "color": "#facc15", "range": "25-49"},
    "something_is_brewing": {"label": "全球失序", "color": "#fb923c", "range": "50-74"},
    "things_are_happening": {"label": "世界大战", "color": "#ef4444", "range": "75-100"},
}
CATEGORY_LABELS = {
    "military_conflict": "军事冲突",
    "nuclear_escalation": "核升级",
    "political_instability": "政治失稳",
    "economic_crisis": "经济危机",
    "public_health": "公共卫生",
}
REGION_LABELS = {
    "middle_east": "中东",
    "east_asia": "东亚",
    "korean_peninsula": "朝鲜半岛",
    "europe": "欧洲",
    "global": "全球",
    "north_america": "北美",
    "balkans": "巴尔干",
}


snapshot = de.get_latest_geopolitical_risk_snapshot()
if not snapshot:
    st.warning("暂无可用快照，请先运行更新脚本生成数据。")
    st.stop()

top_markets = list(snapshot.get("top_markets") or [])
category_breakdown = list(snapshot.get("category_breakdown") or [])
pair_breakdown = list(snapshot.get("pair_breakdown") or [])
headline_explanations = list(snapshot.get("headline_explanations") or [])
source_status = dict(snapshot.get("source_status") or {})
score_components = dict(source_status.get("score_components") or {})
ongoing_clusters = list(source_status.get("ongoing_clusters") or [])
monitored_markets = list(source_status.get("monitored_markets") or top_markets)
recent_snapshots = list(de.get_recent_geopolitical_risk_snapshots(limit=8) or [])

score_raw = float(snapshot.get("score_raw") or 0.0)
score_display = float(snapshot.get("score_display") or 0.0)
updated_at = str(snapshot.get("updated_at") or "")
methodology_version = str(snapshot.get("methodology_version") or "wci_v1")
tracked_count = len(EVENT_BASKET_V1) + len(ONGOING_CHAOS_CLUSTERS_V1)
identified_count = len(monitored_markets)


def _safe_num(value, default=0.0) -> float:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _fmt_pct_delta(value: float) -> str:
    return f"{value * 100:+.1f}%"


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _risk_accent(probability: float) -> tuple[str, str]:
    p = max(0.0, min(1.0, probability))
    if p >= 0.75:
        return "#ef4444", "rgba(239,68,68,.14)"
    if p >= 0.5:
        return "#fb923c", "rgba(251,146,60,.14)"
    if p >= 0.25:
        return "#facc15", "rgba(250,204,21,.12)"
    return "#22c55e", "rgba(34,197,94,.12)"


def _market_history_key(item: dict) -> str:
    return str(
        item.get("event_slug")
        or item.get("market_slug")
        or item.get("event_key")
        or item.get("pair_tag")
        or item.get("display_title")
        or ""
    ).strip().lower()


def _build_market_trend_map(snapshots: list[dict], threshold: float = 0.005) -> dict[str, dict]:
    series_by_key: dict[str, list[float]] = {}
    for snap in snapshots or []:
        items = list((((snap.get("source_status") or {}).get("monitored_markets")) or snap.get("top_markets") or []))
        seen_in_snapshot: set[str] = set()
        for item in items:
            key = _market_history_key(item)
            if not key or key in seen_in_snapshot:
                continue
            seen_in_snapshot.add(key)
            series_by_key.setdefault(key, []).append(_safe_num(item.get("probability")))

    trend_map: dict[str, dict] = {}
    for key, values in series_by_key.items():
        recent_values = values[-4:]
        if len(recent_values) < 2:
            continue
        deltas = [recent_values[idx] - recent_values[idx - 1] for idx in range(1, len(recent_values))]
        latest_delta = deltas[-1]
        directions = []
        for delta in deltas:
            if delta >= threshold:
                directions.append(1)
            elif delta <= -threshold:
                directions.append(-1)
            else:
                directions.append(0)
        strength = 0
        direction = 0
        for expected in (1, -1):
            run = 0
            for value in reversed(directions):
                if value == expected:
                    run += 1
                else:
                    break
            if run > 0:
                strength = run
                direction = expected
                break
        arrow_text = ""
        arrow_direction = ""
        if strength > 0:
            strength = min(3, strength)
            if direction > 0:
                arrow_text = "▲" * strength
                arrow_direction = "up"
            else:
                arrow_text = "▼" * strength
                arrow_direction = "down"

        flame_text = ""
        if latest_delta >= 0.05:
            flame_text = "🔥"

        if not arrow_text and not flame_text:
            continue
        trend_map[key] = {
            "arrows": arrow_text,
            "direction": arrow_direction,
            "flames": flame_text,
            "latest_delta": latest_delta,
        }
    return trend_map


def _best_polymarket_url(item: dict) -> str:
    event_slug = str(item.get("event_slug") or "").strip()
    if event_slug:
        return f"https://polymarket.com/event/{event_slug}"
    source_url = str(item.get("source_url") or "").strip()
    if source_url:
        return source_url
    market_slug = str(item.get("market_slug") or "").strip()
    if market_slug:
        return f"https://polymarket.com/event/{market_slug}"
    return ""


def _band_for_score(value: float) -> dict:
    if value < 25:
        return BAND_META["nothing_happens"]
    if value < 50:
        return BAND_META["something_might_happen"]
    if value < 75:
        return BAND_META["something_is_brewing"]
    return BAND_META["things_are_happening"]


def _region_label(region_tag: str) -> str:
    return REGION_LABELS.get(str(region_tag or ""), str(region_tag or "全球").replace("_", " "))


def _format_updated_at(value: str) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%H:%M:%S")
    except Exception:
        return value


def _polar_to_cartesian(cx: float, cy: float, radius: float, angle_deg: float) -> tuple[float, float]:
    angle_rad = math.radians(angle_deg)
    return cx + radius * math.cos(angle_rad), cy - radius * math.sin(angle_rad)


def _arc_path(cx: float, cy: float, radius: float, start_deg: float, end_deg: float) -> str:
    start_x, start_y = _polar_to_cartesian(cx, cy, radius, start_deg)
    end_x, end_y = _polar_to_cartesian(cx, cy, radius, end_deg)
    large_arc = 1 if abs(end_deg - start_deg) > 180 else 0
    sweep = 0 if end_deg > start_deg else 1
    return f"M {start_x:.2f} {start_y:.2f} A {radius:.2f} {radius:.2f} 0 {large_arc} {sweep} {end_x:.2f} {end_y:.2f}"


def _build_gauge_markup(value: float) -> str:
    score = max(0.0, min(100.0, value))
    angle = 180 - score * 1.8
    segments = [
        (180, 135, "#16a34a", "局势偏稳"),
        (135, 90, "#facc15", "混乱升温"),
        (90, 45, "#fb923c", "全球失序"),
        (45, 0, "#ef4444", "世界大战"),
    ]
    segment_paths = []
    glow_paths = []
    label_nodes = []
    for start, end, color, label in segments:
        segment_paths.append(
            f"<path d='{_arc_path(320, 320, 235, start, end)}' fill='none' stroke='{color}' stroke-width='58' stroke-linecap='butt' opacity='0.26'/>"
        )
        glow_paths.append(
            f"<path d='{_arc_path(320, 320, 235, start, end)}' fill='none' stroke='{color}' stroke-width='4' stroke-linecap='round' opacity='0.55' filter='url(#gaugeGlow)'/>"
        )
    tick_angles = [180, 144, 108, 72, 36, 0]
    tick_values = ["0", "20", "40", "60", "80", "100"]
    tick_nodes = []
    for ang, tick in zip(tick_angles, tick_values):
        tx, ty = _polar_to_cartesian(320, 320, 286, ang)
        ix1, iy1 = _polar_to_cartesian(320, 320, 252, ang)
        ix2, iy2 = _polar_to_cartesian(320, 320, 268, ang)
        tick_nodes.append(f"<line x1='{ix1:.2f}' y1='{iy1:.2f}' x2='{ix2:.2f}' y2='{iy2:.2f}' stroke='rgba(184,201,232,.55)' stroke-width='2'/>")
        tick_nodes.append(f"<text x='{tx:.2f}' y='{ty:.2f}' fill='rgba(214,228,255,.82)' font-size='13' font-family='IBM Plex Mono, monospace' text-anchor='middle'>{tick}</text>")
    label_specs = [
        (157.5, 195, "#6ee7b7", "局势偏稳"),
        (112.5, 202, "#fde047", "混乱升温"),
        (67.5, 202, "#fdba74", "全球失序"),
        (22.5, 195, "#fda4af", "世界大战"),
    ]
    for ang, radius, color, label in label_specs:
        lx, ly = _polar_to_cartesian(320, 320, radius, ang)
        label_nodes.append(
            f"<text x='{lx:.2f}' y='{ly:.2f}' fill='{color}' font-size='14' font-weight='700' font-family='Rajdhani, Noto Sans SC, sans-serif' text-anchor='middle'>{label}</text>"
        )
    px, py = _polar_to_cartesian(320, 320, 190, angle)
    hx, hy = _polar_to_cartesian(320, 320, 32, angle)
    return f"""
    <div class="gauge-shell">
      <div class="gauge-radar"></div>
      <div class="gauge-sweep"></div>
      <div class="gauge-core-ring gauge-core-ring-1"></div>
      <div class="gauge-core-ring gauge-core-ring-2"></div>
      <div class="gauge-core-ring gauge-core-ring-3"></div>
      <div class="gauge-core-grid"></div>
      <svg viewBox="0 0 640 390" class="gauge-svg" aria-hidden="true">
        <defs>
          <filter id="gaugeGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="4" result="blur"/>
            <feMerge>
              <feMergeNode in="blur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>
        <path d="{_arc_path(320, 320, 244, 180, 0)}" fill="none" stroke="rgba(120,154,214,.16)" stroke-width="2" />
        <path d="{_arc_path(320, 320, 235, 180, 0)}" fill="none" stroke="rgba(255,255,255,.05)" stroke-width="62" />
        {''.join(segment_paths)}
        {''.join(glow_paths)}
        <path d="{_arc_path(320, 320, 158, 180, 0)}" fill="none" stroke="rgba(255,255,255,.06)" stroke-width="2" />
        <path d="{_arc_path(320, 320, 122, 180, 0)}" fill="none" stroke="rgba(56,189,248,.09)" stroke-width="2" />
        {''.join(tick_nodes)}
        {''.join(label_nodes)}
        <line x1="{hx:.2f}" y1="{hy:.2f}" x2="{px:.2f}" y2="{py:.2f}" stroke="#ffb347" stroke-width="8" stroke-linecap="round" />
        <circle cx="320" cy="320" r="30" fill="rgba(255,179,71,.08)" stroke="rgba(255,179,71,.18)" stroke-width="1"/>
        <circle cx="320" cy="320" r="16" fill="#f59e0b" stroke="rgba(255,214,102,.32)" stroke-width="8"/>
      </svg>
      <div class="gauge-center">
        <div class="gauge-kicker">WORLD CHAOS INDEX</div>
        <div class="gauge-value">{score:.1f}</div>
        <div class="gauge-band">{_band_for_score(score)['label']}</div>
      </div>
    </div>
    """


market_trend_map = _build_market_trend_map(recent_snapshots, threshold=0.005)


st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
    :root {
        --risk-bg-0:#050914;
        --risk-bg-1:#081121;
        --risk-card:rgba(10,18,37,.88);
        --risk-card2:rgba(12,22,44,.96);
        --risk-line:rgba(100,149,237,.20);
        --risk-text:#eef4ff;
        --risk-muted:#93a9cc;
        --risk-green:#22c55e;
        --risk-yellow:#facc15;
        --risk-orange:#fb923c;
        --risk-red:#ef4444;
        --risk-cyan:#38bdf8;
    }
    .stApp {
        background:
            radial-gradient(900px 520px at 88% -10%, rgba(239,68,68,.14), transparent 58%),
            radial-gradient(980px 600px at 0% 0%, rgba(59,130,246,.14), transparent 58%),
            linear-gradient(160deg, var(--risk-bg-0), var(--risk-bg-1));
        color: var(--risk-text);
        font-family: "Rajdhani", "Noto Sans SC", sans-serif;
    }
    [data-testid="stMainBlockContainer"] { max-width: 110rem !important; padding-top: .8rem; padding-bottom: 1.5rem; }
    [data-testid="stHeader"] { background: transparent !important; }
    [data-testid="stDecoration"] { display: none; }
    h1, h2, h3, p, label, .stCaption { color: var(--risk-text) !important; }
    .hero-card, .panel-card {
        border: 1px solid var(--risk-line);
        border-radius: 24px;
        background: linear-gradient(140deg, var(--risk-card), var(--risk-card2));
        box-shadow: 0 16px 44px rgba(0,0,0,.28), inset 0 1px 0 rgba(255,255,255,.03);
    }
    .hero-card { padding: 12px 18px 14px; min-height: 116px; position: relative; overflow: hidden; }
    .hero-card::after {
        content:"";
        position:absolute;
        inset:auto -8% -40% auto;
        width:340px;
        height:340px;
        background: radial-gradient(circle, rgba(250,204,21,.10), transparent 68%);
        pointer-events:none;
    }
    .hero-title { font-size: clamp(34px,4.6vw,62px); line-height: 1.02; font-weight: 800; }
    .hero-sub { margin-top: 6px; max-width: 780px; color: var(--risk-muted); font-size: 14px; line-height: 1.4; }
    .meta-line { display:flex; justify-content:flex-end; align-items:center; gap:12px; margin-bottom: 2px; color:#b9cae7; font-size:14px; }
    .panel-head { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; margin-bottom: 8px; }
    .panel-head .section-title { text-align:left; }
    .panel-head .section-sub { text-align:left; margin-bottom: 0; }
    .status-chip {
        display:inline-flex; align-items:center; gap:10px; padding:7px 12px; border-radius:999px;
        border:1px solid rgba(125,211,252,.16); background:linear-gradient(180deg, rgba(7,13,28,.92), rgba(9,16,32,.72));
        color:#dbeafe; font-size:12px; font-family:"IBM Plex Mono", monospace;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.04);
    }
    .status-dot {
        width:8px; height:8px; border-radius:999px; background:#22c55e; display:inline-block;
        box-shadow:0 0 12px rgba(34,197,94,.55);
    }
    .status-time { color:#9fb5da; }
    .panel-card { padding: 14px 14px 12px; }
    .section-title { font-size: 24px; font-weight: 800; margin: 0; line-height: 1.05; }
    .section-sub { color: var(--risk-muted); font-size: 12px; margin-top: 4px; margin-bottom: 12px; }
    .component-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; margin-top: 8px; }
    .component-card {
        padding:10px 12px; border-radius:15px; border:1px solid rgba(148,163,184,.12); background:linear-gradient(180deg, rgba(2,6,23,.56), rgba(4,10,22,.34));
        box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
    }
    .component-label { color: var(--risk-muted); font-size: 12px; }
    .component-value { color:#f8fbff; font-size: 21px; font-family:"IBM Plex Mono", monospace; margin-top: 3px; }
    .driving-card {
        margin-top: 8px; border-radius: 18px; border:1px solid rgba(148,163,184,.14); background:rgba(8,16,33,.78); padding: 12px 14px;
    }
    .gauge-shell {
        position: relative;
        min-height: 540px;
        border-radius: 28px;
        border: 1px solid rgba(120,154,214,.16);
        background:
            radial-gradient(circle at 50% 68%, rgba(255,193,7,.12), transparent 28%),
            linear-gradient(180deg, rgba(9,16,32,.96), rgba(7,13,28,.82));
        overflow: hidden;
        box-shadow: inset 0 0 0 1px rgba(255,255,255,.03), 0 22px 60px rgba(0,0,0,.22);
    }
    .gauge-shell::before {
        content:"";
        position:absolute;
        inset:0;
        background:
            linear-gradient(rgba(56,189,248,.06) 1px, transparent 1px),
            linear-gradient(90deg, rgba(56,189,248,.05) 1px, transparent 1px);
        background-size: 28px 28px, 28px 28px;
        mask-image: linear-gradient(180deg, rgba(255,255,255,.55), transparent 92%);
        pointer-events:none;
    }
    .gauge-radar {
        position:absolute;
        left:-8%;
        bottom:-10%;
        width:72%;
        height:72%;
        border-radius:50%;
        background: radial-gradient(circle, rgba(34,197,94,.06), transparent 58%);
        filter: blur(6px);
        pointer-events:none;
        z-index:1;
    }
    .gauge-sweep {
        position:absolute;
        left:50%;
        top:56%;
        width:420px;
        height:420px;
        transform:translate(-50%, -50%);
        border-radius:50%;
        background: conic-gradient(from 204deg, rgba(56,189,248,.18), rgba(56,189,248,.00) 18deg, transparent 90deg, transparent 360deg);
        filter: blur(1px);
        opacity:.8;
        pointer-events:none;
        mask-image: radial-gradient(circle, transparent 0 32%, rgba(255,255,255,.9) 34%, rgba(255,255,255,.0) 66%);
        z-index:1;
    }
    .gauge-core-grid {
        position:absolute;
        left:50%;
        bottom:24px;
        width:420px;
        height:210px;
        transform:translateX(-50%);
        border-radius: 420px 420px 0 0;
        background:
            linear-gradient(rgba(56,189,248,.06) 1px, transparent 1px),
            linear-gradient(90deg, rgba(56,189,248,.05) 1px, transparent 1px);
        background-size: 24px 24px, 24px 24px;
        opacity:.18;
        pointer-events:none;
        mask-image: radial-gradient(circle at 50% 100%, rgba(255,255,255,.95) 0 63%, transparent 76%);
        z-index:1;
    }
    .gauge-core-ring {
        position:absolute;
        left:50%;
        bottom:28px;
        transform:translateX(-50%);
        border-radius:50%;
        border:1px solid rgba(125,211,252,.10);
        pointer-events:none;
        z-index:1;
        box-shadow: 0 0 18px rgba(56,189,248,.04);
    }
    .gauge-core-ring-1 { width:170px; height:170px; }
    .gauge-core-ring-2 { width:260px; height:260px; border-color: rgba(125,211,252,.08); }
    .gauge-core-ring-3 { width:350px; height:350px; border-color: rgba(125,211,252,.05); }
    .gauge-svg { width:100%; height:auto; display:block; position:relative; z-index:2; }
    .gauge-center {
        position:absolute;
        left:50%;
        bottom:42px;
        transform:translateX(-50%);
        width: 300px;
        text-align:center;
        z-index:3;
    }
    .gauge-kicker {
        color:#7dd3fc;
        letter-spacing:.18em;
        font-size:11px;
        font-family:"IBM Plex Mono", monospace;
        margin-bottom:8px;
        text-shadow:0 0 12px rgba(125,211,252,.18);
    }
    .gauge-value {
        font-size:78px;
        line-height:1;
        color:#ffd12a;
        text-shadow:0 0 24px rgba(255,209,42,.18), 0 0 42px rgba(255,179,71,.08);
        font-family:"IBM Plex Mono", monospace;
    }
    .gauge-band {
        margin-top:8px;
        display:inline-flex;
        padding:6px 12px;
        border-radius:999px;
        border:1px solid rgba(255,255,255,.10);
        background:linear-gradient(180deg, rgba(7,13,28,.92), rgba(9,16,32,.72));
        color:#d7e6ff;
        font-size:13px;
        box-shadow: inset 0 1px 0 rgba(255,255,255,.04);
    }
    .driving-top { color: var(--risk-muted); font-size: 10px; letter-spacing: .18em; margin-bottom: 6px; }
    .driving-head { display:flex; justify-content:space-between; gap:14px; align-items:flex-start; }
    .driving-name { font-size: 18px; font-weight: 700; line-height: 1.28; }
    .driving-prob { font-size: 38px; font-family:"IBM Plex Mono", monospace; color: var(--risk-yellow); white-space: nowrap; }
    .driving-meta { margin-top: 6px; color: var(--risk-muted); font-size: 12px; }
    .monitored-wrap { max-height: 760px; overflow-y:auto; padding-right: 2px; }
    .market-row {
        display:flex; justify-content:space-between; gap:12px; align-items:center; padding:10px 10px 10px 12px; border-radius:14px;
        border:1px solid rgba(148,163,184,.09); background:
            linear-gradient(90deg, rgba(9,18,38,.88), rgba(8,16,33,.70));
        margin-bottom:8px;
        position: relative;
        overflow: hidden;
    }
    .market-link {
        display:block;
        text-decoration:none;
        color:inherit !important;
        border-radius:14px;
        transition: transform .14s ease, filter .14s ease;
    }
    .market-link, .market-link:hover, .market-link:visited, .market-link:active {
        text-decoration:none !important;
        color:inherit !important;
    }
    .market-link * {
        text-decoration:none !important;
        color:inherit;
    }
    .market-link:hover {
        text-decoration:none;
        transform: translateY(-1px);
        filter: brightness(1.03);
    }
    .market-link:hover .market-row {
        border-color: rgba(125,211,252,.26);
        box-shadow: 0 8px 24px rgba(2,6,23,.24), inset 0 1px 0 rgba(255,255,255,.04);
    }
    .market-link .market-row {
        cursor:pointer;
    }
    .market-row::before {
        content:"";
        position:absolute;
        left:0;
        top:10px;
        bottom:10px;
        width:3px;
        border-radius:999px;
        background: linear-gradient(180deg, var(--market-accent, rgba(56,189,248,.9)), rgba(255,255,255,.16));
        opacity:.9;
    }
    .market-left { display:flex; align-items:flex-start; gap:10px; min-width:0; }
    .market-rank {
        width:22px; height:22px; border-radius:999px; flex:0 0 22px;
        display:flex; align-items:center; justify-content:center;
        font-family:"IBM Plex Mono", monospace; font-size:10px; color:#a9bddf;
        border:1px solid rgba(125,211,252,.10); background:rgba(15,23,42,.42);
        margin-top:2px;
    }
    .market-dot {
        width:8px; height:8px; border-radius:999px; background:var(--market-accent, #22c55e); display:inline-block;
        box-shadow:0 0 10px var(--market-accent-soft, rgba(34,197,94,.22));
    }
    .market-head { display:flex; align-items:center; gap:8px; }
    .market-title-wrap { display:flex; align-items:center; gap:8px; min-width:0; }
    .market-name { font-size: 16px; font-weight: 700; line-height: 1.2; }
    .market-trend {
        display:inline-flex;
        align-items:center;
        gap:2px;
        font-family:"IBM Plex Mono", monospace;
        font-size:11px;
        letter-spacing:.04em;
        padding:2px 6px;
        border-radius:999px;
        border:1px solid rgba(148,163,184,.14);
        background:rgba(8,16,33,.52);
        flex:0 0 auto;
    }
    .market-trend-up { color:#f87171; box-shadow:0 0 12px rgba(248,113,113,.08); }
    .market-trend-down { color:#34d399; box-shadow:0 0 12px rgba(52,211,153,.08); }
    .market-heat {
        display:inline-flex;
        align-items:center;
        font-size:12px;
        line-height:1;
        letter-spacing:.06em;
        padding:2px 6px;
        border-radius:999px;
        border:1px solid rgba(251,146,60,.18);
        background:rgba(251,146,60,.10);
        color:#fb923c;
        box-shadow:0 0 16px rgba(251,146,60,.08);
        flex:0 0 auto;
    }
    .market-meta { color: rgba(147,169,204,.78); font-size: 10px; margin-top: 4px; letter-spacing: .16em; }
    .market-prob {
        font-size: 20px; font-family:"IBM Plex Mono", monospace; font-weight: 700; color: var(--market-accent, var(--risk-yellow)); white-space: nowrap;
        padding:6px 10px; border-radius:12px; background:var(--market-accent-soft, rgba(250,204,21,.06)); border:1px solid var(--market-accent-soft, rgba(250,204,21,.14));
    }
    .table-card { border:1px solid var(--risk-line); border-radius:18px; background:linear-gradient(140deg,var(--risk-card),var(--risk-card2)); padding:12px 14px; margin-top: 12px; }
    .table-card h3 { margin:0; font-size:24px; }
    .table-sub { color: var(--risk-muted); font-size: 12px; margin-top: 4px; margin-bottom: 8px; }
    .risk-table { width:100%; border-collapse: collapse; }
    .risk-table th, .risk-table td { padding:9px 8px; border-bottom:1px solid rgba(148,163,184,.10); text-align:left; }
    .risk-table th { color:#c9d9f3; font-size:13px; }
    .risk-table td { color:#eff6ff; font-size:14px; }
    .pair-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(260px, 1fr)); gap:12px; }
    .pair-card { border:1px solid rgba(96,165,250,.18); border-radius:16px; padding:12px; background:rgba(6,14,31,.72); }
    .pair-title { display:flex; justify-content:space-between; gap:8px; align-items:center; font-size:18px; font-weight:700; margin-bottom:8px; }
    .pair-chip { font-family:"IBM Plex Mono", monospace; font-size:12px; color:#93c5fd; }
    .pair-item { padding:8px 0; border-top:1px dashed rgba(148,163,184,.15); }
    .pair-item:first-child { border-top:none; padding-top:0; }
    @media (max-width: 900px) {
        .component-grid { grid-template-columns:1fr; }
        .gauge-shell { min-height: 430px; }
        .gauge-center { width: 220px; bottom: 28px; }
        .gauge-value { font-size: 56px; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    f"""
    <div class="hero-card">
      <div class="meta-line">
        <div class="status-chip"><span class="status-dot"></span><span>LIVE</span><span class="status-time">{_format_updated_at(updated_at)}</span></div>
      </div>
      <div class="hero-title">世界混乱指数</div>
      <div class="hero-sub">追踪长期冲突、升级风险与跨区域联动，快速判断全球失序温度。</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

left, right = st.columns([1.08, 0.92], gap="large")
with left:
    st.markdown(_build_gauge_markup(score_raw), unsafe_allow_html=True)
    primary_driver = top_markets[0] if top_markets else None
    if primary_driver:
        st.markdown(
            f"""
            <div class="driving-card">
              <div class="driving-top">推动指数的事件</div>
              <div class="driving-head">
                <div>
                  <div class="driving-name">{primary_driver.get('display_title', '-')}</div>
                  <div class="driving-meta">{_region_label(primary_driver.get('region_tag'))} · {primary_driver.get('pair_tag', '-')} · 24h {_fmt_pct_delta(_safe_num(primary_driver.get('delta_24h')))}</div>
                </div>
                <div class="driving-prob">{_fmt_pct(_safe_num(primary_driver.get('probability')))}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown(
        f"""
        <div class="component-grid">
          <div class="component-card"><div class="component-label">持续基础分</div><div class="component-value">{_safe_num(score_components.get('ongoing_baseline')):.1f}</div></div>
          <div class="component-card"><div class="component-label">升级风险分</div><div class="component-value">{_safe_num(score_components.get('escalation_pressure')):.1f}</div></div>
          <div class="component-card"><div class="component-label">联动加成</div><div class="component-value">{_safe_num(score_components.get('contagion_bonus')):.1f}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with right:
    market_rows = []
    for idx, item in enumerate(monitored_markets[:20]):
        accent, accent_soft = _risk_accent(_safe_num(item.get("probability")))
        trend_info = market_trend_map.get(_market_history_key(item), {})
        trend_arrows = str(trend_info.get("arrows") or "")
        trend_direction = str(trend_info.get("direction") or "")
        trend_flames = str(trend_info.get("flames") or "")
        trend_parts = []
        if trend_arrows:
            trend_class = "market-trend-up" if trend_direction == "up" else "market-trend-down"
            trend_parts.append(f"<span class='market-trend {trend_class}'>{trend_arrows}</span>")
        if trend_flames:
            trend_parts.append(f"<span class='market-heat'>{trend_flames}</span>")
        trend_html = "".join(trend_parts)
        row_html = (
            f"<div class='market-row' style=\"--market-accent:{accent};--market-accent-soft:{accent_soft};\">"
            f"<div class='market-left'><div class='market-rank'>{idx + 1:02d}</div><div>"
            f"<div class='market-head'><span class='market-dot'></span><div class='market-title-wrap'><div class='market-name'>{item.get('display_title','-')}</div>{trend_html}</div></div>"
            f"<div class='market-meta'>{_region_label(item.get('region_tag'))} · {item.get('pair_tag','-')} · POLYMARKET</div>"
            f"</div></div><div class='market-prob'>{_fmt_pct(_safe_num(item.get('probability')))}</div></div>"
        )
        source_url = _best_polymarket_url(item)
        if source_url:
            market_rows.append(
                f"<a class='market-link' href='{source_url}' target='_blank' rel='noopener noreferrer'>{row_html}</a>"
            )
        else:
            market_rows.append(row_html)
    st.markdown(
        f"""
        <div class="panel-card">
            <div class="panel-head">
            <div>
              <div class="section-title">监控市场</div>
              <div class="section-sub">数据来源：Polymarket</div>
            </div>
            <div style="font-family:'IBM Plex Mono',monospace;color:#93a9cc;white-space:nowrap;">{identified_count} 个监控市场</div>
          </div>
          <div class="monitored-wrap">
            {''.join(market_rows) or "<div class='market-row'><div class='market-name'>暂无已识别市场</div><div class='market-prob'>--</div></div>"}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

left_bottom, right_bottom = st.columns([1.08, 0.92], gap="large")
with left_bottom:
    st.markdown('<div class="table-card"><h3>主要推升项</h3><div class="table-sub">按指数贡献排序的核心市场</div>', unsafe_allow_html=True)
    if top_markets:
        rows = []
        for item in top_markets[:8]:
            rows.append(
                f"<tr><td>{item.get('display_title','-')}</td><td>{_region_label(item.get('region_tag'))}</td><td>{_fmt_pct(_safe_num(item.get('probability')))}</td><td>{_fmt_pct_delta(_safe_num(item.get('delta_24h')))}</td><td>{float(item.get('event_raw', 0)):.3f}</td></tr>"
            )
        st.markdown(
            '<table class="risk-table"><thead><tr><th>事件</th><th>区域</th><th>概率</th><th>24h</th><th>指数贡献</th></tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("暂无核心风险贡献。")
        st.markdown('</div>', unsafe_allow_html=True)

with right_bottom:
    st.markdown('<div class="table-card"><h3>风险来源分布</h3><div class="table-sub">按风险类别查看持续基础分与升级风险分</div>', unsafe_allow_html=True)
    if category_breakdown:
        cat_df = pd.DataFrame(category_breakdown)
        max_total = float((cat_df["baseline"].fillna(0) + cat_df["escalation"].fillna(0)).max() or 0.0)
        xaxis_max = max(40.0, math.ceil(max_total / 5.0) * 5.0)
        cat_df = cat_df.sort_values("raw", ascending=True)
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                y=cat_df["label"],
                x=cat_df["baseline"],
                orientation="h",
                name="持续基础分",
                marker=dict(color="#22c55e"),
                width=0.78,
                hovertemplate="%{y}<br>持续基础分 %{x:.1f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Bar(
                y=cat_df["label"],
                x=cat_df["escalation"],
                orientation="h",
                name="升级风险分",
                marker=dict(color="#38bdf8"),
                width=0.78,
                hovertemplate="%{y}<br>升级风险分 %{x:.1f}<extra></extra>",
            )
        )
        total_annotations = []
        for _, row in cat_df.iterrows():
            total_value = float(_safe_num(row.get("baseline")) + _safe_num(row.get("escalation")))
            total_annotations.append(
                dict(
                    x=min(xaxis_max - 0.4, total_value + 0.6),
                    y=row["label"],
                    xref="x",
                    yref="y",
                    text=f"{total_value:.1f}",
                    showarrow=False,
                    font=dict(color="#dbeafe", size=12, family="IBM Plex Mono"),
                    xanchor="left",
                )
            )
        fig.update_layout(
            height=330,
            margin=dict(l=18, r=24, t=16, b=8),
            barmode="stack",
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                range=[0, xaxis_max],
                dtick=5,
                gridcolor="rgba(148,163,184,.16)",
                tickfont=dict(color="#dbeafe", size=12),
                title_font=dict(color="#dbeafe"),
                zeroline=False,
            ),
            yaxis=dict(gridcolor="rgba(0,0,0,0)", tickfont=dict(color="#eef4ff", size=14)),
            legend=dict(
                orientation="h",
                y=1.12,
                x=0,
                font=dict(color="#dbeafe", size=13),
                bgcolor="rgba(8,16,33,.35)",
                bordercolor="rgba(148,163,184,.12)",
                borderwidth=1,
            ),
            font=dict(color="#eef4ff"),
            annotations=total_annotations,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("暂无分数构成。")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="table-card"><h3>持续基础分</h3><div class="table-sub">用停火、结束、恢复正常类反向市场推断长期混乱是否还会持续</div>', unsafe_allow_html=True)
if ongoing_clusters:
    cluster_rows = []
    for cluster in ongoing_clusters[:6]:
        reverse_markets = cluster.get("reverse_markets") or []
        reverse_text = " / ".join(
            [
                f"{item.get('market_title', '-')}: {_fmt_pct(_safe_num(item.get('probability_end')))} 结束"
                for item in reverse_markets[:2]
            ]
        ) or "暂无反向市场细节"
        cluster_rows.append(
            f"<tr><td>{cluster.get('display_title','-')}</td><td>{_fmt_pct(_safe_num(cluster.get('persistence_score')))}</td><td>{float(cluster.get('contribution',0)):.1f}</td><td>{reverse_text}</td></tr>"
        )
    st.markdown(
        '<table class="risk-table"><thead><tr><th>长期混乱簇</th><th>持续度</th><th>基础分贡献</th><th>反向市场</th></tr></thead><tbody>' + ''.join(cluster_rows) + '</tbody></table></div>',
        unsafe_allow_html=True,
    )
else:
    st.info("当前还没有识别到可用于反推长期混乱的反向市场。")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="table-card"><h3>今日扰动来源</h3><div class="table-sub">按 24 小时变化与当前贡献度排序，解释世界混乱指数为什么在动</div>', unsafe_allow_html=True)
move_items = sorted(top_markets, key=lambda item: abs(_safe_num(item.get("delta_24h")) * _safe_num(item.get("event_raw"))), reverse=True)
if move_items:
    explanation_map = {str(item.get("event_key")): item for item in headline_explanations}
    move_rows = []
    for item in move_items[:5]:
        explanation = explanation_map.get(str(item.get("event_key")), {})
        move_rows.append(
            f"<tr><td>{item.get('display_title', '-')}</td><td>{_fmt_pct_delta(_safe_num(item.get('delta_24h')))}</td><td>{float(item.get('event_raw', 0)):.3f}</td><td>{explanation.get('one_line_reason', '市场正在重新定价该事件。')}</td></tr>"
        )
    st.markdown(
        '<table class="risk-table"><thead><tr><th>事件</th><th>24h</th><th>指数贡献</th><th>一句话解释</th></tr></thead><tbody>' + ''.join(move_rows) + '</tbody></table></div>',
        unsafe_allow_html=True,
    )
else:
    st.info("暂无驱动变化数据。")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="table-card"><h3>双边风险监控</h3><div class="table-sub">按双边主题聚合，展示风险最高的关系与持续基础分</div>', unsafe_allow_html=True)
if pair_breakdown:
    pair_cards = []
    for pair in pair_breakdown[:8]:
        pair_tag = str(pair.get("pair_tag") or "-")
        members = [item for item in top_markets if str(item.get("pair_tag")) == pair_tag][:2]
        ongoing_members = [item for item in ongoing_clusters if str(item.get("pair_tag")) == pair_tag][:1]
        member_html = "".join(
            [
                    f"<div class='pair-item'><div style='font-weight:700;color:#f8fafc;'>{m.get('display_title','-')}</div><div style='font-size:13px;color:#cbd5e1;'>概率 {_fmt_pct(_safe_num(m.get('probability')))} · 24h {_fmt_pct_delta(_safe_num(m.get('delta_24h')))} · 指数贡献 {float(m.get('event_raw',0)):.3f}</div></div>"
                    for m in members
                ]
            )
        if not member_html and ongoing_members:
            member_html = "".join(
                [
                    f"<div class='pair-item'><div style='font-weight:700;color:#f8fafc;'>{m.get('display_title','-')}</div><div style='font-size:13px;color:#cbd5e1;'>持续度 {_fmt_pct(_safe_num(m.get('persistence_score')))} · 基础分贡献 {float(m.get('contribution',0)):.1f}</div></div>"
                    for m in ongoing_members
                ]
            )
        if not member_html:
            member_html = "<div class='pair-item'>暂无展开事件</div>"
        pair_cards.append(
            f"<div class='pair-card'><div class='pair-title'><span>{pair_tag}</span><span class='pair-chip'>share {float(pair.get('share_of_total',0))*100:.1f}%</span></div>{member_html}</div>"
        )
    st.markdown(f"<div class='pair-grid'>{''.join(pair_cards)}</div></div>", unsafe_allow_html=True)
else:
    st.info("暂无双边监控数据。")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown(
    f"""
    <div class="table-card">
      <h3>方法与说明</h3>
      <div class="table-sub">把“已经在持续的混乱”和“未来可能升级的风险”合在一起衡量。</div>
      <div style="line-height:1.85;color:#dbeafe;font-size:14px;">
        <div>1. 持续基础分：看停火、结束、恢复正常这类反向市场，越难结束，分数越高。</div>
        <div>2. 升级风险分：看冲突升级、封锁、衰退等市场的实时概率，再结合影响权重和流动性计算。</div>
        <div>3. 联动加成：多个地区同时升温时额外加分。</div>
        <div>4. 总分固定在 0-100 之间，不会超过 100。</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)
