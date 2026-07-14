from __future__ import annotations

import json
from pathlib import Path
from string import Template
from typing import Any

import streamlit as st
import streamlit.components.v1 as components


@st.cache_data(show_spinner=False)
def lightweight_charts_script() -> str:
    try:
        import lightweight_charts

        script_path = Path(lightweight_charts.__file__).resolve().parent / "js" / "lightweight-charts.js"
        return script_path.read_text(encoding="utf-8", errors="ignore").replace("</script>", "<\\/script>")
    except Exception:
        return ""


def lightweight_chart_loader_html() -> str:
    chart_js = lightweight_charts_script()
    if not chart_js:
        return ""
    return f"<script>{chart_js}</script>"


def render_option_kline_chart(
    payload: dict[str, Any],
    *,
    chart_loader_html: str,
    height: int = 650,
) -> None:
    """Render the shared US/ETF options Lightweight Charts surface."""
    if not chart_loader_html:
        st.warning("本地图表库加载失败，暂时无法渲染自研 K 线。")
        return
    period_payloads = payload.get("datasets") or {}
    if not payload.get("candles") and not any(
        isinstance(dataset, dict) and dataset.get("candles")
        for dataset in period_payloads.values()
    ):
        st.info("暂无有效 OHLC 数据，暂时无法渲染自研 K 线。")
        return

    payload_json = json.dumps(payload, ensure_ascii=False, allow_nan=False)
    chart_height = max(int(height or 650), 420)
    html = Template(
        """
        <!doctype html>
        <html lang="zh-CN">
        <head>
          <meta charset="utf-8" />
          <style>
            html, body {
              margin: 0;
              padding: 0;
              background: #ffffff;
              color: #0f172a;
              color-scheme: light;
              overflow: hidden;
              font-family: "Microsoft YaHei", "PingFang SC", Arial, sans-serif;
            }
            .lwc-shell {
              position: relative;
              height: ${height}px;
              min-height: ${height}px;
              border: 1px solid #e2e8f0;
              border-radius: 8px;
              background: #ffffff;
              overflow: hidden;
              box-sizing: border-box;
            }
            .lwc-head {
              position: absolute;
              left: 16px;
              right: 16px;
              top: 10px;
              z-index: 3;
              display: flex;
              align-items: center;
              justify-content: space-between;
              gap: 16px;
              pointer-events: none;
            }
            .lwc-left-head {
              display: inline-flex;
              align-items: center;
              gap: 10px;
              min-width: 0;
              flex: 1 1 auto;
            }
            .lwc-title {
              display: inline-flex;
              align-items: baseline;
              gap: 9px;
              padding: 6px 9px;
              border: 1px solid rgba(226, 232, 240, .88);
              border-radius: 8px;
              background: rgba(255,255,255,.86);
              box-shadow: 0 8px 28px rgba(15,23,42,.05);
              backdrop-filter: blur(8px);
              flex: 0 0 auto;
              white-space: nowrap;
            }
            .lwc-title strong {
              font-size: 14px;
              line-height: 1;
              font-weight: 760;
            }
            .lwc-title span,
            .lwc-latest span {
              color: #64748b;
              font-size: 11px;
            }
            .lwc-latest {
              display: inline-flex;
              align-items: baseline;
              gap: 8px;
              padding: 6px 9px;
              border: 1px solid rgba(226, 232, 240, .88);
              border-radius: 8px;
              background: rgba(255,255,255,.86);
              box-shadow: 0 8px 28px rgba(15,23,42,.05);
              backdrop-filter: blur(8px);
              font-size: 12px;
              font-weight: 700;
              flex: 0 0 auto;
              white-space: nowrap;
            }
            .lwc-controls {
              display: inline-flex;
              align-items: center;
              gap: 6px;
              padding: 4px;
              border: 1px solid rgba(226, 232, 240, .88);
              border-radius: 8px;
              background: rgba(255,255,255,.86);
              box-shadow: 0 8px 28px rgba(15,23,42,.05);
              backdrop-filter: blur(8px);
              pointer-events: auto;
              min-width: 0;
              max-width: 100%;
            }
            .lwc-period-controls {
              display: none;
              align-items: center;
              gap: 2px;
              flex: 0 0 auto;
            }
            .lwc-period-controls.visible {
              display: inline-flex;
            }
            .lwc-period {
              height: 26px;
              min-width: 42px;
              padding: 0 9px;
              border: 0;
              border-radius: 6px;
              background: transparent;
              color: #64748b;
              font-family: inherit;
              font-size: 11px;
              font-weight: 760;
              line-height: 1;
              cursor: pointer;
              color-scheme: light;
              appearance: none;
              -webkit-appearance: none;
              flex: 0 0 auto;
              white-space: nowrap;
              word-break: keep-all;
              writing-mode: horizontal-tb;
            }
            .lwc-period.active {
              background: #fee2e2;
              color: #dc2626;
            }
            .lwc-shell.hide-title .lwc-title,
            .lwc-shell.hide-latest .lwc-latest {
              display: none;
            }
            .lwc-tool-divider {
              width: 1px;
              height: 22px;
              margin: 0 2px;
              background: #e2e8f0;
            }
            .lwc-toggle {
              display: inline-flex;
              align-items: center;
              gap: 5px;
              height: 26px;
              padding: 0 8px;
              border: 0;
              border-radius: 6px;
              background: transparent;
              color: #64748b;
              font-size: 11px;
              font-weight: 760;
              line-height: 1;
              cursor: pointer;
              font-family: inherit;
              color-scheme: light;
              appearance: none;
              -webkit-appearance: none;
              flex: 0 0 auto;
              white-space: nowrap;
              word-break: keep-all;
              writing-mode: horizontal-tb;
            }
            .lwc-toggle::before {
              content: "";
              width: 7px;
              height: 7px;
              border-radius: 999px;
              background: var(--line-color, #94a3b8);
              opacity: .38;
            }
            .lwc-toggle.active {
              background: #eff6ff;
              color: #0f172a;
            }
            .lwc-toggle.active::before {
              opacity: 1;
            }
            .lwc-toggle:disabled {
              opacity: .42;
              cursor: not-allowed;
            }
            .lwc-draw-tool {
              height: 26px;
              padding: 0 8px;
              border: 0;
              border-radius: 6px;
              background: transparent;
              color: #475569;
              font-size: 11px;
              font-weight: 760;
              line-height: 1;
              cursor: pointer;
              font-family: inherit;
              color-scheme: light;
              appearance: none;
              -webkit-appearance: none;
              flex: 0 0 auto;
              white-space: nowrap;
              word-break: keep-all;
              writing-mode: horizontal-tb;
            }
            .lwc-draw-tool:hover,
            .lwc-draw-tool.active {
              background: #fff7ed;
              color: #ea580c;
            }
            .lwc-draw-tool.danger:hover,
            .lwc-draw-tool.danger.active {
              background: #fef2f2;
              color: #dc2626;
            }
            .lwc-readout {
              position: absolute;
              left: 16px;
              top: 52px;
              z-index: 3;
              display: none;
              flex-wrap: wrap;
              gap: 6px 10px;
              max-width: calc(100% - 32px);
              min-height: 26px;
              align-items: center;
              padding: 6px 10px;
              border: 1px solid rgba(226, 232, 240, .86);
              border-radius: 8px;
              background: rgba(255, 255, 255, .88);
              box-shadow: 0 10px 30px rgba(15, 23, 42, .05);
              backdrop-filter: blur(8px);
              color: #64748b;
              font-size: 11px;
              line-height: 1.2;
              pointer-events: none;
              box-sizing: border-box;
            }
            .lwc-readout.visible {
              display: flex;
            }
            .lwc-readout span {
              display: inline-flex;
              align-items: baseline;
              gap: 4px;
              white-space: nowrap;
            }
            .lwc-readout b {
              color: #0f172a;
              font-weight: 760;
            }
            .lwc-chart {
              position: absolute;
              inset: 0;
              background: #ffffff;
            }
            .lwc-chart.drawing-hover {
              cursor: grab;
            }
            .lwc-chart.dragging-drawing {
              cursor: grabbing;
            }
            .lwc-drawing-layer {
              position: absolute;
              inset: 0;
              z-index: 2;
              width: 100%;
              height: 100%;
              pointer-events: none;
              overflow: visible;
            }
            .lwc-drawing-layer.active {
              pointer-events: auto;
              cursor: crosshair;
            }
            .lwc-drawing-line {
              vector-effect: non-scaling-stroke;
              pointer-events: none;
            }
            .lwc-drawing-hit {
              stroke: transparent;
              stroke-width: 14;
              vector-effect: non-scaling-stroke;
              pointer-events: stroke;
              cursor: grab;
            }
            .lwc-drawing-label {
              fill: #475569;
              font-size: 11px;
              font-weight: 760;
              paint-order: stroke;
              stroke: rgba(255,255,255,.94);
              stroke-width: 4px;
              stroke-linejoin: round;
            }
            .lwc-draw-hint {
              position: absolute;
              right: 16px;
              bottom: 12px;
              z-index: 3;
              display: none;
              padding: 6px 9px;
              border: 1px solid rgba(251, 146, 60, .35);
              border-radius: 8px;
              background: rgba(255, 247, 237, .92);
              color: #9a3412;
              font-size: 11px;
              font-weight: 700;
              pointer-events: none;
              box-shadow: 0 8px 28px rgba(15,23,42,.05);
              backdrop-filter: blur(8px);
            }
            .lwc-draw-hint.visible {
              display: block;
            }
            .lwc-error {
              position: absolute;
              inset: 0;
              display: none;
              align-items: center;
              justify-content: center;
              padding: 24px;
              background: #ffffff;
              color: #475569;
              font-size: 13px;
              line-height: 1.45;
              text-align: center;
              box-sizing: border-box;
            }
            @media (max-width: 1040px) {
              .lwc-head {
                align-items: flex-start;
              }
              .lwc-left-head {
                flex-direction: column;
                align-items: flex-start;
                gap: 6px;
              }
              .lwc-controls {
                flex-wrap: wrap;
                row-gap: 4px;
              }
              .lwc-readout {
                top: 88px;
              }
            }
            @media (max-width: 720px) {
              .lwc-head {
                flex-direction: column;
                align-items: stretch;
                gap: 6px;
              }
              .lwc-left-head {
                width: 100%;
              }
              .lwc-controls {
                width: 100%;
                box-sizing: border-box;
              }
              .lwc-latest {
                align-self: flex-start;
              }
              .lwc-readout {
                top: 150px;
              }
            }
          </style>
        </head>
        <body>
          <div class="lwc-shell">
            <div class="lwc-head">
              <div class="lwc-left-head">
                <div class="lwc-title"><strong id="lwc-symbol"></strong><span id="lwc-title-context">日线 · 本地数据库 · 复权K线</span></div>
                <div class="lwc-controls" aria-label="图表指标开关">
                  <span id="lwc-period-controls" class="lwc-period-controls" aria-label="K线周期">
                    <button class="lwc-period active" data-period="daily" type="button">日K</button>
                    <button class="lwc-period" data-period="weekly" type="button">周K</button>
                    <span class="lwc-tool-divider"></span>
                  </span>
                  <button class="lwc-toggle" style="--line-color:#f59e0b" data-series="ma5" type="button">MA5</button>
                  <button class="lwc-toggle active" style="--line-color:#2563eb" data-series="ma20" type="button">MA20</button>
                  <button class="lwc-toggle" style="--line-color:#7c3aed" data-series="ma60" type="button">MA60</button>
                  <button class="lwc-toggle active" style="--line-color:#db2777" data-series="iv" type="button">ATM IV</button>
                  <span class="lwc-tool-divider"></span>
                  <button class="lwc-draw-tool" data-draw-mode="hline" type="button" title="点击图表价格位置添加水平线">水平线</button>
                  <button class="lwc-draw-tool" data-draw-mode="trend" type="button" title="点击两个位置添加趋势线">趋势线</button>
                  <button class="lwc-draw-tool danger" data-draw-mode="delete" type="button" title="点击已有画线删除">删除</button>
                  <button id="lwc-clear-drawings" class="lwc-draw-tool danger" type="button" title="清空当前标的全部本地画线">清空</button>
                </div>
              </div>
              <div class="lwc-latest"><span>最新</span><strong id="lwc-close"></strong><span id="lwc-change"></span></div>
            </div>
            <div id="lwc-readout" class="lwc-readout">移动十字光标查看每日 OHLC / 均线 / IV</div>
            <div id="lwc-chart" class="lwc-chart"></div>
            <svg id="lwc-drawing-layer" class="lwc-drawing-layer" aria-hidden="true"></svg>
            <div id="lwc-draw-hint" class="lwc-draw-hint"></div>
            <div id="lwc-error" class="lwc-error"></div>
          </div>
          ${chart_loader_html}
          <script>
          (function() {
            const payload = ${payload_json};
            const config = Object.assign({
              showTitle: true,
              showLatest: true,
              enablePeriodSwitch: false,
              activePeriod: "daily",
              priceDigits: 2,
              useTimeVisibleRange: false,
              storageNamespace: "us-options-chart-drawings",
              titleContext: "日线 · 本地数据库 · 复权K线",
              ivLabel: "ATM IV"
            }, payload.config || {});
            const datasets = payload.datasets || {};
            let activePeriod = String(config.activePeriod || "daily");
            let activeDataset = datasets[activePeriod] || payload;
            const errorEl = document.getElementById("lwc-error");
            function showError(message) {
              errorEl.style.display = "flex";
              errorEl.textContent = "图表加载失败：" + message;
            }
            function fmt(value, digits) {
              if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
              return Number(value).toLocaleString("en-US", {
                minimumFractionDigits: digits,
                maximumFractionDigits: digits
              });
            }
            try {
              if (!window.LightweightCharts || !window.LightweightCharts.createChart) {
                throw new Error("本地 lightweight-charts 未正确加载");
              }
              const shellEl = document.querySelector(".lwc-shell");
              const chartEl = document.getElementById("lwc-chart");
              const periodControls = document.getElementById("lwc-period-controls");
              const priceDigits = Math.max(0, Math.min(6, Number(config.priceDigits || 2)));
              const ivLabel = String(config.ivLabel || "ATM IV");
              shellEl.classList.toggle("hide-title", !config.showTitle);
              shellEl.classList.toggle("hide-latest", !config.showLatest);
              shellEl.dataset.activePeriod = activePeriod;
              periodControls.classList.toggle("visible", Boolean(config.enablePeriodSwitch));
              document.querySelector('[data-series="iv"]').textContent = ivLabel;
              document.getElementById("lwc-title-context").textContent = config.titleContext;
              const latest = activeDataset.latest || payload.latest || {};
              const change = Number(latest.change || 0);
              document.getElementById("lwc-symbol").textContent = payload.symbol || "";
              document.getElementById("lwc-close").textContent = fmt(latest.close, priceDigits);
              const changeEl = document.getElementById("lwc-change");
              changeEl.textContent = (change >= 0 ? "+" : "") + fmt(change, priceDigits) + " (" + (change >= 0 ? "+" : "") + fmt(latest.change_pct, 2) + "%)";
              changeEl.style.color = change >= 0 ? "#dc2626" : "#059669";

              const rect = chartEl.getBoundingClientRect();
              const chart = LightweightCharts.createChart(chartEl, {
                width: Math.max(rect.width, 480),
                height: Math.max(rect.height, 420),
                layout: {
                  background: { type: LightweightCharts.ColorType.Solid, color: "#ffffff" },
                  textColor: "#334155",
                  fontSize: 12,
                  fontFamily: "Microsoft YaHei, PingFang SC, Arial, sans-serif"
                },
                grid: {
                  vertLines: { color: "rgba(148, 163, 184, 0.12)" },
                  horzLines: { color: "rgba(148, 163, 184, 0.16)" }
                },
                crosshair: {
                  mode: LightweightCharts.CrosshairMode.Normal,
                  vertLine: { color: "rgba(71, 85, 105, 0.45)", style: 2, width: 1 },
                  horzLine: { color: "rgba(71, 85, 105, 0.45)", style: 2, width: 1 }
                },
                rightPriceScale: {
                  borderVisible: false,
                  scaleMargins: { top: 0.08, bottom: 0.40 }
                },
                leftPriceScale: {
                  visible: false,
                  borderVisible: false,
                  scaleMargins: { top: 0.70, bottom: 0.15 }
                },
                timeScale: {
                  borderVisible: false,
                  rightOffset: 8,
                  barSpacing: 6,
                  minBarSpacing: 4,
                  timeVisible: true,
                  secondsVisible: false
                },
                localization: {
                  locale: "zh-CN",
                  priceFormatter: function(price) { return fmt(price, priceDigits); }
                },
                handleScale: true,
                handleScroll: true
              });

              const candleSeries = chart.addCandlestickSeries({
                upColor: "#ef4444",
                downColor: "#10b981",
                borderUpColor: "#ef4444",
                borderDownColor: "#10b981",
                wickUpColor: "#ef4444",
                wickDownColor: "#10b981",
                priceLineColor: "#64748b",
                lastValueVisible: true,
                priceFormat: {
                  type: "price",
                  precision: priceDigits,
                  minMove: Math.pow(10, -priceDigits)
                }
              });
              candleSeries.setData(activeDataset.candles || []);
              (payload.referenceLines || []).forEach((line) => {
                const price = Number(line && line.price);
                if (!Number.isFinite(price)) return;
                candleSeries.createPriceLine({
                  price,
                  color: line.color || "#64748b",
                  lineWidth: Number(line.lineWidth || 1),
                  lineStyle: LightweightCharts.LineStyle.Dashed,
                  axisLabelVisible: true,
                  title: String(line.title || "")
                });
              });
              const referencePrices = (payload.referenceLines || [])
                .map((line) => Number(line && line.price))
                .filter((price) => Number.isFinite(price));
              const referenceExtentSeries = referencePrices.length
                ? chart.addLineSeries({
                    color: "rgba(255,255,255,0)",
                    lineWidth: 1,
                    priceScaleId: "right",
                    priceLineVisible: false,
                    lastValueVisible: false,
                    crosshairMarkerVisible: false
                  })
                : null;
              function updateReferenceScaleData() {
                if (!referenceExtentSeries) return;
                const candles = activeDataset.candles || [];
                if (!candles.length) {
                  referenceExtentSeries.setData([]);
                  return;
                }
                referenceExtentSeries.setData([
                  { time: candles[0].time, value: Math.min(...referencePrices) },
                  { time: candles[candles.length - 1].time, value: Math.max(...referencePrices) }
                ]);
              }
              updateReferenceScaleData();

              const drawingLayer = document.getElementById("lwc-drawing-layer");
              const drawHintEl = document.getElementById("lwc-draw-hint");
              const clearDrawingsButton = document.getElementById("lwc-clear-drawings");
              const storageKey = String(config.storageNamespace || "us-options-chart-drawings") + ":" + String(payload.symbol || "UNKNOWN").toUpperCase();
              const drawingColors = { hline: "#f97316", trend: "#2563eb", draft: "#64748b" };
              let drawings = loadDrawings();
              let drawMode = "none";
              let pendingTrend = null;
              let draftPoint = null;
              let dragState = null;

              function loadDrawings() {
                try {
                  const raw = window.localStorage.getItem(storageKey);
                  const rows = raw ? JSON.parse(raw) : [];
                  if (!Array.isArray(rows)) return [];
                  return rows.filter((item) => item && (item.type === "hline" || item.type === "trend"));
                } catch (_) {
                  return [];
                }
              }
              function saveDrawings() {
                try {
                  window.localStorage.setItem(storageKey, JSON.stringify(drawings));
                } catch (_) {}
              }
              function drawingId() {
                return "d" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
              }
              function setDrawHint(text) {
                if (!text) {
                  drawHintEl.classList.remove("visible");
                  drawHintEl.textContent = "";
                  return;
                }
                drawHintEl.textContent = text;
                drawHintEl.classList.add("visible");
              }
              function setDrawMode(nextMode) {
                drawMode = drawMode === nextMode ? "none" : nextMode;
                pendingTrend = null;
                draftPoint = null;
                drawingLayer.classList.toggle("active", drawMode !== "none");
                document.querySelectorAll("[data-draw-mode]").forEach((button) => {
                  button.classList.toggle("active", button.dataset.drawMode === drawMode);
                });
                if (drawMode === "hline") setDrawHint("点击图表任意价格位置添加水平线；Esc 退出");
                else if (drawMode === "trend") setDrawHint("依次点击趋势线的起点和终点；Esc 退出");
                else if (drawMode === "delete") setDrawHint("点击已有画线删除；Esc 退出");
                else setDrawHint("");
                renderDrawings();
              }
              function pointFromEvent(event) {
                const box = chartEl.getBoundingClientRect();
                const x = event.clientX - box.left;
                const y = event.clientY - box.top;
                const time = chart.timeScale().coordinateToTime(x);
                const price = candleSeries.coordinateToPrice(y);
                if (!time || price === null || price === undefined || Number.isNaN(Number(price))) return null;
                return { x, y, time: timeKey(time), price: Number(price) };
              }
              function chartPointFromEvent(event) {
                const box = chartEl.getBoundingClientRect();
                return { x: event.clientX - box.left, y: event.clientY - box.top };
              }
              function createSvgLine(x1, y1, x2, y2, color, width, id, dashed) {
                const visibleLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
                visibleLine.setAttribute("x1", x1);
                visibleLine.setAttribute("y1", y1);
                visibleLine.setAttribute("x2", x2);
                visibleLine.setAttribute("y2", y2);
                visibleLine.setAttribute("stroke", color);
                visibleLine.setAttribute("stroke-width", width);
                visibleLine.setAttribute("stroke-linecap", "round");
                visibleLine.setAttribute("class", "lwc-drawing-line");
                if (dashed) visibleLine.setAttribute("stroke-dasharray", "6 5");
                drawingLayer.appendChild(visibleLine);

                if (id) {
                  const hitLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
                  hitLine.setAttribute("x1", x1);
                  hitLine.setAttribute("y1", y1);
                  hitLine.setAttribute("x2", x2);
                  hitLine.setAttribute("y2", y2);
                  hitLine.setAttribute("data-drawing-id", id);
                  hitLine.setAttribute("class", "lwc-drawing-hit");
                  drawingLayer.appendChild(hitLine);
                }
              }
              function createSvgLabel(text, x, y) {
                const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
                label.setAttribute("x", x);
                label.setAttribute("y", y);
                label.setAttribute("class", "lwc-drawing-label");
                label.textContent = text;
                drawingLayer.appendChild(label);
              }
              function renderDrawings() {
                drawingLayer.replaceChildren();
                const box = chartEl.getBoundingClientRect();
                const width = Math.max(box.width, 480);
                const height = Math.max(box.height, 420);
                drawingLayer.setAttribute("viewBox", "0 0 " + width + " " + height);
                drawingLayer.setAttribute("width", width);
                drawingLayer.setAttribute("height", height);

                drawings.forEach((drawing) => {
                  if (drawing.type === "hline") {
                    const y = candleSeries.priceToCoordinate(Number(drawing.price));
                    if (y === null || y === undefined || y < -40 || y > height + 40) return;
                    createSvgLine(0, y, width, y, drawing.color || drawingColors.hline, 1.6, drawing.id, false);
                    createSvgLabel(fmt(drawing.price, priceDigits), Math.max(width - 70, 8), Math.max(y - 6, 14));
                    return;
                  }
                  if (drawing.type === "trend") {
                    const x1 = chart.timeScale().timeToCoordinate(drawing.time1);
                    const x2 = chart.timeScale().timeToCoordinate(drawing.time2);
                    const y1 = candleSeries.priceToCoordinate(Number(drawing.price1));
                    const y2 = candleSeries.priceToCoordinate(Number(drawing.price2));
                    if ([x1, x2, y1, y2].some((value) => value === null || value === undefined)) return;
                    createSvgLine(x1, y1, x2, y2, drawing.color || drawingColors.trend, 1.8, drawing.id, false);
                  }
                });

                if (pendingTrend && draftPoint) {
                  createSvgLine(pendingTrend.x, pendingTrend.y, draftPoint.x, draftPoint.y, drawingColors.draft, 1.5, null, true);
                }
              }
              function removeDrawing(id) {
                drawings = drawings.filter((item) => item.id !== id);
                saveDrawings();
                renderDrawings();
              }
              function drawingCoords(drawing) {
                if (!drawing) return null;
                if (drawing.type === "hline") {
                  const y = candleSeries.priceToCoordinate(Number(drawing.price));
                  if (y === null || y === undefined) return null;
                  return { y };
                }
                if (drawing.type === "trend") {
                  const x1 = chart.timeScale().timeToCoordinate(drawing.time1);
                  const x2 = chart.timeScale().timeToCoordinate(drawing.time2);
                  const y1 = candleSeries.priceToCoordinate(Number(drawing.price1));
                  const y2 = candleSeries.priceToCoordinate(Number(drawing.price2));
                  if ([x1, x2, y1, y2].some((value) => value === null || value === undefined)) return null;
                  return { x1, y1, x2, y2 };
                }
                return null;
              }
              function distanceToSegment(px, py, x1, y1, x2, y2) {
                const dx = x2 - x1;
                const dy = y2 - y1;
                if (dx === 0 && dy === 0) {
                  return Math.hypot(px - x1, py - y1);
                }
                const t = Math.max(0, Math.min(1, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)));
                const projectionX = x1 + t * dx;
                const projectionY = y1 + t * dy;
                return Math.hypot(px - projectionX, py - projectionY);
              }
              function nearestDrawing(point, threshold) {
                let best = null;
                drawings.forEach((drawing) => {
                  const coords = drawingCoords(drawing);
                  if (!coords) return;
                  let distance = Infinity;
                  if (drawing.type === "hline") {
                    distance = Math.abs(point.y - coords.y);
                  } else if (drawing.type === "trend") {
                    distance = distanceToSegment(point.x, point.y, coords.x1, coords.y1, coords.x2, coords.y2);
                  }
                  if (distance <= threshold && (!best || distance < best.distance)) {
                    best = { drawing, coords, distance };
                  }
                });
                return best;
              }
              function startDrawingDrag(event, drawingId) {
                const point = chartPointFromEvent(event);
                const hit = drawingId
                  ? (() => {
                      const drawing = drawings.find((item) => item.id === drawingId);
                      const coords = drawingCoords(drawing);
                      return drawing && coords ? { drawing, coords, distance: 0 } : null;
                    })()
                  : nearestDrawing(point, 8);
                if (!hit) return false;
                event.preventDefault();
                event.stopPropagation();
                dragState = {
                  id: hit.drawing.id,
                  type: hit.drawing.type,
                  startX: point.x,
                  startY: point.y,
                  coords: { ...hit.coords },
                  drawing: { ...hit.drawing },
                };
                chartEl.classList.add("dragging-drawing");
                setDrawHint("拖动画线调整位置，松开后自动保存");
                window.addEventListener("pointermove", dragDrawing, true);
                window.addEventListener("pointerup", stopDrawingDrag, true);
                window.addEventListener("pointercancel", stopDrawingDrag, true);
                return true;
              }
              function dragDrawing(event) {
                if (!dragState) return;
                event.preventDefault();
                event.stopPropagation();
                const point = chartPointFromEvent(event);
                const dx = point.x - dragState.startX;
                const dy = point.y - dragState.startY;
                const drawing = drawings.find((item) => item.id === dragState.id);
                if (!drawing) return;
                if (dragState.type === "hline") {
                  const price = candleSeries.coordinateToPrice(dragState.coords.y + dy);
                  if (price !== null && price !== undefined && !Number.isNaN(Number(price))) {
                    drawing.price = Number(price);
                  }
                } else if (dragState.type === "trend") {
                  const time1 = chart.timeScale().coordinateToTime(dragState.coords.x1 + dx);
                  const time2 = chart.timeScale().coordinateToTime(dragState.coords.x2 + dx);
                  const price1 = candleSeries.coordinateToPrice(dragState.coords.y1 + dy);
                  const price2 = candleSeries.coordinateToPrice(dragState.coords.y2 + dy);
                  if (
                    time1 && time2 &&
                    price1 !== null && price1 !== undefined &&
                    price2 !== null && price2 !== undefined &&
                    !Number.isNaN(Number(price1)) &&
                    !Number.isNaN(Number(price2))
                  ) {
                    drawing.time1 = timeKey(time1);
                    drawing.time2 = timeKey(time2);
                    drawing.price1 = Number(price1);
                    drawing.price2 = Number(price2);
                  }
                }
                renderDrawings();
              }
              function stopDrawingDrag(event) {
                if (!dragState) return;
                event.preventDefault();
                event.stopPropagation();
                dragState = null;
                chartEl.classList.remove("dragging-drawing");
                saveDrawings();
                setDrawHint(drawMode === "hline" ? "点击图表任意价格位置添加水平线；Esc 退出"
                  : drawMode === "trend" ? "依次点击趋势线的起点和终点；Esc 退出"
                  : drawMode === "delete" ? "点击已有画线删除；Esc 退出"
                  : "");
                window.removeEventListener("pointermove", dragDrawing, true);
                window.removeEventListener("pointerup", stopDrawingDrag, true);
                window.removeEventListener("pointercancel", stopDrawingDrag, true);
              }
              function updateDrawingHover(event) {
                if (drawMode !== "none" || dragState) {
                  chartEl.classList.remove("drawing-hover");
                  return;
                }
                const hit = nearestDrawing(chartPointFromEvent(event), 8);
                chartEl.classList.toggle("drawing-hover", Boolean(hit));
              }
              document.querySelectorAll("[data-draw-mode]").forEach((button) => {
                button.addEventListener("click", () => setDrawMode(button.dataset.drawMode));
              });
              clearDrawingsButton.addEventListener("click", () => {
                if (!drawings.length) return;
                drawings = [];
                pendingTrend = null;
                draftPoint = null;
                saveDrawings();
                renderDrawings();
              });
              drawingLayer.addEventListener("pointerdown", (event) => {
                const target = event.target && event.target.closest ? event.target.closest("[data-drawing-id]") : null;
                if (drawMode === "none") {
                  if (target && target.dataset.drawingId) {
                    startDrawingDrag(event, target.dataset.drawingId);
                  }
                  return;
                }
                event.preventDefault();
                event.stopPropagation();
                if (drawMode === "delete") {
                  if (target && target.dataset.drawingId) removeDrawing(target.dataset.drawingId);
                  return;
                }
                if (target && target.dataset.drawingId && startDrawingDrag(event, target.dataset.drawingId)) {
                  return;
                }
                const point = pointFromEvent(event);
                if (!point) return;
                if (drawMode === "hline") {
                  drawings.push({
                    id: drawingId(),
                    type: "hline",
                    price: point.price,
                    color: drawingColors.hline
                  });
                  saveDrawings();
                  renderDrawings();
                  return;
                }
                if (drawMode === "trend") {
                  if (!pendingTrend) {
                    pendingTrend = point;
                    draftPoint = point;
                    setDrawHint("已选择起点，再点击一次设置终点；Esc 取消");
                    renderDrawings();
                  } else {
                    drawings.push({
                      id: drawingId(),
                      type: "trend",
                      time1: pendingTrend.time,
                      price1: pendingTrend.price,
                      time2: point.time,
                      price2: point.price,
                      color: drawingColors.trend
                    });
                    pendingTrend = null;
                    draftPoint = null;
                    setDrawHint("趋势线已添加，可继续画线；Esc 退出");
                    saveDrawings();
                    renderDrawings();
                  }
                }
              });
              drawingLayer.addEventListener("pointermove", (event) => {
                if (drawMode !== "trend" || !pendingTrend) return;
                const point = pointFromEvent(event);
                if (!point) return;
                draftPoint = point;
                renderDrawings();
              });
              window.addEventListener("keydown", (event) => {
                if (event.key === "Escape" && drawMode !== "none") setDrawMode("none");
              });
              chartEl.addEventListener("pointerdown", (event) => {
                if (drawMode !== "none") return;
                startDrawingDrag(event, null);
              }, true);
              chartEl.addEventListener("pointermove", updateDrawingHover, true);
              chartEl.addEventListener("mouseleave", () => {
                if (!dragState) chartEl.classList.remove("drawing-hover");
              });

              const volumeSeries = chart.addHistogramSeries({
                priceFormat: { type: "volume" },
                priceScaleId: "volume",
                priceLineVisible: false,
                lastValueVisible: false
              });
              volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.86, bottom: 0.02 } });
              volumeSeries.setData(activeDataset.volumes || []);

              const overlaySeries = {};
              const defaultVisible = { ma5: false, ma20: true, ma60: false, iv: true };
              function setSeriesVisible(item, visible) {
                item.visible = visible;
                if (item.button) item.button.classList.toggle("active", visible);
                try {
                  item.series.applyOptions({ visible: visible });
                } catch (_) {}
                item.series.setData(visible ? item.data : []);
                if (item.key === "iv") {
                  chart.applyOptions({
                    leftPriceScale: {
                      visible: visible,
                      borderVisible: false,
                      scaleMargins: { top: 0.70, bottom: 0.15 }
                    }
                  });
                }
              }
              function addToggleLine(key, title, color, priceScaleId, lineWidth) {
                const data = activeDataset[key] || [];
                const button = document.querySelector('[data-series="' + key + '"]');
                if (!data.length) {
                  if (button) {
                    button.disabled = true;
                    button.classList.remove("active");
                  }
                  return null;
                }
                const series = chart.addLineSeries({
                  color,
                  lineWidth,
                  title,
                  priceScaleId,
                  priceLineVisible: false,
                  lastValueVisible: false
                });
                overlaySeries[key] = { key, series, data, visible: false, button };
                setSeriesVisible(overlaySeries[key], Boolean(defaultVisible[key]));
                return series;
              }
              addToggleLine("ma5", "MA5", "#f59e0b", "right", 1);
              addToggleLine("ma20", "MA20", "#2563eb", "right", 2);
              addToggleLine("ma60", "MA60", "#7c3aed", "right", 2);
              const ivSeries = addToggleLine("iv", ivLabel, "#db2777", "left", 2);
              if (ivSeries) {
                ivSeries.priceScale().applyOptions({ scaleMargins: { top: 0.70, bottom: 0.15 } });
              }

              document.querySelectorAll(".lwc-toggle").forEach((button) => {
                const key = button.dataset.series;
                const item = overlaySeries[key];
                if (!item) return;
                button.addEventListener("click", () => {
                  setSeriesVisible(item, !item.visible);
                });
              });

              const readoutEl = document.getElementById("lwc-readout");
              function timeKey(time) {
                if (!time) return null;
                if (typeof time === "string") return time;
                if (typeof time === "object" && time.year && time.month && time.day) {
                  return String(time.year) + "-" + String(time.month).padStart(2, "0") + "-" + String(time.day).padStart(2, "0");
                }
                return String(time);
              }
              function valueMap(rows, field) {
                const map = new Map();
                (rows || []).forEach((row) => {
                  if (!row || !row.time) return;
                  map.set(String(row.time), field ? row[field] : row);
                });
                return map;
              }
              function buildLookup(dataset) {
                return {
                  candles: valueMap(dataset.candles || null),
                  volume: valueMap(dataset.volumes || null, "value"),
                  ma5: valueMap(dataset.ma5 || null, "value"),
                  ma20: valueMap(dataset.ma20 || null, "value"),
                  ma60: valueMap(dataset.ma60 || null, "value"),
                  iv: valueMap(dataset.iv || null, "value")
                };
              }
              let lookup = buildLookup(activeDataset);
              function hideReadout() {
                readoutEl.classList.remove("visible");
                readoutEl.replaceChildren();
              }
              function fmtVolume(value) {
                if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
                const number = Number(value);
                const abs = Math.abs(number);
                if (abs >= 1000000000) return fmt(number / 1000000000, 2) + "B";
                if (abs >= 1000000) return fmt(number / 1000000, 2) + "M";
                if (abs >= 1000) return fmt(number / 1000, 1) + "K";
                return fmt(number, 0);
              }
              function addReadoutItem(label, value) {
                const span = document.createElement("span");
                const strong = document.createElement("b");
                strong.textContent = label;
                span.appendChild(strong);
                span.appendChild(document.createTextNode(value));
                readoutEl.appendChild(span);
              }
              function renderReadout(time) {
                const key = timeKey(time);
                const candle = key ? lookup.candles.get(key) : null;
                readoutEl.replaceChildren();
                if (!key || !candle) {
                  hideReadout();
                  return;
                }
                readoutEl.classList.add("visible");
                addReadoutItem("日期", key);
                addReadoutItem("开", fmt(candle.open, priceDigits));
                addReadoutItem("高", fmt(candle.high, priceDigits));
                addReadoutItem("低", fmt(candle.low, priceDigits));
                addReadoutItem("收", fmt(candle.close, priceDigits));
                addReadoutItem("量", fmtVolume(lookup.volume.get(key)));
                addReadoutItem("MA5", fmt(lookup.ma5.get(key), priceDigits));
                addReadoutItem("MA20", fmt(lookup.ma20.get(key), priceDigits));
                addReadoutItem("MA60", fmt(lookup.ma60.get(key), priceDigits));
                const ivValue = lookup.iv.get(key);
                addReadoutItem(ivLabel, ivValue === null || ivValue === undefined ? "-" : fmt(ivValue, 2) + "%");
              }
              let pointerInsideChart = false;
              chartEl.addEventListener("mouseenter", () => {
                pointerInsideChart = true;
              });
              chartEl.addEventListener("mouseleave", () => {
                pointerInsideChart = false;
                hideReadout();
              });
              chart.subscribeCrosshairMove((param) => {
                if (!pointerInsideChart || !param || !param.time || !param.point || param.point.x < 0 || param.point.y < 0) {
                  hideReadout();
                  return;
                }
                renderReadout(param.time);
              });
              hideReadout();

              function fitActiveDataset() {
                chart.timeScale().fitContent();
                const length = (activeDataset.candles || []).length;
                if (length > 130) {
                  if (config.useTimeVisibleRange) {
                    chart.timeScale().setVisibleRange({
                      from: activeDataset.candles[length - 126].time,
                      to: activeDataset.candles[length - 1].time
                    });
                  } else {
                    chart.timeScale().setVisibleLogicalRange({ from: length - 126, to: length + 6 });
                  }
                }
              }
              function switchPeriod(nextPeriod) {
                const nextDataset = datasets[nextPeriod];
                if (!nextDataset || !Array.isArray(nextDataset.candles) || !nextDataset.candles.length) return;
                activePeriod = nextPeriod;
                activeDataset = nextDataset;
                shellEl.dataset.activePeriod = activePeriod;
                candleSeries.setData(activeDataset.candles || []);
                volumeSeries.setData(activeDataset.volumes || []);
                updateReferenceScaleData();
                Object.values(overlaySeries).forEach((item) => {
                  item.data = activeDataset[item.key] || [];
                  if (item.button) item.button.disabled = !item.data.length;
                  setSeriesVisible(item, Boolean(item.visible && item.data.length));
                });
                lookup = buildLookup(activeDataset);
                document.querySelectorAll("[data-period]").forEach((button) => {
                  button.classList.toggle("active", button.dataset.period === activePeriod);
                });
                hideReadout();
                fitActiveDataset();
                renderDrawings();
              }
              document.querySelectorAll("[data-period]").forEach((button) => {
                button.classList.toggle("active", button.dataset.period === activePeriod);
                button.addEventListener("click", () => switchPeriod(button.dataset.period));
              });
              fitActiveDataset();
              renderDrawings();
              chart.timeScale().subscribeVisibleLogicalRangeChange(() => renderDrawings());

              function resize() {
                const box = chartEl.getBoundingClientRect();
                chart.resize(Math.max(box.width, 480), Math.max(box.height, 420));
                renderDrawings();
              }
              if ("ResizeObserver" in window) {
                new ResizeObserver(resize).observe(chartEl);
              } else {
                window.addEventListener("resize", resize);
              }
              requestAnimationFrame(resize);
            } catch (err) {
              showError(err && err.message ? err.message : String(err));
            }
          })();
          </script>
        </body>
        </html>
        """
    ).substitute(
        height=chart_height,
        chart_loader_html=chart_loader_html,
        payload_json=payload_json,
    )
    components.html(html, height=chart_height + 2, scrolling=False)


__all__ = ["lightweight_chart_loader_html", "render_option_kline_chart"]
