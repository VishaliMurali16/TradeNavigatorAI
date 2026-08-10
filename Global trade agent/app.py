import asyncio
import concurrent.futures
import json
import os
import re
import threading
import time

from flask import Flask, render_template_string, redirect, url_for, jsonify, request
from dotenv import load_dotenv
from air import AsyncAIRefinery

from agents_registry import AGENTS, CLUSTERS, get_agent, agents_by_cluster
from data_simulator import get_kpis, get_alerts, get_tariff_sources, get_tariff_feed
import fta_data_source
from fta_data_source import SHIPMENT_TEMPLATE_CSV_WITH_DICT, COO_TEMPLATE_CSV
from fta_simulator import (COUNTRY_NAMES as _CTRY, PREF_STATUS_LABELS, ROO_STATUS_LABELS,
                           POO_STATUS_LABELS, FIELD_DICTIONARY as _FIELD_DICTIONARY)

load_dotenv()
_API_KEY = str(os.getenv("API_KEY"))

app = Flask(__name__)

# Cache so repeated refreshes never trigger a second AI call
_ai_cache: dict = {"text": None, "ts": 0.0, "lock": threading.Lock(), "in_flight": False}
_AI_CACHE_TTL = 60  # seconds

# ---------------------------------------------------------------------------
# AI Integration
# ---------------------------------------------------------------------------

async def _call_ai(system: str, user: str) -> str:
    client = AsyncAIRefinery(api_key=_API_KEY)
    response = await client.chat.completions.create(
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        model="Qwen/Qwen3-32B",
    )
    return response.choices[0].message.content


def _run_ai_in_thread(system: str, user: str) -> str:
    """Run async AI call in a fresh event loop on a background thread."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(asyncio.wait_for(_call_ai(system, user), timeout=30))
    finally:
        loop.close()


def _strip_reasoning(text: str) -> str:
    """Remove model chain-of-thought artifacts before the actual summary.

    Qwen3 thinking models can emit <think>…</think> blocks or plain-text
    reasoning prefixes.  Strip both so only the clean summary reaches the UI.
    """
    # Remove <think>…</think> blocks (Qwen3 extended-thinking format)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    text = text.strip()

    # Drop leading lines that are clearly internal monologue
    _reasoning_re = re.compile(
        r"^(okay[,.]?|let me|the user wants|first[,.]?|i need to|alright[,.]?|"
        r"sure[,.]?|certainly[,.]?|here'?s?( is)?|to summarize|based on (the|this)|"
        r"looking at|analyzing|my task is|i'?ll|i will|so[,.]?|now[,.]?)",
        re.IGNORECASE,
    )
    lines = text.splitlines()
    while lines and _reasoning_re.match(lines[0].strip()):
        lines.pop(0)

    return "\n".join(lines).strip()


def _fallback_summary(context: dict) -> str:
    rate  = context.get("fta_capture_rate_pct", "—")
    risk  = context.get("value_at_risk_m", "—")
    flags = context.get("open_compliance_flags", "—")
    return (
        f"Your FTA capture rate of {rate}% signals meaningful preferential-duty "
        f"opportunities remain unclaimed — prioritise an origin-qualification review. "
        f"With ${risk}M at risk and {flags} open compliance flags, immediate attention "
        f"to screening and tariff-mitigation planning is advised."
    )


def generate_ai_explanation(context: dict) -> str:
    """
    Return a cached AI summary if one exists and is fresh; otherwise fetch a
    new one from Qwen/Qwen3-32B in the background and return the fallback
    immediately so the caller is never blocked.

    *** PLUG NEW AGENT LOGIC HERE ***
    When activating an agent, pass its specific context dict and update the
    prompt below to match that agent's domain.
    """
    global _ai_cache

    now = time.time()
    with _ai_cache["lock"]:
        # Cache hit — return immediately
        if _ai_cache["text"] and (now - _ai_cache["ts"]) < _AI_CACHE_TTL:
            return _ai_cache["text"]

        # Already fetching — return fallback so this request doesn't block
        if _ai_cache["in_flight"]:
            return _fallback_summary(context)

        _ai_cache["in_flight"] = True

    system_prompt = (
        "You are TradeNavigator AI, a senior global trade advisor briefing the C-suite.\n"
        "Your ONLY output is a 3–4 sentence executive trade posture summary. "
        "Follow these rules exactly:\n"
        "1. Output ONLY the summary — no thinking, no reasoning steps, no preamble, "
        "no 'here is the summary', no 'based on the data' opener.\n"
        "2. Use the exact figures provided. Do not round or invent numbers.\n"
        "3. Cover one key OPPORTUNITY and one key RISK with brief impact reasoning "
        "suitable for a CFO or VP of Trade.\n"
        "4. Tone: confident, direct, boardroom-ready. No hedging. No filler phrases.\n"
        "Begin the summary immediately with the first word of the first sentence."
    )

    user_prompt = (
        "Current trade KPI snapshot:\n"
        f"- Total duty paid this quarter: ${context.get('total_duty_paid_m', 'N/A')}M\n"
        f"- FTA capture rate: {context.get('fta_capture_rate_pct', 'N/A')}% of eligible shipments\n"
        f"- Duty drawback recovered: ${context.get('drawback_recovered_k', 'N/A')}K YTD\n"
        f"- Open compliance flags: {context.get('open_compliance_flags', 'N/A')}\n"
        f"- Active tariff alerts: {context.get('active_tariff_alerts', 'N/A')}\n"
        f"- Value at risk: ${context.get('value_at_risk_m', 'N/A')}M estimated exposure\n\n"
        "Write the executive trade posture summary now."
    )

    def _fetch():
        try:
            raw    = _run_ai_in_thread(system_prompt, user_prompt)
            result = _strip_reasoning(raw)
            if not result:
                result = _fallback_summary(context)
        except Exception:
            result = _fallback_summary(context)
        with _ai_cache["lock"]:
            _ai_cache["text"] = result
            _ai_cache["ts"]   = time.time()
            _ai_cache["in_flight"] = False

    threading.Thread(target=_fetch, daemon=True).start()
    return _fallback_summary(context)


# ---------------------------------------------------------------------------
# API endpoint — JS fetches this after page paint; never blocks page load
# ---------------------------------------------------------------------------

@app.route("/api/posture-summary")
def api_posture_summary():
    kpis = get_kpis()
    text = generate_ai_explanation(kpis)
    return jsonify({"summary": text, "kpis": kpis})


@app.route("/api/tariff-feed")
def api_tariff_feed():
    """Return live tariff events and source monitoring data."""
    sources = get_tariff_sources()
    feed = get_tariff_feed()
    return jsonify({"sources": sources, "feed": feed})


# ---------------------------------------------------------------------------
# Shared CSS
# ---------------------------------------------------------------------------

_CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, sans-serif;
    background: #f4f4f8;
    color: #1a0533;
    display: flex;
    min-height: 100vh;
}

/* ── Sidebar ─────────────────────────────────────────────────── */
.sidebar {
    width: 260px;
    min-height: 100vh;
    background: #1a0533;
    color: #fff;
    display: flex;
    flex-direction: column;
    position: fixed;
    top: 0; left: 0; bottom: 0;
    overflow-y: auto;
    z-index: 100;
}
.sidebar-brand {
    padding: 24px 20px 16px;
    border-bottom: 1px solid rgba(255,255,255,0.1);
}
.sidebar-brand .logo-text {
    font-size: 1.05rem;
    font-weight: 700;
    color: #A100FF;
    letter-spacing: 0.5px;
}
.sidebar-brand .logo-sub {
    font-size: 0.7rem;
    color: rgba(255,255,255,0.45);
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin-top: 2px;
}
.sidebar-nav { padding: 12px 0; flex: 1; }
.nav-home {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 20px;
    color: rgba(255,255,255,0.9);
    text-decoration: none;
    font-weight: 600;
    font-size: 0.9rem;
    border-left: 3px solid #A100FF;
    background: rgba(161,0,255,0.12);
    margin-bottom: 8px;
}
.nav-home:hover { background: rgba(161,0,255,0.22); }
.cluster-label {
    padding: 10px 20px 4px;
    font-size: 0.65rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.8px;
    opacity: 0.55;
    margin-top: 8px;
}
.nav-agent {
    display: flex; align-items: center; gap: 8px;
    padding: 8px 20px 8px 28px;
    color: rgba(255,255,255,0.78);
    text-decoration: none;
    font-size: 0.82rem;
    transition: background 0.15s;
    position: relative;
}
.nav-agent:hover { background: rgba(255,255,255,0.07); color: #fff; }
.nav-agent.active { color: #fff; font-weight: 600; }
.nav-agent .soon-tag {
    margin-left: auto;
    font-size: 0.6rem;
    background: rgba(255,255,255,0.12);
    color: rgba(255,255,255,0.45);
    padding: 2px 6px;
    border-radius: 4px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
}
.nav-agent.coming-soon { opacity: 0.5; }

/* ── Main content ────────────────────────────────────────────── */
.main { margin-left: 260px; flex: 1; display: flex; }
.main-content { flex: 1; padding: 32px 36px; overflow-y: auto; }
.main-right-pane { width: 340px; background: #fff; border-left: 1px solid #e8e0f0; overflow-y: auto; display: flex; flex-direction: column; }

/* ── Right tariff pane ───────────────────────────────────────── */
.tariff-pane-header {
    padding: 16px 20px;
    border-bottom: 2px solid #A100FF;
    background: linear-gradient(135deg, #f0e6ff 0%, #fff 100%);
}
.tariff-pane-title { font-size: 0.85rem; font-weight: 700; color: #A100FF; text-transform: uppercase; letter-spacing: 1px; }
.tariff-pane-subtitle { font-size: 0.7rem; color: #888; margin-top: 2px; }

.sources-monitor {
    padding: 12px 16px;
    border-bottom: 1px solid #f0eaf8;
}
.sources-title { font-size: 0.65rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1.2px; color: #999; margin-bottom: 8px; }
.source-row {
    display: flex; align-items: center; gap: 8px;
    padding: 6px 0; font-size: 0.78rem;
}
.source-icon { font-size: 1rem; }
.source-name { flex: 1; color: #2d1a4a; }
.source-badge { font-size: 0.6rem; background: #e6fff9; color: #12B3A3; padding: 2px 6px; border-radius: 3px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }

.tariff-feed {
    flex: 1;
    overflow-y: auto;
    padding: 8px 0;
}
.feed-section-label { font-size: 0.65rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1.2px; color: #999; padding: 8px 16px 4px; }
.tariff-event {
    padding: 12px 16px;
    border-bottom: 1px solid #f5f3fa;
    cursor: pointer;
    transition: background 0.15s;
}
.tariff-event:hover { background: #fafaf8; }
.tariff-event-header {
    display: flex; justify-content: space-between; align-items: flex-start; gap: 8px;
    margin-bottom: 4px;
}
.tariff-time { font-size: 0.7rem; color: #999; white-space: nowrap; }
.tariff-status-badge {
    font-size: 0.6rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px;
    padding: 2px 6px; border-radius: 3px; white-space: nowrap;
}
.status-cleared { background: #d4edda; color: #155724; }
.status-issued { background: #f8d7da; color: #721c24; }
.status-pending { background: #fff3cd; color: #856404; }
.tariff-headline { font-size: 0.8rem; font-weight: 600; color: #2d1a4a; line-height: 1.3; }
.tariff-detail { font-size: 0.7rem; color: #888; margin-top: 3px; }
.tariff-source { font-size: 0.65rem; color: #aaa; margin-top: 4px; font-style: italic; }

.tariff-pane-footer {
    padding: 12px 16px;
    border-top: 1px solid #f0eaf8;
    font-size: 0.7rem;
    color: #aaa;
    text-align: center;
}
.aggregator-badge {
    display: inline-flex; align-items: center; gap: 4px;
    background: #e6f7ff; color: #0050b3; padding: 3px 8px; border-radius: 3px;
    font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;
}
.pulse-dot {
    display: inline-block;
    width: 6px; height: 6px; background: #12B3A3; border-radius: 50%;
    animation: pulse 2s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
}

/* ── Page header ─────────────────────────────────────────────── */
.page-header {
    background: linear-gradient(135deg, #1a0533 0%, #2d0a5a 100%);
    color: #fff;
    border-radius: 12px;
    padding: 28px 32px;
    margin-bottom: 28px;
    display: flex; align-items: center; gap: 20px;
}
.page-header .header-icon { font-size: 2.4rem; }
.page-header h1 { font-size: 1.6rem; font-weight: 700; }
.page-header .header-sub { font-size: 0.85rem; color: rgba(255,255,255,0.6); margin-top: 4px; }
.accent-bar { height: 4px; border-radius: 2px; margin-bottom: 2px; }

/* ── KPI grid (2 rows × 3 columns) ──────────────────────────── */
.kpi-strip {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    grid-template-rows: auto auto;
    gap: 16px;
    margin-bottom: 28px;
}
.kpi-card {
    background: #fff;
    border-radius: 10px;
    padding: 20px 20px 24px;
    box-shadow: 0 2px 8px rgba(26,5,51,0.07);
    border-top: 3px solid #A100FF;
    min-height: 110px;
}
.kpi-card.risk    { border-top-color: #F76C6C; }
.kpi-card.agility { border-top-color: #12B3A3; }
.kpi-label { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 1px; color: #888; font-weight: 600; }
.kpi-value { font-size: 1.75rem; font-weight: 800; color: #1a0533; margin-top: 6px; }
.kpi-unit  { font-size: 0.78rem; color: #666; margin-top: 2px; }

/* ── AI summary ──────────────────────────────────────────────── */
.ai-summary {
    background: linear-gradient(135deg, #f0e6ff 0%, #e8f7f6 100%);
    border: 1px solid rgba(161,0,255,0.2);
    border-left: 4px solid #A100FF;
    border-radius: 10px;
    padding: 20px 24px;
    margin-bottom: 28px;
    display: flex; gap: 14px; align-items: flex-start;
    min-height: 80px;
}
.ai-summary .ai-icon  { font-size: 1.5rem; flex-shrink: 0; }
.ai-summary .ai-label { font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: #A100FF; }
.ai-summary .ai-text  { font-size: 0.9rem; color: #2d1a4a; line-height: 1.6; margin-top: 4px; }
.ai-loading { color: #aaa; font-style: italic; font-size: 0.88rem; margin-top: 4px; }

/* ── Section card ────────────────────────────────────────────── */
.section-card {
    background: #fff;
    border-radius: 10px;
    box-shadow: 0 2px 8px rgba(26,5,51,0.07);
    overflow: hidden;
    margin-bottom: 28px;
}
.section-card-header {
    padding: 14px 20px;
    font-size: 0.8rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    border-bottom: 1px solid #f0eaf8;
    display: flex; align-items: center; gap: 8px;
}

/* ── Alerts feed ─────────────────────────────────────────────── */
.alert-row {
    display: flex; align-items: flex-start; gap: 12px;
    padding: 12px 20px;
    border-bottom: 1px solid #f5f3fa;
    font-size: 0.83rem;
}
.alert-row:last-child { border-bottom: none; }
.sev-dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; margin-top: 5px; }
.sev-high   { background: #F76C6C; }
.sev-medium { background: #F5A623; }
.sev-low    { background: #12B3A3; }
.alert-msg { flex: 1; color: #2d1a4a; }
.alert-ts  { font-size: 0.72rem; color: #aaa; white-space: nowrap; }

/* ── Agent grid ──────────────────────────────────────────────── */
.cluster-section { margin-bottom: 28px; }
.cluster-section h3 {
    font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1.5px; margin-bottom: 12px; padding-left: 4px;
}
.agent-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 16px; }
.agent-tile {
    background: #fff;
    border-radius: 10px;
    padding: 20px 16px;
    box-shadow: 0 2px 8px rgba(26,5,51,0.07);
    text-decoration: none;
    color: #1a0533;
    display: flex; flex-direction: column; gap: 8px;
    border-top: 3px solid;
    transition: transform 0.15s, box-shadow 0.15s;
    position: relative;
}
.agent-tile:hover { transform: translateY(-2px); box-shadow: 0 6px 18px rgba(26,5,51,0.12); }
.agent-tile.coming-soon { opacity: 0.55; pointer-events: none; }
.agent-tile .tile-icon { font-size: 1.6rem; }
.agent-tile .tile-name { font-size: 0.85rem; font-weight: 700; line-height: 1.3; }
.agent-tile .tile-tag  { font-size: 0.72rem; color: #888; }
.tile-status {
    position: absolute; top: 10px; right: 10px;
    font-size: 0.6rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.8px; padding: 2px 7px; border-radius: 4px;
}
.status-live { background: #e6fff9; color: #12B3A3; }
.status-soon { background: #f4f4f8; color: #aaa; }

/* ── Agent detail ────────────────────────────────────────────── */
.agent-detail-header {
    border-radius: 12px; padding: 28px 32px; margin-bottom: 28px;
    color: #fff; display: flex; align-items: center; gap: 20px;
}
.coming-soon-box {
    background: #fff; border-radius: 12px; padding: 60px 40px;
    text-align: center; box-shadow: 0 2px 8px rgba(26,5,51,0.07);
}
.coming-soon-box .cs-icon { font-size: 3rem; margin-bottom: 16px; }
.coming-soon-box h2 { font-size: 1.3rem; font-weight: 700; margin-bottom: 8px; }
.coming-soon-box p  { font-size: 0.88rem; color: #888; max-width: 400px; margin: 0 auto; }

/* ── Misc ─────────────────────────────────────────────────────── */
.back-link {
    display: inline-flex; align-items: center; gap: 6px;
    color: #A100FF; text-decoration: none; font-size: 0.83rem; font-weight: 600;
    margin-bottom: 18px;
}
.back-link:hover { text-decoration: underline; }
.footer {
    margin-top: 40px; padding-top: 16px;
    border-top: 1px solid #e8e0f0;
    font-size: 0.72rem; color: #bbb; text-align: right;
}
"""

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _sidebar_html(active_id: str = "") -> str:
    cluster_blocks = ""
    for cluster in CLUSTERS:
        items = [a for a in AGENTS if a["cluster"] == cluster["name"]]
        rows = ""
        for a in items:
            cs_cls   = "coming-soon" if a["status"] == "coming_soon" else ""
            soon_tag = '<span class="soon-tag">Soon</span>' if a["status"] == "coming_soon" else ""
            active_cls = "active" if a["id"] == active_id else ""
            rows += (
                f'<a href="/agent/{a["id"]}" class="nav-agent {cs_cls} {active_cls}">'
                f'{a["icon"]} {a["display_name"]}{soon_tag}</a>'
            )
        cluster_blocks += (
            f'<div class="cluster-label" style="color:{cluster["color"]}">'
            f'{cluster["name"]}</div>{rows}'
        )

    home_active = "active" if not active_id else ""
    return f"""
    <nav class="sidebar">
      <div class="sidebar-brand">
        <div class="logo-text">🌐 TradeNavigator AI</div>
        <div class="logo-sub">Powered by Accenture</div>
      </div>
      <div class="sidebar-nav">
        <a href="/" class="nav-home {home_active}">🗼 Control Tower</a>
        {cluster_blocks}
      </div>
    </nav>
    """

# ---------------------------------------------------------------------------
# Base template
# ---------------------------------------------------------------------------

BASE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{ title }} — TradeNavigator AI</title>
<style>{{ css }}</style>
</head>
<body>
{{ sidebar | safe }}
<main class="main">
  <div class="main-content">
    {{ content | safe }}
    <div class="footer">TradeNavigator AI &mdash; Accenture &copy; 2025</div>
  </div>
  <div class="main-right-pane" id="tariff-pane">
    <div class="tariff-pane-header">
      <div class="tariff-pane-title">📊 Tariff Intelligence</div>
      <div class="tariff-pane-subtitle">Live aggregation by AI agent</div>
    </div>
    
    <div class="sources-monitor" id="sources-container">
      <div class="sources-title">Source Monitoring</div>
    </div>
    
    <div class="tariff-feed" id="tariff-feed-container">
      <div class="feed-section-label">Recent Feed</div>
    </div>
    
    <div class="tariff-pane-footer">
      <div class="aggregator-badge">
        <span class="pulse-dot"></span>
        Aggregator Active
      </div>
    </div>
  </div>
</main>
{{ scripts | safe }}
</body>
</html>"""

# ---------------------------------------------------------------------------
# Control Tower (home) — renders instantly; AI summary fetched by JS
# ---------------------------------------------------------------------------

@app.route("/")
def control_tower():
    kpis   = get_kpis()
    alerts = get_alerts()

    kpi_html = f"""
    <div class="kpi-strip">
      <div class="kpi-card">
        <div class="kpi-label">Total Duty Paid</div>
        <div class="kpi-value">${kpis['total_duty_paid_m']}M</div>
        <div class="kpi-unit">This quarter</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">FTA Capture Rate</div>
        <div class="kpi-value">{kpis['fta_capture_rate_pct']}%</div>
        <div class="kpi-unit">Of eligible shipments</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Drawback Recovered</div>
        <div class="kpi-value">${kpis['drawback_recovered_k']}K</div>
        <div class="kpi-unit">YTD</div>
      </div>
      <div class="kpi-card risk">
        <div class="kpi-label">Compliance Flags</div>
        <div class="kpi-value">{kpis['open_compliance_flags']}</div>
        <div class="kpi-unit">Open items</div>
      </div>
      <div class="kpi-card risk">
        <div class="kpi-label">Tariff Alerts</div>
        <div class="kpi-value">{kpis['active_tariff_alerts']}</div>
        <div class="kpi-unit">Active</div>
      </div>
      <div class="kpi-card agility">
        <div class="kpi-label">Value at Risk</div>
        <div class="kpi-value">${kpis['value_at_risk_m']}M</div>
        <div class="kpi-unit">Estimated exposure</div>
      </div>
    </div>
    """

    ai_html = """
    <div class="ai-summary">
      <div class="ai-icon">🤖</div>
      <div>
        <div class="ai-label">AI Trade Posture Summary</div>
        <div class="ai-text" id="ai-posture-text">
          <span class="ai-loading">Generating trade posture analysis…</span>
        </div>
      </div>
    </div>
    """

    alert_rows = ""
    for al in alerts:
        sev = al["severity"]
        alert_rows += f"""
        <div class="alert-row">
          <div class="sev-dot sev-{sev}"></div>
          <div class="alert-msg">{al['message']}</div>
          <div class="alert-ts">{al['timestamp']}</div>
        </div>"""

    alerts_html = f"""
    <div class="section-card">
      <div class="section-card-header">🔔 Live Alert Feed</div>
      {alert_rows}
    </div>
    """

    cluster_tiles = ""
    for cluster in CLUSTERS:
        agents = [a for a in AGENTS if a["cluster"] == cluster["name"]]
        tiles = ""
        for a in agents:
            is_soon = a["status"] == "coming_soon"
            cs_cls  = "coming-soon" if is_soon else ""
            st_cls  = "status-soon" if is_soon else "status-live"
            st_lbl  = "Soon" if is_soon else "Live"
            href    = f'/agent/{a["id"]}'
            tiles += f"""
            <a href="{href}" class="agent-tile {cs_cls}" style="border-top-color:{cluster['color']}">
              <span class="tile-status {st_cls}">{st_lbl}</span>
              <div class="tile-icon">{a['icon']}</div>
              <div class="tile-name">{a['display_name']}</div>
              <div class="tile-tag">{a['tagline'][:55]}…</div>
            </a>"""

        cluster_tiles += f"""
        <div class="cluster-section">
          <h3 style="color:{cluster['color']}">{cluster['name']}</h3>
          <div class="agent-grid">{tiles}</div>
        </div>"""

    content = f"""
    <div class="page-header">
      <div class="header-icon">🗼</div>
      <div>
        <div class="accent-bar" style="background:#A100FF; width:60px;"></div>
        <h1>Trade Control Tower</h1>
        <div class="header-sub">Consolidated global trade intelligence — all agents, one view</div>
      </div>
    </div>
    {kpi_html}
    {ai_html}
    {alerts_html}
    {cluster_tiles}
    """

    # Fetch AI summary after page renders so the page is never blocked
    scripts = """
    <script>
    (function() {
      // Fetch posture summary
      fetch('/api/posture-summary')
        .then(r => r.json())
        .then(data => {
          var el = document.getElementById('ai-posture-text');
          if (el) el.innerHTML = data.summary.replace(/\\n/g, '<br>');
        })
        .catch(function() {
          var el = document.getElementById('ai-posture-text');
          if (el) el.innerHTML = 'AI summary unavailable — check API connectivity.';
        });

      // Fetch tariff feed on page load
      function loadTariffFeed() {
        fetch('/api/tariff-feed')
          .then(r => r.json())
          .then(data => {
            // Render sources
            var sourcesContainer = document.getElementById('sources-container');
            if (sourcesContainer && data.sources.length > 0) {
              var sourcesHtml = '<div class="sources-title">Source Monitoring</div>';
              data.sources.forEach(source => {
                sourcesHtml += `
                  <div class="source-row">
                    <span class="source-icon">${source.icon}</span>
                    <span class="source-name">${source.name}</span>
                    <span class="source-badge">${source.status}</span>
                  </div>`;
              });
              sourcesContainer.innerHTML = sourcesHtml;
            }

            // Render tariff feed
            var feedContainer = document.getElementById('tariff-feed-container');
            if (feedContainer && data.feed.length > 0) {
              var feedHtml = '<div class="feed-section-label">Recent Feed</div>';
              data.feed.forEach(event => {
                var statusClass = 'status-' + event.status;
                feedHtml += `
                  <div class="tariff-event">
                    <div class="tariff-event-header">
                      <div class="tariff-headline">${event.headline}</div>
                      <span class="tariff-status-badge ${statusClass}">${event.status}</span>
                    </div>
                    <div class="tariff-time">${event.time_short}</div>
                    <div class="tariff-detail">${event.detail}</div>
                    <div class="tariff-source">via ${event.source}</div>
                  </div>`;
              });
              feedContainer.innerHTML = feedHtml;
            }
          })
          .catch(function(e) {
            console.error('Tariff feed error:', e);
          });
      }

      loadTariffFeed();
      // Refresh tariff feed every 10 seconds for live updates
      setInterval(loadTariffFeed, 10000);
    })();
    </script>
    """

    return render_template_string(
        BASE,
        title="Control Tower",
        css=_CSS,
        sidebar=_sidebar_html(""),
        content=content,
        scripts=scripts,
    )


# ---------------------------------------------------------------------------
# FTA & Preferential Trade — API endpoint + dedicated page
# ---------------------------------------------------------------------------

@app.route("/api/fta/explain", methods=["POST"])
def api_fta_explain():
    data         = request.get_json(force=True) or {}
    # JS passes data-attributes set from SAP field names; ro_status is already
    # the human-readable label (Qualified / Near-Miss / Fail) sent by the row onclick.
    shipment_id  = data.get("shipment_id", "")   # TOR_ID
    product      = data.get("product", "")        # PRODUCT_TEXT
    hs_code      = data.get("hs_code", "")        # CCNGN
    origin       = data.get("origin", "")         # CTYDP display name
    destination  = data.get("destination", "")    # CTYAR display name
    fta_name     = data.get("fta_name", "")       # AGREEMENT
    est_saving_k = data.get("est_saving_k", 0)
    ro_status         = data.get("ro_status", "")          # human-readable label
    rvc_pct           = data.get("rvc_pct", 0)             # RVC_PCT
    rvc_threshold_pct = data.get("rvc_threshold_pct", 0)   # RVC_THRESHOLD

    system_prompt = (
        "You are TradeNavigator AI, an expert FTA compliance advisor. "
        "Output ONLY a plain-English explanation, 3-4 sentences, no preamble, "
        "no reasoning steps. Begin immediately with the first word."
    )
    # Phrase the RoO sentence precisely to match the computed ROO_STATUS.
    if ro_status == "Near-Miss":
        roo_sentence = (
            f"Its actual RVC_PCT is {rvc_pct}%, which falls just {rvc_threshold_pct - rvc_pct} "
            f"percentage point(s) short of the {rvc_threshold_pct}% RVC_THRESHOLD required "
            f"under {fta_name} — a near-miss that could be resolved with targeted "
            f"sourcing adjustments."
        )
    elif ro_status == "Qualified":
        roo_sentence = (
            f"Its RVC_PCT of {rvc_pct}% comfortably clears the {rvc_threshold_pct}% "
            f"RVC_THRESHOLD required under {fta_name}."
        )
    else:
        roo_sentence = (
            f"Its RVC_PCT of {rvc_pct}% does not meet the {rvc_threshold_pct}% "
            f"RVC_THRESHOLD required under {fta_name}."
        )

    user_prompt = (
        f"Explain why shipment {shipment_id} ({product}, HS {hs_code}) from "
        f"{origin} to {destination} is FTA-eligible under {fta_name}. "
        f"Rules-of-origin: {roo_sentence} "
        f"Estimated duty saving: ${est_saving_k}K. "
        f"State the recommended immediate action. Tone: actionable, CFO-ready."
    )

    try:
        raw  = _run_ai_in_thread(system_prompt, user_prompt)
        text = _strip_reasoning(raw)
        if not text:
            raise ValueError("empty response")
    except Exception:
        gap = rvc_threshold_pct - rvc_pct
        if ro_status == "Near-Miss":
            text = (
                f"TOR_ID {shipment_id} ({product}, CCNGN {hs_code}) has RVC_PCT {rvc_pct}%, "
                f"falling {gap} pts short of the {rvc_threshold_pct}% RVC_THRESHOLD for {fta_name}. "
                f"A targeted sourcing adjustment or supplier invoice restructure could close the gap. "
                f"The ${est_saving_k}K duty saving is at risk — request a revised proof-of-origin "
                f"from the supplier (SUPPLIER_NAME) within 5 business days."
            )
        else:
            text = (
                f"TOR_ID {shipment_id} ({product}, CCNGN {hs_code}) qualifies for {fta_name} "
                f"preferential treatment: RVC_PCT {rvc_pct}% clears the {rvc_threshold_pct}% "
                f"RVC_THRESHOLD. Claiming PREF_STATUS=E recovers an estimated ${est_saving_k}K in duty. "
                f"Immediate action: submit the {fta_name} Certificate of Origin to customs "
                f"within the current entry window."
            )

    return jsonify({"explanation": text})


# ---------------------------------------------------------------------------
# FTA data upload / reset / template routes
# ---------------------------------------------------------------------------

@app.route("/api/fta/upload/shipments", methods=["POST"])
def fta_upload_shipments():
    f = request.files.get("shipment_file")
    if f and f.filename:
        fta_data_source.upload_shipment_data(f.read(), f.filename)
    return redirect(url_for("agent_fta_preferential"))


@app.route("/api/fta/upload/coo", methods=["POST"])
def fta_upload_coo():
    f = request.files.get("coo_file")
    if f and f.filename:
        fta_data_source.upload_coo_data(f.read(), f.filename)
    return redirect(url_for("agent_fta_preferential"))


@app.route("/api/fta/reset", methods=["POST"])
def fta_reset():
    fta_data_source.reset_to_simulated()
    return redirect(url_for("agent_fta_preferential"))


@app.route("/api/fta/template/<which>")
def fta_template(which):
    from flask import Response
    if which == "shipments":
        return Response(
            SHIPMENT_TEMPLATE_CSV_WITH_DICT,
            mimetype="text/csv",
            headers={"Content-Disposition":
                     "attachment; filename=fta_shipments_sap_template.csv"},
        )
    if which == "coo":
        return Response(
            COO_TEMPLATE_CSV,
            mimetype="text/csv",
            headers={"Content-Disposition":
                     "attachment; filename=fta_coo_template.csv"},
        )
    return ("Not found", 404)


@app.route("/agent/fta_preferential")
def agent_fta_preferential():
    kpis         = fta_data_source.get_fta_kpis()
    lanes        = fta_data_source.get_fta_lanes()
    shipments    = fta_data_source.get_fta_shipments()
    coo_requests = fta_data_source.get_coo_requests()
    roo_items    = fta_data_source.get_roo_assessments()
    roadmap      = fta_data_source.get_qualification_roadmap()
    source       = fta_data_source.get_source_info()
    s_msg, c_msg = fta_data_source.take_upload_messages()

    # Period label comes from the data source (dynamic for uploaded data)
    period_label = kpis.get("period_label", "")

    # ── Header ──────────────────────────────────────────────────────────
    _is_sim = (source["shipment_mode"] == "simulated"
               and source["coo_mode"]  == "simulated")
    _subtitle = (
        'Closing the ~23% FTA utilization gap — illustrative benchmarks requiring '
        'client-specific quantification'
        if _is_sim else
        'Showing uploaded data — figures derived from your shipment and CoO files'
    )
    header_html = (
        '<a href="/" class="back-link">← Control Tower</a>'
        '<div class="agent-detail-header" '
        'style="background: linear-gradient(135deg, #1a0533 0%, #A100FF88 100%);">'
        '<div style="font-size:2.4rem">\U0001f91d</div>'
        '<div>'
        '<div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:1.5px;'
        'color:#A100FF;font-weight:700;">Value Capture</div>'
        '<h1 style="font-size:1.5rem;font-weight:700;">'
        'FTA &amp; Preferential Trade Agent</h1>'
        f'<div style="font-size:0.85rem;color:rgba(255,255,255,0.65);margin-top:4px;">'
        f'{_subtitle}</div>'
        '</div></div>'
    )

    # ── Upload / data-source section ─────────────────────────────────────
    # Build source badge
    if _is_sim:
        _src_badge = (
            '<span style="padding:2px 10px;border-radius:4px;font-size:0.68rem;'
            'font-weight:700;background:#f0eaf8;color:#999;letter-spacing:1px">'
            'SIMULATED DATA</span>'
        )
    else:
        _parts = []
        if source["shipment_mode"] == "uploaded":
            _parts.append(f'Shipments: {source["shipment_filename"]}')
        if source["coo_mode"] == "uploaded":
            _parts.append(f'CoO: {source["coo_filename"]}')
        _src_badge = (
            '<span style="padding:2px 10px;border-radius:4px;font-size:0.68rem;'
            'font-weight:700;background:#A100FF;color:#fff;letter-spacing:1px">'
            f'UPLOADED · {" | ".join(_parts)}</span>'
        )

    # Build upload feedback banners
    _feedback = ""
    for _msg, _label in [(s_msg, "Shipment"), (c_msg, "CoO")]:
        if not _msg:
            continue
        if _msg["ok"]:
            _warn_str = (" — " + "; ".join(_msg["warnings"])) if _msg["warnings"] else ""
            _feedback += (
                f'<div style="margin:6px 0;padding:8px 12px;background:#e6fff9;'
                f'border-left:3px solid #12B3A3;border-radius:0 6px 6px 0;'
                f'font-size:0.78rem;color:#0a7060;font-weight:600">'
                f'✓ {_label} data loaded{_warn_str}</div>'
            )
        else:
            _err_str = "; ".join(_msg["errors"])
            _feedback += (
                f'<div style="margin:6px 0;padding:8px 12px;background:#fde8e8;'
                f'border-left:3px solid #c0392b;border-radius:0 6px 6px 0;'
                f'font-size:0.78rem;color:#c0392b;font-weight:600">'
                f'✗ {_label} upload failed: {_err_str}</div>'
            )

    # Auto-open the panel when there's a message or when already in uploaded mode
    _panel_open = "open" if (_feedback or not _is_sim) else ""

    # ── SAP source badge helper ──────────────────────────────────────────────
    def _sap_badge(label: str) -> str:
        return (
            f'<span style="font-size:0.6rem;font-weight:600;color:#888;'
            f'background:#f0f0f4;padding:1px 7px;border-radius:3px;'
            f'letter-spacing:0.5px;margin-left:auto">{label}</span>'
        )

    # Country + code translation helpers (SAP codes → human-readable)
    def _ctry(iso2: str) -> str:
        return _CTRY.get(iso2, iso2)

    def _dats_display(dats: str) -> str:
        """SAP DATS YYYYMMDD → YYYY-MM-DD for display."""
        s = str(dats)
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        return s

    # ── Provenance helpers (pull from FIELD_DICTIONARY — single source of truth) ─
    _fd = {row[0]: row for row in _FIELD_DICTIONARY}

    def _field_json(sap_keys: list) -> str:
        """Serialize FIELD_DICTIONARY entries for a dot button's data-fields attribute."""
        rows = []
        for k in sap_keys:
            if k not in _fd:
                continue
            e = _fd[k]
            rows.append({
                "sap":  e[0],
                "univ": e[1],
                "prov": "green" if e[2] == "🟢" else "amber",
                "fmt":  e[3],
                "src":  e[5] if len(e) > 5 else "SAP GTS",
                "long": e[6] if len(e) > 6 else e[4],
            })
        # HTML-encode JSON so it's safe inside a double-quoted attribute
        return json.dumps(rows, ensure_ascii=False).replace('"', '&quot;')

    def _prov_btn(display: str, sap_keys: list, size: int = 8) -> str:
        """Colored circle <button> that opens the persistent SAP provenance panel."""
        entries = [_fd[k] for k in sap_keys if k in _fd]
        if not entries:
            return ""
        all_green = all(e[2] == "🟢" for e in entries)
        dot_col   = "#12B3A3" if all_green else "#F5A623"
        prov_code = "green" if all_green else "amber"
        data_json = _field_json(sap_keys)
        return (
            f'<button class="prov-dot-btn" onclick="openSapProv(this)" '
            f'data-col-label="{display}" data-prov-code="{prov_code}" '
            f'data-fields="{data_json}" '
            f'style="width:{size}px;height:{size}px;border-radius:50%;'
            f'background:{dot_col};flex-shrink:0;cursor:pointer;border:none;'
            f'padding:0;display:inline-block;vertical-align:middle" '
            f'title="Click for SAP field details" '
            f'aria-label="SAP provenance for {display}"></button>'
        )

    def _kpi_lbl(display: str, sap_keys: list) -> str:
        """KPI card label div with clickable provenance dot."""
        btn = _prov_btn(display, sap_keys, size=9)
        if not btn:
            return f'<div class="kpi-label">{display}</div>'
        return (
            f'<div class="kpi-label" style="display:flex;align-items:center;gap:5px">'
            f'{display}{btn}</div>'
        )

    prov_legend = (
        '<div style="display:flex;align-items:center;flex-wrap:wrap;gap:14px;'
        'margin-bottom:10px;padding:7px 14px;background:#f8f6fc;border-radius:8px;'
        'width:fit-content;border:1px solid #ede8f8">'
        '<span style="font-size:0.63rem;font-weight:700;color:#aaa;'
        'text-transform:uppercase;letter-spacing:1.2px">Field provenance</span>'
        '<span style="display:flex;align-items:center;gap:5px;font-size:0.72rem;color:#555">'
        '<span style="width:9px;height:9px;border-radius:50%;background:#12B3A3;'
        'display:inline-block;flex-shrink:0"></span>'
        'Verified SAP field</span>'
        '<span style="display:flex;align-items:center;gap:5px;font-size:0.72rem;color:#555">'
        '<span style="width:9px;height:9px;border-radius:50%;background:#F5A623;'
        'display:inline-block;flex-shrink:0"></span>'
        'Pending SAP confirmation</span>'
        '<span style="font-size:0.63rem;color:#bbb;font-style:italic">'
        'Click any dot for full SAP field details</span>'
        '</div>'
    )

    # Column list hint strings
    _shp_req_str  = ", ".join(fta_data_source.SHIPMENT_REQUIRED)
    _shp_opt_str  = ", ".join(fta_data_source.SHIPMENT_ROO + fta_data_source.SHIPMENT_ROADMAP)
    _coo_cols_str = ", ".join(fta_data_source.COO_REQUIRED)

    upload_section = (
        f'<details {_panel_open} style="margin-bottom:16px;border-radius:10px;'
        'box-shadow:0 2px 8px rgba(26,5,51,0.07);background:#fff;overflow:hidden">'
        '<summary style="padding:12px 20px;font-size:0.8rem;font-weight:700;'
        'text-transform:uppercase;letter-spacing:1px;'
        'display:flex;align-items:center;gap:10px;cursor:pointer;list-style:none;'
        'user-select:none;border-bottom:1px solid #f0eaf8">'
        '\U0001f4c2 Data Source'
        f'<span style="margin-left:8px">{_src_badge}</span>'
        '<span style="margin-left:auto;font-size:0.72rem;font-weight:400;'
        'color:#A100FF;text-transform:none;letter-spacing:0">▼ upload real data</span>'
        '</summary>'
        '<div style="padding:16px 20px">'
        + (_feedback if _feedback else "")
        + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:8px">'
        # ── Shipment upload ──
        '<div style="border:1px solid #ede8f8;border-radius:8px;padding:14px">'
        '<div style="font-size:0.82rem;font-weight:700;margin-bottom:4px">'
        '\U0001f4e6 Shipment Data'
        '</div>'
        '<div style="font-size:0.72rem;color:#888;margin-bottom:10px">'
        'Powers: lanes, eligibility feed, KPIs, RoO assessment, qualification roadmap'
        '</div>'
        f'<details style="margin-bottom:10px"><summary style="font-size:0.7rem;'
        f'color:#A100FF;cursor:pointer">Expected columns</summary>'
        f'<div style="font-size:0.68rem;color:#666;margin-top:6px;line-height:1.5">'
        f'<strong>Required:</strong> {_shp_req_str}<br>'
        f'<strong>Optional (RoO + Roadmap):</strong> {_shp_opt_str}</div></details>'
        '<form action="/api/fta/upload/shipments" method="post" '
        'enctype="multipart/form-data" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
        '<input type="file" name="shipment_file" accept=".csv,.xlsx" '
        'style="font-size:0.78rem;flex:1;min-width:0">'
        '<button type="submit" style="padding:6px 14px;background:#A100FF;color:#fff;'
        'border:none;border-radius:6px;font-size:0.78rem;font-weight:600;cursor:pointer">'
        'Upload</button>'
        '<a href="/api/fta/template/shipments" '
        'style="font-size:0.75rem;color:#A100FF;text-decoration:none;white-space:nowrap">'
        '\U0001f4e5 Template</a>'
        '</form>'
        '</div>'
        # ── CoO upload ──
        '<div style="border:1px solid #ede8f8;border-radius:8px;padding:14px">'
        '<div style="font-size:0.82rem;font-weight:700;margin-bottom:4px">'
        '\U0001f4cb CoO Requests <span style="font-size:0.7rem;font-weight:400;color:#999">(optional)</span>'
        '</div>'
        '<div style="font-size:0.72rem;color:#888;margin-bottom:10px">'
        'Powers: CoO Supplier Tracker — independent of shipment data'
        '</div>'
        f'<details style="margin-bottom:10px"><summary style="font-size:0.7rem;'
        f'color:#A100FF;cursor:pointer">Expected columns</summary>'
        f'<div style="font-size:0.68rem;color:#666;margin-top:6px;line-height:1.5">'
        f'<strong>Required:</strong> {_coo_cols_str}</div></details>'
        '<form action="/api/fta/upload/coo" method="post" '
        'enctype="multipart/form-data" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
        '<input type="file" name="coo_file" accept=".csv,.xlsx" '
        'style="font-size:0.78rem;flex:1;min-width:0">'
        '<button type="submit" style="padding:6px 14px;background:#A100FF;color:#fff;'
        'border:none;border-radius:6px;font-size:0.78rem;font-weight:600;cursor:pointer">'
        'Upload</button>'
        '<a href="/api/fta/template/coo" '
        'style="font-size:0.75rem;color:#A100FF;text-decoration:none;white-space:nowrap">'
        '\U0001f4e5 Template</a>'
        '</form>'
        '</div>'
        '</div>'
        + (
            '<div style="margin-top:12px;padding-top:10px;border-top:1px solid #f0eaf8;'
            'display:flex;align-items:center;gap:12px">'
            '<form action="/api/fta/reset" method="post" style="display:inline">'
            '<button type="submit" style="padding:5px 14px;background:#fff;color:#888;'
            'border:1px solid #ddd;border-radius:6px;font-size:0.75rem;cursor:pointer">'
            '↩ Reset to simulated data</button>'
            '</form>'
            '<span style="font-size:0.68rem;color:#bbb">'
            'Clears all uploads and restores illustrative benchmark data</span>'
            '</div>'
            if not _is_sim else ""
        )
        + '</div></details>'
    )

    # ── KPI strip (4 cards) ──────────────────────────────────────────────
    kpi_html = (
        f'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">'
        f'{prov_legend}'
        f'{_sap_badge("SAP TM + GTS")}</div>'
        '<div style="display:grid;grid-template-columns:repeat(4,1fr);'
        'gap:16px;margin-bottom:28px">'
        f'<div class="kpi-card" style="border-top:3px solid #A100FF">'
        + _kpi_lbl("Utilization Rate", ["PREF_STATUS", "CUSVAL"]) +
        f'<div class="kpi-value">{kpis["utilization_pct"]}%</div>'
        f'<div class="kpi-unit">{period_label}</div></div>'
        f'<div class="kpi-card" style="border-top:3px solid #F76C6C">'
        + _kpi_lbl("Unclaimed Opportunity", ["CUSVAL", "MFN_RATE", "PREF_RATE"]) +
        f'<div class="kpi-value">${kpis["unclaimed_opportunity_m"]}M</div>'
        f'<div class="kpi-unit">Duty savings · {period_label}</div></div>'
        f'<div class="kpi-card" style="border-top:3px solid #A100FF">'
        + _kpi_lbl("Retroactive Claims", ["ENTRY_DATE", "CUSVAL", "MFN_RATE", "PREF_RATE"]) +
        f'<div class="kpi-value">${kpis["retroactive_claims_k"]}K</div>'
        f'<div class="kpi-unit">{kpis["retro_window_label"]} · {period_label}</div></div>'
        f'<div class="kpi-card" style="border-top:3px solid #F5A623">'
        + _kpi_lbl("CoOs Outstanding", ["POO_STATUS"]) +
        f'<div class="kpi-value">{kpis["coo_outstanding"]}</div>'
        f'<div class="kpi-unit">POO_STATUS: Pending + Overdue + Received</div></div>'
        '</div>'
    )

    # ── Shared table-header cell style ───────────────────────────────────
    th = (
        'style="padding:10px 12px;text-align:left;font-size:0.72rem;'
        'text-transform:uppercase;letter-spacing:1px;color:#888;font-weight:700;'
        'background:#faf8fe;position:sticky;top:0"'
    )

    def _pth(display: str, sap_keys: list) -> str:
        """Returns a <th> element with clickable provenance dot button."""
        btn = _prov_btn(display, sap_keys, size=8)
        if not btn:
            return f'<th {th}>{display}</th>'
        return (
            f'<th {th}><div style="display:flex;align-items:center;gap:4px">'
            f'{display}{btn}'
            f'</div></th>'
        )

    # ── Lane utilization table ───────────────────────────────────────────
    lane_rows = ""
    for lane in lanes:
        util       = lane["UTILIZATION_PCT"]
        uncl_k     = lane["UNCLAIMED_SAVINGS_K"]
        sc         = "#c0392b" if uncl_k > 200 else "#1a0533"
        ctydp_name = _ctry(lane["CTYDP"])
        ctyar_name = _ctry(lane["CTYAR"])
        lane_rows += (
            '<tr style="border-bottom:1px solid #f5f3fa">'
            f'<td style="padding:10px 12px;font-size:0.82rem">'
            f'{ctydp_name} → {ctyar_name}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600">'
            f'{lane["AGREEMENT"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">'
            f'${lane["ELIGIBLE_VALUE_M"]}M</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">'
            f'${lane["CLAIMED_VALUE_M"]}M</td>'
            f'<td style="padding:10px 12px;font-size:0.78rem;white-space:nowrap;color:#555">'
            f'<span style="color:#c0392b;font-weight:600">{lane["MFN_RATE"]}%</span>'
            f' MFN → '
            f'<span style="color:#12B3A3;font-weight:600">{lane["PREF_RATE"]}%</span>'
            f' pref</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;white-space:nowrap">'
            f'<div style="background:#e8e0f0;border-radius:3px;height:6px;'
            f'width:80px;display:inline-block">'
            f'<div style="background:#A100FF;height:6px;border-radius:3px;'
            f'width:{util}%"></div></div>'
            f'<span style="margin-left:6px;font-size:0.75rem">{util}%</span></td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:{sc}">'
            f'${uncl_k}K</td>'
            '</tr>'
        )

    lane_section = (
        '<div class="section-card">'
        f'<div class="section-card-header">\U0001f310 FTA Lane Utilization Gap'
        f'<span style="font-size:0.72rem;font-weight:400;color:#888;'
        f'margin-left:10px;letter-spacing:0">({period_label})</span>'
        f'{_sap_badge("SAP TM + GTS")}</div>'
        '<div style="overflow-x:auto;max-height:300px;overflow-y:auto">'
        '<table style="width:100%;border-collapse:collapse">'
        '<thead><tr>'
        + _pth("Trade Lane", ["CTYDP", "CTYAR"])
        + _pth("FTA Agreement", ["AGREEMENT"])
        + _pth("Eligible Value", ["CUSVAL", "PREF_STATUS"])
        + _pth("Claimed", ["CUSVAL", "PREF_STATUS"])
        + _pth("Rate Differential", ["MFN_RATE", "PREF_RATE"])
        + _pth("Utilization", ["PREF_STATUS", "CUSVAL"])
        + _pth("Unclaimed Savings", ["CUSVAL", "MFN_RATE", "PREF_RATE"]) +
        '</tr></thead>'
        f'<tbody>{lane_rows}</tbody>'
        '</table></div></div>'
    )

    # ── Shipment eligibility feed (PREF_STATUS=U only) ───────────────────
    roo_badge_styles = {
        "Q": "background:#e6fff9;color:#12B3A3",
        "M": "background:#fff3cd;color:#856404",
        "F": "background:#fde8e8;color:#c0392b",
    }
    unclaimed     = [s for s in shipments if s["PREF_STATUS"] == "U"]
    shipment_rows = ""
    for s in unclaimed:
        roo_code  = s["ROO_STATUS"]
        roo_label = ROO_STATUS_LABELS.get(roo_code, roo_code)
        row_bg    = "background:#fff8e6;" if roo_code == "M" else ""
        ro_badge  = roo_badge_styles.get(roo_code, "")
        tor_id    = s["TOR_ID"]
        saving_k  = round(s["duty_saving"] / 1000, 1)
        origin_n  = _ctry(s["CTYDP"])
        dest_n    = _ctry(s["CTYAR"])
        entry_d   = _dats_display(s["ENTRY_DATE"])
        shipment_rows += (
            f'<tr style="{row_bg}cursor:pointer;border-bottom:1px solid #f5f3fa" '
            f"onclick=\"fetchFTAExplain(this, '{tor_id}')\" "
            f'data-shipment-id="{tor_id}" '
            f'data-product="{s["PRODUCT_TEXT"]}" '
            f'data-hs-code="{s["CCNGN"]}" '
            f'data-origin="{origin_n}" '
            f'data-destination="{dest_n}" '
            f'data-fta-name="{s["AGREEMENT"]}" '
            f'data-value-k="{round(s["CUSVAL"]/1000,1)}" '
            f'data-eligibility="eligible-unclaimed" '
            f'data-est-saving-k="{saving_k}" '
            f'data-ro-status="{roo_label}" '
            f'data-rvc-pct="{s["RVC_PCT"]}" '
            f'data-rvc-threshold="{s["RVC_THRESHOLD"]}">'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600">{tor_id}</td>'
            f'<td style="padding:10px 12px;font-size:0.78rem;color:#666;'
            f'font-family:monospace">{entry_d}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">{s["PRODUCT_TEXT"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-family:monospace">'
            f'{s["CCNGN"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">'
            f'{origin_n} → {dest_n}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;'
            f'color:#A100FF">${saving_k}K</td>'
            f'<td style="padding:10px 12px">'
            f'<span style="padding:2px 8px;border-radius:4px;font-size:0.72rem;'
            f'font-weight:600;{ro_badge}">{roo_label}</span>'
            f'<div style="font-size:0.65rem;color:#999;margin-top:3px">'
            f'RVC_PCT {s["RVC_PCT"]}% / {s["RVC_THRESHOLD"]}% req\'d</div></td>'
            f'<td style="padding:10px 12px;font-size:0.78rem;color:#A100FF;'
            f'font-weight:600">▶ Explain</td>'
            '</tr>'
        )

    shipment_section = (
        '<div class="section-card">'
        '<div class="section-card-header">'
        f'\U0001f4e6 Shipment Eligibility Feed — Eligible / Unclaimed (PREF_STATUS=U)'
        f'<span style="font-size:0.72rem;font-weight:400;color:#888;'
        f'margin-left:10px;letter-spacing:0">({period_label})</span>'
        f'{_sap_badge("SAP TM + GTS")}</div>'
        '<div style="overflow-x:auto">'
        '<table style="width:100%;border-collapse:collapse">'
        '<thead><tr>'
        + _pth("Freight Order", ["TOR_ID"])
        + _pth("Entry Date", ["ENTRY_DATE"])
        + _pth("Product", ["PRODUCT_TEXT"])
        + _pth("HS Code", ["CCNGN"])
        + _pth("Lane", ["CTYDP", "CTYAR"])
        + _pth("Est. Saving", ["CUSVAL", "MFN_RATE", "PREF_RATE"])
        + _pth("RoO Status", ["ROO_STATUS"])
        + _pth("Action", []) +
        '</tr></thead>'
        f'<tbody>{shipment_rows}</tbody>'
        '</table></div>'
        '<div id="fta-ai-box" class="ai-summary" '
        'style="display:none;margin:16px 20px"></div>'
        '</div>'
    )

    # ── CoO / Proof-of-Origin Tracker ───────────────────────────────────
    coo_badge_styles = {
        "OVERDUE":   "background:#fde8e8;color:#c0392b",
        "PENDING":   "background:#fff8e6;color:#856404",
        "VALIDATED": "background:#e6fff9;color:#12B3A3",
        "RECEIVED":  "background:#e6f0ff;color:#0050b3",
    }
    coo_rows = ""
    for req in coo_requests:
        poo_s      = req["POO_STATUS"]
        poo_label  = POO_STATUS_LABELS.get(poo_s, poo_s.title())
        badge      = coo_badge_styles.get(poo_s, "")
        lane_disp  = f'{_ctry(req["CTYDP"])} → {_ctry(req["CTYAR"])}'
        deadline_d = _dats_display(req["VDECL_DEADLINE"])
        coo_rows += (
            '<tr style="border-bottom:1px solid #f5f3fa">'
            f'<td style="padding:10px 12px;font-size:0.82rem">{req["SUPPLIER_NAME"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">{lane_disp}</td>'
            f'<td style="padding:10px 12px;font-size:0.75rem;color:#555">'
            f'{req["POO_TYPE"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-family:monospace">'
            f'{deadline_d}</td>'
            f'<td style="padding:10px 12px">'
            f'<span style="padding:2px 8px;border-radius:4px;font-size:0.72rem;'
            f'font-weight:600;{badge}">{poo_label}</span></td>'
            '</tr>'
        )

    coo_section = (
        '<div class="section-card">'
        '<div class="section-card-header">\U0001f4cb CoO / Proof-of-Origin Tracker'
        '<span style="font-size:0.72rem;font-weight:400;color:#888;'
        'margin-left:10px;letter-spacing:0">Current Worklist</span>'
        f'{_sap_badge("SAP GTS")}</div>'
        '<table style="width:100%;border-collapse:collapse">'
        '<thead><tr>'
        + _pth("Supplier", ["SUPPLIER_NAME"])
        + _pth("Trade Lane", ["CTYDP", "CTYAR"])
        + _pth("Doc Type", ["POO_TYPE"])
        + _pth("Deadline", ["VDECL_DEADLINE"])
        + _pth("Status", ["POO_STATUS"]) +
        '</tr></thead>'
        f'<tbody>{coo_rows}</tbody>'
        '</table></div>'
    )

    # ── RoO Compliance Assessment (collapsible) ──────────────────────────
    roo_badge_styles_assess = {
        "Q": "background:#e6fff9;color:#12B3A3",
        "M": "background:#fff3cd;color:#856404",
        "F": "background:#fde8e8;color:#c0392b",
    }
    effort_styles = {
        "Low":    "background:#e6fff9;color:#12B3A3",
        "Medium": "background:#fff3cd;color:#856404",
        "High":   "background:#fde8e8;color:#c0392b",
    }

    # roo_items is a list or {"unavailable": True, "missing_cols": [...]}
    _roo_unavailable = isinstance(roo_items, dict) and roo_items.get("unavailable")

    if _roo_unavailable:
        _miss_str = ", ".join(roo_items.get("missing_cols", []))
        roo_body = (
            '<div style="padding:24px 20px;text-align:center;color:#999">'
            '\U0001f4cb RoO Assessment requires upload columns: '
            f'<code style="font-size:0.78rem;color:#A100FF">{_miss_str}</code><br>'
            '<span style="font-size:0.72rem">Add these columns to your shipment file and re-upload.</span>'
            '</div>'
        )
        _roo_footer = ""
    else:
        roo_rows = ""
        for p in roo_items:
            roo_code  = p["ROO_STATUS"]
            roo_label = ROO_STATUS_LABELS.get(roo_code, roo_code)
            badge     = roo_badge_styles_assess.get(roo_code, "")
            gap_cell  = (
                f'<span style="color:#c0392b;font-weight:600">−{p["gap_pct"]} pts</span>'
                if p["gap_pct"] > 0 else
                '<span style="color:#12B3A3;font-weight:600">—</span>'
            )
            roo_rows += (
                '<tr style="border-bottom:1px solid #f5f3fa">'
                f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600">{p["PRODUCT_TEXT"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.78rem;font-family:monospace">{p["CCNGN"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:#A100FF">{p["AGREEMENT"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.78rem;color:#555">{p["roo_test_type"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.82rem;text-align:center">'
                f'{p["RVC_PCT"]}% / {p["RVC_THRESHOLD"]}%</td>'
                f'<td style="padding:10px 12px;text-align:center">'
                f'<span style="padding:2px 8px;border-radius:4px;font-size:0.72rem;'
                f'font-weight:600;{badge}">{roo_label}</span></td>'
                f'<td style="padding:10px 12px;text-align:center">{gap_cell}</td>'
                f'<td style="padding:10px 12px;font-size:0.78rem;color:#666">{p["compliance_note"]}</td>'
                '</tr>'
            )
        roo_body = (
            '<div style="overflow-x:auto;max-height:280px;overflow-y:auto">'
            '<table style="width:100%;border-collapse:collapse">'
            '<thead><tr>'
            + _pth("Product", ["PRODUCT_TEXT"])
            + _pth("HS Code", ["CCNGN"])
            + _pth("Agreement", ["AGREEMENT"])
            + _pth("RoO Test", [])
            + _pth("RVC Actual / Required", ["RVC_PCT", "RVC_THRESHOLD"])
            + _pth("RoO Status", ["ROO_STATUS"])
            + _pth("Gap", ["RVC_PCT", "RVC_THRESHOLD"])
            + _pth("Compliance Note", []) +
            '</tr></thead>'
            f'<tbody>{roo_rows}</tbody>'
            '</table></div>'
        )
        _roo_footer = (
            '<div style="padding:8px 16px;font-size:0.68rem;color:#bbb;border-top:1px solid #f0eaf8">'
            + ('Illustrative benchmarks — requires client-specific quantification.'
               if _is_sim else 'Derived from uploaded shipment data.')
            + '</div>'
        )

    roo_section = (
        '<details style="margin-bottom:12px;border-radius:10px;'
        'box-shadow:0 2px 8px rgba(26,5,51,0.07);background:#fff;overflow:hidden">'
        '<summary style="padding:14px 20px;font-size:0.8rem;font-weight:700;'
        'text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #f0eaf8;'
        'display:flex;align-items:center;gap:8px;cursor:pointer;list-style:none;'
        'user-select:none">'
        '\U0001f4cb RoO / Preference Assessment'
        f'{_sap_badge("SAP GTS")}'
        '<span style="font-size:0.72rem;font-weight:400;'
        'color:#A100FF;text-transform:none;letter-spacing:0">▼ expand</span>'
        '</summary>'
        + roo_body + _roo_footer
        + '</details>'
    )

    # ── Qualification Roadmap (collapsible) ──────────────────────────────
    roadmap_rows = ""
    for item in roadmap:
        effort_badge = effort_styles.get(item["effort"], "")
        sc = "#c0392b" if item["UNCLAIMED_SAVINGS_K"] > 200 else "#1a0533"
        roadmap_rows += (
            '<tr style="border-bottom:1px solid #f5f3fa">'
            f'<td style="padding:10px 12px;font-size:0.82rem">{item["lane_display"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:#A100FF">'
            f'{item["AGREEMENT"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">{item["UTILIZATION_PCT"]}%</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:{sc}">'
            f'${item["UNCLAIMED_SAVINGS_K"]}K</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">'
            f'<div style="font-weight:600;color:#1a0533">{item["primary_action"]}</div>'
            f'<div style="font-size:0.75rem;color:#888;margin-top:2px">{item["secondary_action"]}</div>'
            '</td>'
            f'<td style="padding:10px 12px">'
            f'<span style="padding:2px 8px;border-radius:4px;font-size:0.72rem;'
            f'font-weight:600;{effort_badge}">{item["effort"]}</span></td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;white-space:nowrap;color:#555">'
            f'{item["timeline"]}</td>'
            '</tr>'
        )

    roadmap_section = (
        '<details style="margin-bottom:12px;border-radius:10px;'
        'box-shadow:0 2px 8px rgba(26,5,51,0.07);background:#fff;overflow:hidden">'
        '<summary style="padding:14px 20px;font-size:0.8rem;font-weight:700;'
        'text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #f0eaf8;'
        'display:flex;align-items:center;gap:8px;cursor:pointer;list-style:none;'
        'user-select:none">'
        '\U0001f5fa Qualification Roadmap — Under-Utilised Lanes'
        f'{_sap_badge("SAP GTS")}'
        '<span style="font-size:0.72rem;font-weight:400;'
        'color:#A100FF;text-transform:none;letter-spacing:0">▼ expand</span>'
        '</summary>'
        '<div style="overflow-x:auto;max-height:280px;overflow-y:auto">'
        '<table style="width:100%;border-collapse:collapse">'
        '<thead><tr>'
        + _pth("Lane", ["CTYDP", "CTYAR"])
        + _pth("FTA", ["AGREEMENT"])
        + _pth("Utilization", ["PREF_STATUS", "CUSVAL"])
        + _pth("Opportunity", ["CUSVAL", "MFN_RATE", "PREF_RATE"])
        + _pth("Recommended Actions", [])
        + _pth("Effort", [])
        + _pth("Timeline", []) +
        '</tr></thead>'
        f'<tbody>{roadmap_rows}</tbody>'
        '</table></div>'
        '<div style="padding:8px 16px;font-size:0.68rem;color:#bbb;border-top:1px solid #f0eaf8">'
        'Opportunity figures sourced from FTA Lane Utilization Gap table — same formula. '
        + ('Illustrative benchmarks — requires client-specific quantification.'
           if _is_sim else 'Derived from uploaded shipment data.')
        + '</div>'
        '</details>'
    )

    # ── SAP Provenance Modal (persistent panel, opened by dot buttons) ─────
    prov_modal_html = (
        '<div id="sap-prov-modal" '
        'style="display:none;position:fixed;inset:0;z-index:10000;'
        'background:rgba(26,5,51,0.48);align-items:center;justify-content:center" '
        'onclick="if(event.target===this)closeSapProv()">'
        '<div style="background:#fff;border-radius:14px;padding:28px 32px 24px;'
        'max-width:560px;width:92%;box-shadow:0 12px 48px rgba(26,5,51,0.25);'
        'position:relative;max-height:82vh;overflow-y:auto">'
        '<button onclick="closeSapProv()" '
        'style="position:absolute;top:14px;right:16px;background:#f5f3fa;border:none;'
        'width:28px;height:28px;border-radius:50%;font-size:0.85rem;color:#888;'
        'cursor:pointer;display:flex;align-items:center;justify-content:center;'
        'line-height:1" title="Close (Esc)" aria-label="Close">&#x2715;</button>'
        '<div id="sap-prov-content"></div>'
        '</div></div>'
    )

    # ── Compose page ─────────────────────────────────────────────────────
    content = (
        header_html + upload_section + kpi_html
        + '<div style="display:flex;gap:20px;align-items:flex-start;margin-bottom:20px">'
        + f'<div style="flex:3;min-width:0">{lane_section}{shipment_section}</div>'
        + f'<div style="flex:2;min-width:0">{coo_section}</div>'
        + '</div>'
        + roo_section
        + roadmap_section
        + prov_modal_html
    )

    # ── Scripts ──────────────────────────────────────────────────────────
    scripts = """
<script>
function fetchFTAExplain(row, shipmentId) {
    var box = document.getElementById('fta-ai-box');
    if (!box) return;
    var payload = {
        shipment_id:  row.dataset.shipmentId,
        product:      row.dataset.product,
        hs_code:      row.dataset.hsCode,
        origin:       row.dataset.origin,
        destination:  row.dataset.destination,
        fta_name:     row.dataset.ftaName,
        value_k:      parseFloat(row.dataset.valueK),
        eligibility:  row.dataset.eligibility,
        est_saving_k: parseFloat(row.dataset.estSavingK),
        ro_status:        row.dataset.roStatus,
        rvc_pct:          parseFloat(row.dataset.rvcPct),
        rvc_threshold_pct: parseFloat(row.dataset.rvcThreshold)
    };
    box.style.display = 'flex';
    box.innerHTML = '<div class="ai-icon">\U0001f916</div><div>'
        + '<div class="ai-label">AI FTA Advisor</div>'
        + '<div class="ai-text"><span class="ai-loading">Analyzing '
        + shipmentId + '…</span></div></div>';
    fetch('/api/fta/explain', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        box.innerHTML = '<div class="ai-icon">\U0001f916</div><div>'
            + '<div class="ai-label">AI FTA Analysis — ' + shipmentId + '</div>'
            + '<div class="ai-text">'
            + data.explanation.replace(/\\n/g, '<br>') + '</div></div>';
        box.scrollIntoView({behavior: 'smooth', block: 'nearest'});
    })
    .catch(function() {
        box.innerHTML = '<div class="ai-icon">\U0001f916</div><div>'
            + '<div class="ai-label">AI FTA Advisor</div>'
            + '<div class="ai-text">Analysis unavailable — please try again.'
            + '</div></div>';
    });
}

(function() {
  function loadTariffFeed() {
    fetch('/api/tariff-feed')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var sc = document.getElementById('sources-container');
        if (sc && data.sources.length > 0) {
          var h = '<div class="sources-title">Source Monitoring</div>';
          data.sources.forEach(function(s) {
            h += '<div class="source-row">'
               + '<span class="source-icon">' + s.icon + '</span>'
               + '<span class="source-name">' + s.name + '</span>'
               + '<span class="source-badge">' + s.status + '</span></div>';
          });
          sc.innerHTML = h;
        }
        var fc = document.getElementById('tariff-feed-container');
        if (fc && data.feed.length > 0) {
          var h2 = '<div class="feed-section-label">Recent Feed</div>';
          data.feed.forEach(function(ev) {
            h2 += '<div class="tariff-event">'
               + '<div class="tariff-event-header">'
               + '<div class="tariff-headline">' + ev.headline + '</div>'
               + '<span class="tariff-status-badge status-' + ev.status + '">'
               + ev.status + '</span></div>'
               + '<div class="tariff-time">' + ev.time_short + '</div>'
               + '<div class="tariff-detail">' + ev.detail + '</div>'
               + '<div class="tariff-source">via ' + ev.source + '</div></div>';
          });
          fc.innerHTML = h2;
        }
      })
      .catch(function(e) { console.error('Tariff feed error:', e); });
  }
  loadTariffFeed();
  setInterval(loadTariffFeed, 10000);
})();

// ── SAP Provenance Panel ─────────────────────────────────────────────────
function openSapProv(btn) {
    var colLabel = btn.dataset.colLabel;
    var provCode = btn.dataset.provCode;
    var fields;
    try { fields = JSON.parse(btn.dataset.fields); }
    catch(e) { console.error('Prov panel parse error:', e); return; }

    var overallGreen = provCode === 'green';
    var dotColor  = overallGreen ? '#12B3A3' : '#F5A623';
    var provLabel = overallGreen ? '🟢 Verified SAP field' : '🟡 Pending SAP confirmation';
    var provBg    = overallGreen ? '#e6fff9' : '#fff8e6';
    var provFg    = overallGreen ? '#0a7060' : '#856404';

    // Build SAP name list and deduplicate source systems
    var sapNames = fields.map(function(f){ return f.sap; }).join(' · ');
    var srcMap = {}; fields.forEach(function(f){ srcMap[f.src] = 1; });
    var srcSystems = Object.keys(srcMap).join(' + ');

    var html = '';
    // ── Panel header ──────────────────────────────────────────────────────
    html += '<div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:18px">';
    html += '<span style="width:15px;height:15px;border-radius:50%;background:' + dotColor +
            ';display:inline-block;flex-shrink:0;margin-top:4px"></span>';
    html += '<div style="flex:1">';
    html += '<div style="font-size:1.05rem;font-weight:700;color:#1a0533;margin-bottom:9px">' +
            colLabel + '</div>';
    html += '<div style="display:flex;gap:6px;flex-wrap:wrap">';
    html += '<span style="font-size:0.72rem;font-weight:700;background:' + provBg +
            ';color:' + provFg + ';padding:3px 10px;border-radius:5px">' + provLabel + '</span>';
    html += '<span style="font-family:monospace;font-size:0.76rem;font-weight:700;' +
            'background:#f0eaf8;color:#A100FF;padding:3px 10px;border-radius:5px">' +
            sapNames + '</span>';
    html += '<span style="font-size:0.72rem;background:#f5f5fa;color:#666;' +
            'padding:3px 10px;border-radius:5px">' + srcSystems + '</span>';
    html += '</div></div></div>';

    // ── Field descriptions ────────────────────────────────────────────────
    if (fields.length === 1) {
        var f = fields[0];
        html += '<p style="font-size:0.86rem;color:#2d2d3a;line-height:1.72;margin:0 0 16px;' +
                'border-top:1px solid #f0eaf8;padding-top:16px">' + f.long + '</p>';
        if (f.fmt) {
            html += '<div style="background:#faf8fe;border-radius:7px;padding:10px 14px;' +
                    'border-left:3px solid #A100FF;font-size:0.79rem;color:#555">' +
                    '<span style="font-weight:700;color:#1a0533">Format / Example:&nbsp;</span>' +
                    f.fmt + '</div>';
        }
    } else {
        fields.forEach(function(f, i) {
            var fg   = f.prov === 'green';
            var fDot = fg ? '#12B3A3' : '#F5A623';
            var fBg  = fg ? '#e6fff9' : '#fff8e6';
            var fFg  = fg ? '#0a7060' : '#856404';
            var fPL  = fg ? '🟢 Verified' : '🟡 Pending confirmation';
            html += '<div style="border-top:1px solid #f0eaf8;padding-top:14px;' +
                    'margin-top:' + (i === 0 ? '0' : '14px') + '">';
            html += '<div style="display:flex;align-items:center;gap:7px;margin-bottom:8px">';
            html += '<span style="width:8px;height:8px;border-radius:50%;background:' + fDot +
                    ';display:inline-block;flex-shrink:0"></span>';
            html += '<span style="font-family:monospace;font-size:0.83rem;font-weight:700;' +
                    'color:#A100FF">' + f.sap + '</span>';
            html += '<span style="font-size:0.76rem;color:#888">' + f.univ + '</span>';
            html += '<span style="font-size:0.65rem;font-weight:700;background:' + fBg +
                    ';color:' + fFg + ';padding:2px 7px;border-radius:4px;margin-left:auto">' +
                    fPL + '</span>';
            html += '</div>';
            html += '<p style="font-size:0.84rem;color:#2d2d3a;line-height:1.68;margin:0 0 6px">' +
                    f.long + '</p>';
            if (f.fmt) {
                html += '<div style="font-size:0.73rem;color:#777;margin-top:6px">' +
                        '<span style="font-weight:700;color:#555">Format:&nbsp;</span>' +
                        f.fmt + '</div>';
            }
            html += '</div>';
        });
    }

    document.getElementById('sap-prov-content').innerHTML = html;
    var modal = document.getElementById('sap-prov-modal');
    modal.style.display = 'flex';
    document.body.style.overflow = 'hidden';
}

function closeSapProv() {
    var modal = document.getElementById('sap-prov-modal');
    if (modal) { modal.style.display = 'none'; }
    document.body.style.overflow = '';
}

document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') { closeSapProv(); }
});
</script>
"""

    return render_template_string(
        BASE,
        title="FTA & Preferential Trade Agent",
        css=_CSS,
        sidebar=_sidebar_html("fta_preferential"),
        content=content,
        scripts=scripts,
    )


# ---------------------------------------------------------------------------
# Agent detail page
# ---------------------------------------------------------------------------

@app.route("/agent/<agent_id>")
def agent_detail(agent_id):
    agent = get_agent(agent_id)
    if not agent:
        return redirect(url_for("control_tower"))

    color = agent["cluster_color"]

    header_html = f"""
    <a href="/" class="back-link">← Control Tower</a>
    <div class="agent-detail-header"
         style="background: linear-gradient(135deg, #1a0533 0%, {color}88 100%);">
      <div style="font-size:2.4rem">{agent['icon']}</div>
      <div>
        <div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:1.5px;
                    color:{color};font-weight:700;">{agent['cluster']}</div>
        <h1 style="font-size:1.5rem;font-weight:700;">{agent['display_name']}</h1>
        <div style="font-size:0.85rem;color:rgba(255,255,255,0.65);margin-top:4px;">
          {agent['tagline']}
        </div>
      </div>
    </div>
    """

    if agent["status"] == "coming_soon":
        body_html = f"""
        <div class="coming-soon-box">
          <div class="cs-icon">🚧</div>
          <h2>This agent is coming soon</h2>
          <p>{agent['display_name']} is under development and will appear here
             fully operational once activated.</p>
        </div>
        """
        scripts = ""
    else:
        # ================================================================
        # AGENT LOGIC PLUG-IN POINT
        # When activating an agent:
        #   1. Flip status to "live" in agents_registry.py
        #   2. Replace the placeholder section below with real agent output
        #   3. Call generate_ai_explanation(agent_context) for AI narrative
        # ================================================================
        body_html = f"""
        <div class="ai-summary" id="agent-ai-box">
          <div class="ai-icon">🤖</div>
          <div>
            <div class="ai-label">AI Agent Analysis</div>
            <div class="ai-text" id="agent-ai-text">
              <span class="ai-loading">Generating analysis…</span>
            </div>
          </div>
        </div>
        <div class="section-card">
          <div class="section-card-header">📊 Agent Output</div>
          <div style="padding:40px;text-align:center;color:#aaa;font-size:0.88rem;">
            Agent data will render here once the live data pipeline is connected.
          </div>
        </div>
        """
        scripts = f"""
        <script>
        (function() {{
          fetch('/api/posture-summary')
            .then(r => r.json())
            .then(data => {{
              var el = document.getElementById('agent-ai-text');
              if (el) el.innerHTML = data.summary.replace(/\\n/g, '<br>');
            }})
            .catch(function() {{
              var el = document.getElementById('agent-ai-text');
              if (el) el.innerHTML = 'AI analysis unavailable.';
            }});

          // Load tariff feed
          function loadTariffFeed() {{
            fetch('/api/tariff-feed')
              .then(r => r.json())
              .then(data => {{
                var sourcesContainer = document.getElementById('sources-container');
                if (sourcesContainer && data.sources.length > 0) {{
                  var sourcesHtml = '<div class="sources-title">Source Monitoring</div>';
                  data.sources.forEach(source => {{
                    sourcesHtml += `
                      <div class="source-row">
                        <span class="source-icon">${{source.icon}}</span>
                        <span class="source-name">${{source.name}}</span>
                        <span class="source-badge">${{source.status}}</span>
                      </div>`;
                  }});
                  sourcesContainer.innerHTML = sourcesHtml;
                }}

                var feedContainer = document.getElementById('tariff-feed-container');
                if (feedContainer && data.feed.length > 0) {{
                  var feedHtml = '<div class="feed-section-label">Recent Feed</div>';
                  data.feed.forEach(event => {{
                    var statusClass = 'status-' + event.status;
                    feedHtml += `
                      <div class="tariff-event">
                        <div class="tariff-event-header">
                          <div class="tariff-headline">${{event.headline}}</div>
                          <span class="tariff-status-badge ${{statusClass}}">${{event.status}}</span>
                        </div>
                        <div class="tariff-time">${{event.time_short}}</div>
                        <div class="tariff-detail">${{event.detail}}</div>
                        <div class="tariff-source">via ${{event.source}}</div>
                      </div>`;
                  }});
                  feedContainer.innerHTML = feedHtml;
                }}
              }});
          }}
          loadTariffFeed();
          setInterval(loadTariffFeed, 10000);
        }})();
        </script>
        """

    return render_template_string(
        BASE,
        title=agent["display_name"],
        css=_CSS,
        sidebar=_sidebar_html(agent_id),
        content=header_html + body_html,
        scripts=scripts if agent["status"] != "coming_soon" else "",
    )


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=False, port=5000, threaded=True, use_reloader=False)
