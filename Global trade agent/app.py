import asyncio
import concurrent.futures
import json
import os
import threading
import time

from flask import Flask, render_template_string, redirect, url_for, jsonify, request
from dotenv import load_dotenv
from air import AsyncAIRefinery

from agents_registry import AGENTS, CLUSTERS, get_agent, agents_by_cluster
from data_simulator import get_kpis, get_alerts, get_tariff_sources, get_tariff_feed

load_dotenv()
_API_KEY = str(os.getenv("API_KEY"))

app = Flask(__name__)

# Cache so repeated refreshes never trigger a second AI call
_ai_cache: dict = {"text": None, "ts": 0.0, "lock": threading.Lock(), "in_flight": False}
_AI_CACHE_TTL = 60  # seconds

# In-memory action log for the control tower workflow
_action_log: list[dict] = []


def _render_action_log_rows(limit: int | None = None) -> str:
    entries = list(reversed(_action_log))
    if limit is not None:
        entries = entries[:limit]

    if not entries:
        return """
        <tr>
          <td colspan="5" style="padding: 18px; color: #6f6b7d;">No completed actions yet. Mark a task complete in the control tower to populate this page.</td>
        </tr>
        """

    rows = ""
    for item in entries:
        remarks = (item.get("remarks") or "—").replace("\n", "<br>")
        rows += f"""
        <tr>
          <td>{item['role']}</td>
          <td>{item['taskText']}</td>
          <td><span class="badge-pill">{item['status']}</span></td>
          <td>{item['completedOn']}</td>
          <td>{remarks}</td>
        </tr>
        """
    return rows


# ---------------------------------------------------------------------------
# AI Integration
# ---------------------------------------------------------------------------

async def _call_ai(prompt: str) -> str:
    client = AsyncAIRefinery(api_key=_API_KEY)
    response = await client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="Qwen/Qwen3-32B",
    )
    return response.choices[0].message.content


def _run_ai_in_thread(prompt: str) -> str:
    """Run async AI call in a fresh event loop on a background thread."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(asyncio.wait_for(_call_ai(prompt), timeout=30))
    finally:
        loop.close()


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

    prompt = (
        "You are TradeNavigator AI, an expert global trade advisor. "
        "Based on the following trade KPIs, write a concise 2-sentence executive "
        "summary of the company's current trade posture, highlighting the biggest "
        "opportunity and the biggest risk. Be specific with the numbers.\n\n"
        f"Total duty paid this quarter: ${context.get('total_duty_paid_m', 'N/A')}M\n"
        f"FTA capture rate: {context.get('fta_capture_rate_pct', 'N/A')}%\n"
        f"Drawback recovered: ${context.get('drawback_recovered_k', 'N/A')}K\n"
        f"Open compliance flags: {context.get('open_compliance_flags', 'N/A')}\n"
        f"Active tariff alerts: {context.get('active_tariff_alerts', 'N/A')}\n"
        f"Value at risk: ${context.get('value_at_risk_m', 'N/A')}M\n"
    )

    def _fetch():
        try:
            result = _run_ai_in_thread(prompt)
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


@app.route("/api/action-log", methods=["GET", "POST"])
def api_action_log():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        entry = {
            "id": len(_action_log) + 1,
            "role": payload.get("role", "Supervisor"),
            "taskId": payload.get("taskId", "unknown"),
            "taskText": payload.get("taskText", "Unnamed task"),
            "completedOn": payload.get("completedOn", "—"),
            "remarks": payload.get("remarks", ""),
            "status": "Completed",
        }
        _action_log.append(entry)
        return jsonify({"ok": True, "entry": entry})

    return jsonify({"entries": list(reversed(_action_log))})


# ---------------------------------------------------------------------------
# Shared CSS
# ---------------------------------------------------------------------------

_CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body {
    height: 100%;
    overflow-x: auto;
    overflow-y: hidden;
}
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, sans-serif;
    background: linear-gradient(135deg, #f7f5ff 0%, #f2f6fb 100%);
    color: #1a0533;
    display: flex;
    min-height: 100vh;
    min-width: 100%;
}

/* ── Sidebar ─────────────────────────────────────────────────── */
.sidebar {
    width: 260px;
    min-height: 100vh;
    background: linear-gradient(180deg, #1a0533 0%, #22074b 100%);
    color: #fff;
    display: flex;
    flex-direction: column;
    position: fixed;
    top: 0; left: 0; bottom: 0;
    overflow-y: auto;
    z-index: 100;
    box-shadow: 10px 0 24px rgba(26,5,51,0.12);
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
.main { margin-left: 260px; flex: 1; display: flex; flex-wrap: nowrap; height: 100vh; min-width: 720px; overflow: hidden; }
.page-shell { display: grid; gap: 16px; }
.page-card {
    background: rgba(255,255,255,0.95);
    border: 1px solid rgba(161,0,255,0.1);
    border-radius: 16px;
    padding: 20px;
    box-shadow: 0 10px 24px rgba(26,5,51,0.06);
}
.page-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: #1a0533;
    margin-bottom: 6px;
}
.page-subtitle {
    font-size: 0.84rem;
    color: #6f6b7d;
    margin-bottom: 12px;
}
.page-grid {
    display: grid;
    gap: 16px;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
}
.info-card {
    background: linear-gradient(135deg, #faf7ff 0%, #f7fbff 100%);
    border: 1px solid rgba(161,0,255,0.08);
    border-radius: 12px;
    padding: 16px;
}
.info-card .label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #A100FF;
    font-weight: 700;
}
.info-card .value {
    font-size: 1rem;
    font-weight: 700;
    color: #1a0533;
    margin-top: 6px;
}
.log-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.82rem;
}
.log-table th, .log-table td {
    padding: 10px 12px;
    border-bottom: 1px solid #efe8f7;
    text-align: left;
    vertical-align: top;
}
.log-table thead th {
    background: #f6f1ff;
    color: #5e5871;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.9px;
}
.log-table tbody tr:hover {
    background: #fcfaff;
}
.badge-pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 8px;
    border-radius: 999px;
    background: #e8fff9;
    color: #12B3A3;
    font-size: 0.72rem;
    font-weight: 700;
}
.main-content {
    flex: 1 1 auto;
    min-width: 0;
    padding: 28px 32px 32px;
    overflow-y: auto;
    overflow-x: hidden;
    display: flex;
    flex-direction: column;
    gap: 20px;
    scrollbar-gutter: stable;
}
.main-right-pane {
    flex: 0 0 360px;
    width: 360px;
    max-width: 360px;
    min-width: 360px;
    background: rgba(255,255,255,0.88);
    backdrop-filter: blur(10px);
    border-left: 1px solid rgba(26,5,51,0.08);
    overflow-y: auto;
    overflow-x: hidden;
    display: flex;
    flex-direction: column;
    box-shadow: -8px 0 24px rgba(26,5,51,0.04);
    scrollbar-gutter: stable;
    flex-shrink: 0;
}

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
    border-radius: 16px;
    padding: 24px 28px;
    display: flex; align-items: center; gap: 18px;
    box-shadow: 0 14px 30px rgba(26,5,51,0.12);
}
.role-switcher {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: rgba(255,255,255,0.12);
    border: 1px solid rgba(255,255,255,0.2);
    border-radius: 999px;
    padding: 7px 10px;
    font-size: 0.75rem;
    font-weight: 600;
    color: #fff;
}
.role-switcher select {
    background: transparent;
    color: #fff;
    border: 0;
    outline: none;
    font-size: 0.75rem;
    font-weight: 600;
}
.role-switcher select option {
    color: #1a0533;
}
.page-header .header-icon { font-size: 2.4rem; }
.page-header h1 { font-size: 1.5rem; font-weight: 700; }
.page-header .header-sub { font-size: 0.85rem; color: rgba(255,255,255,0.65); margin-top: 4px; }
.header-pills { display:flex; gap:8px; flex-wrap:wrap; margin-top: 8px; }
.pill {
    display:inline-flex; align-items:center; gap:6px;
    padding: 6px 10px; border-radius: 999px;
    font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.8px; background: rgba(255,255,255,0.13); color: #fff;
}
.pill.live { background: rgba(18,179,163,0.18); color: #b8fff3; }
.accent-bar { height: 4px; border-radius: 2px; margin-bottom: 2px; }

.overview-band {
    display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 14px;
}
.overview-card {
    background: rgba(255,255,255,0.9);
    border: 1px solid rgba(161,0,255,0.12);
    border-radius: 14px;
    padding: 16px 18px;
    box-shadow: 0 8px 20px rgba(26,5,51,0.06);
}
.overview-label { font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1.2px; color: #A100FF; }
.overview-title { font-size: 0.95rem; font-weight: 600; color: #2d1a4a; margin-top: 6px; line-height: 1.4; }

/* ── KPI strip ───────────────────────────────────────────────── */
.kpi-strip {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
    gap: 16px;
}
.kpi-card {
    background: #fff;
    border-radius: 12px;
    padding: 18px 20px;
    box-shadow: 0 8px 22px rgba(26,5,51,0.06);
    border-top: 3px solid #A100FF;
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
    border-radius: 14px;
    box-shadow: 0 8px 22px rgba(26,5,51,0.06);
    overflow: hidden;
}
.section-card-header {
    padding: 14px 20px;
    font-size: 0.8rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    border-bottom: 1px solid #f0eaf8;
    display: flex; align-items: center; gap: 8px;
    background: linear-gradient(90deg, rgba(161,0,255,0.05) 0%, rgba(255,255,255,0) 100%);
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
.cluster-section { margin-bottom: 8px; }
.cluster-section h3 {
    font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1.5px; margin-bottom: 12px; padding-left: 4px;
}
.agent-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 16px; }
.agent-tile {
    background: linear-gradient(180deg, #ffffff 0%, #faf8ff 100%);
    border-radius: 12px;
    padding: 18px 16px;
    box-shadow: 0 8px 20px rgba(26,5,51,0.06);
    text-decoration: none;
    color: #1a0533;
    display: flex; flex-direction: column; gap: 8px;
    border-top: 3px solid;
    transition: transform 0.15s, box-shadow 0.15s;
    position: relative;
}
.agent-tile:hover { transform: translateY(-2px); box-shadow: 0 10px 24px rgba(26,5,51,0.12); }
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

/* ── Role to-do ─────────────────────────────────────────────── */
.role-todo {
    background: #fff;
    border-radius: 14px;
    box-shadow: 0 8px 22px rgba(26,5,51,0.06);
    padding: 18px 20px;
    border: 1px solid rgba(161,0,255,0.12);
}
.role-todo-title {
    font-size: 0.8rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #A100FF;
    margin-bottom: 10px;
}
.role-todo-list {
    display: grid;
    gap: 8px;
}
.role-todo-item {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 10px 12px;
    background: #f8f6ff;
    border-radius: 10px;
    font-size: 0.84rem;
    color: #2d1a4a;
}
.role-todo-item .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #12B3A3;
    margin-top: 6px;
    flex-shrink: 0;
}
.role-todo-item.completed {
    background: #f2fbf8;
    border: 1px solid rgba(18,179,163,0.15);
}
.role-todo-item.completed .dot {
    background: #12B3A3;
}
.task-text { flex: 1; }
.task-meta { margin-top: 4px; font-size: 0.72rem; color: #6f6b7d; }
.todo-complete-btn {
    background: #1a0533;
    color: #fff;
    border: 0;
    border-radius: 999px;
    font-size: 0.72rem;
    font-weight: 700;
    padding: 6px 10px;
    cursor: pointer;
}
.todo-complete-btn:hover { background: #2d0a5a; }
.todo-state-badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.7px;
    color: #12B3A3;
    margin-left: 8px;
}
.todo-modal {
    position: fixed;
    inset: 0;
    background: rgba(26,5,51,0.45);
    display: none;
    align-items: center;
    justify-content: center;
    z-index: 200;
    padding: 20px;
}
.todo-modal.active { display: flex; }
.todo-modal-card {
    width: min(480px, 100%);
    background: #fff;
    border-radius: 14px;
    padding: 20px;
    box-shadow: 0 16px 40px rgba(26,5,51,0.18);
}
.todo-modal-title {
    font-size: 0.95rem;
    font-weight: 700;
    color: #1a0533;
    margin-bottom: 12px;
}
.todo-modal label { display: block; font-size: 0.8rem; font-weight: 600; color: #5e5871; margin-bottom: 6px; }
.todo-modal textarea, .todo-modal input {
    width: 100%;
    border: 1px solid #e5dff0;
    border-radius: 10px;
    padding: 10px 12px;
    margin-bottom: 12px;
    font-size: 0.84rem;
    color: #1a0533;
}
.todo-modal-actions { display: flex; justify-content: flex-end; gap: 8px; }
.todo-modal-actions button {
    border: 0;
    border-radius: 999px;
    padding: 8px 12px;
    cursor: pointer;
    font-weight: 700;
    font-size: 0.78rem;
}
.todo-modal-actions .save-btn { background: #1a0533; color: #fff; }
.todo-modal-actions .cancel-btn { background: #f2eef9; color: #5e5871; }

/* ── Misc ─────────────────────────────────────────────────────── */
.back-link {
    display: inline-flex; align-items: center; gap: 6px;
    color: #A100FF; text-decoration: none; font-size: 0.83rem; font-weight: 600;
    margin-bottom: 18px;
}
.back-link:hover { text-decoration: underline; }
.footer {
    margin-top: 20px; padding-top: 16px;
    border-top: 1px solid #e8e0f0;
    font-size: 0.72rem; color: #b2a8c8; text-align: right;
}

@media (max-width: 520px) {
    .main { flex-direction: column; }
    .main-right-pane {
        flex: 0 0 auto;
        width: 100%;
        max-width: none;
        min-width: 0;
        border-left: 0;
        border-top: 1px solid rgba(26,5,51,0.08);
        max-height: 420px;
    }
}

@media (max-width: 760px) {
    .sidebar { position: static; width: 100%; min-height: auto; max-height: none; }
    .main { margin-left: 0; flex-direction: column; }
    .main-content { padding: 20px; }
    .page-header { flex-direction: column; align-items: flex-start; }
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

    workspace_links = [
        ("profile", "👤 Profile", "/profile"),
        ("settings", "⚙️ Settings", "/settings"),
        ("action-logs", "🗂️ Action Logs", "/action-logs"),
    ]
    workspace_rows = "".join(
        f'<a href="{url}" class="nav-agent {"active" if active_id == item_id else ""}">{label}</a>'
        for item_id, label, url in workspace_links
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
        <div class="cluster-label" style="color:#A100FF">Workspace</div>
        {workspace_rows}
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

    ai_html = ""

    alert_rows = ""
    for al in alerts:
        sev = al["severity"]
        alert_rows += f"""
        <div class="alert-row">
          <div class="sev-dot sev-{sev}"></div>
          <div class="alert-msg">{al['message']}</div>
          <div class="alert-ts">{al['timestamp']}</div>
        </div>"""

    alerts_html = ""

    cluster_tiles = ""

    role_todos = {
        "Supervisor": [
            {"id": "sup-1", "text": "Review cross-functional escalations and confirm owners."},
            {"id": "sup-2", "text": "Approve this week’s trade-risk mitigation priorities."},
            {"id": "sup-3", "text": "Check status of delayed customs and tariff actions."},
        ],
        "Analyst": [
            {"id": "analyst-1", "text": "Validate the latest tariff and FTA updates."},
            {"id": "analyst-2", "text": "Prepare the daily trade posture summary for review."},
            {"id": "analyst-3", "text": "Flag anomalies in shipment and duty data."},
        ],
        "Operations": [
            {"id": "ops-1", "text": "Coordinate urgent compliance follow-up tasks."},
            {"id": "ops-2", "text": "Confirm shipment exceptions with regional teams."},
            {"id": "ops-3", "text": "Update the response plan for active tariff events."},
        ],
    }
    role_todos_json = json.dumps(role_todos)

    selected_role = "Supervisor"
    log_rows = _render_action_log_rows(5)
    todo_items = "".join(
        f'<div class="role-todo-item" data-role="{selected_role}" data-task-id="{item["id"]}">'
        f'<span class="dot"></span>'
        f'<div class="task-text">{item["text"]}</div>'
        f'<button class="todo-complete-btn" type="button">Complete</button>'
        f'</div>'
        for item in role_todos[selected_role]
    )

    content = f"""
    <div class="page-shell">
      <div class="page-card">
        <div class="page-title">Operations control center</div>
        <div class="page-subtitle">Track your most recent actions and manage the daily workflow from one place.</div>
        <div class="page-grid">
          <div class="info-card">
            <div class="label">Active Persona</div>
            <div class="value">{selected_role}</div>
          </div>
          <div class="info-card">
            <div class="label">Pending Tasks</div>
            <div class="value">{len(role_todos[selected_role])}</div>
          </div>
          <div class="info-card">
            <div class="label">Recent Log Entries</div>
            <div class="value">{len(_action_log)}</div>
          </div>
        </div>
      </div>

      <div class="page-header">
      <div class="header-icon">🗼</div>
      <div style="flex:1;">
        <div class="accent-bar" style="background:#A100FF; width:60px;"></div>
        <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:16px; flex-wrap:wrap;">
          <div>
            <h1>Trade Control Tower</h1>
            <div class="header-sub">Consolidated global trade intelligence — all agents, one view</div>
          </div>
          <div class="role-switcher">
            <span>View as</span>
            <select>
              <option>Supervisor</option>
              <option>Analyst</option>
              <option>Operations</option>
            </select>
          </div>
        </div>
      </div>
    </div>
    {kpi_html}
    <div class="role-todo">
      <div class="role-todo-title" id="role-todo-title">{selected_role} to-do list</div>
      <div class="role-todo-list" id="role-todo-list">{todo_items}</div>
    </div>
    <div class="page-card">
      <div class="page-title">Latest action log</div>
      <div class="page-subtitle">Completed actions are added to the operations log automatically.</div>
      <table class="log-table">
        <thead>
          <tr>
            <th>Role</th>
            <th>Task</th>
            <th>Status</th>
            <th>Completed on</th>
            <th>Remarks</th>
          </tr>
        </thead>
        <tbody>{log_rows}</tbody>
      </table>
    </div>
    <div class="todo-modal" id="todo-modal" aria-hidden="true">
      <div class="todo-modal-card">
        <div class="todo-modal-title">Mark task complete</div>
        <label for="todo-date">Completion date</label>
        <input id="todo-date" type="text" readonly>
        <label for="todo-remarks">Remarks</label>
        <textarea id="todo-remarks" rows="4" placeholder="Add remarks for the handoff or follow-up"></textarea>
        <div class="todo-modal-actions">
          <button class="cancel-btn" type="button" id="todo-cancel">Cancel</button>
          <button class="save-btn" type="button" id="todo-save">Save</button>
        </div>
      </div>
    </div>
    """

    # Fetch AI summary after page renders so the page is never blocked
    scripts = """
    <script>
    (function() {
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
    <script>
    (function() {
      const roleTodosData = __ROLE_TODOS_JSON__;
      const storageKey = 'trade-navigator-role-todos';
      const roleSelect = document.querySelector('.role-switcher select');
      const todoTitle = document.getElementById('role-todo-title');
      const todoList = document.getElementById('role-todo-list');
      const modal = document.getElementById('todo-modal');
      const dateInput = document.getElementById('todo-date');
      const remarksInput = document.getElementById('todo-remarks');
      const cancelBtn = document.getElementById('todo-cancel');
      const saveBtn = document.getElementById('todo-save');
      let activeTask = null;

      function formatToday() {
        const now = new Date();
        return now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
      }

      function getStoredTodos() {
        try {
          return JSON.parse(localStorage.getItem(storageKey)) || {};
        } catch (e) {
          return {};
        }
      }

      function saveStoredTodos(state) {
        localStorage.setItem(storageKey, JSON.stringify(state));
      }

      function getRoleState(role) {
        const stored = getStoredTodos();
        if (stored[role]) return stored[role];
        return roleTodosData[role].map(task => ({ ...task, completed: false, completedOn: '', remarks: '' }));
      }

      function persistRoleState(role, tasks) {
        const stored = getStoredTodos();
        stored[role] = tasks;
        saveStoredTodos(stored);
      }

      function renderRoleTasks(role) {
        const tasks = getRoleState(role);
        todoTitle.textContent = role + ' to-do list';
        todoList.innerHTML = tasks.map(task => {
          const completedClass = task.completed ? ' completed' : '';
          const meta = task.completed ? `<div class="task-meta">Completed ${task.completedOn}${task.remarks ? ' • ' + task.remarks : ''}</div>` : '';
          return `
            <div class="role-todo-item${completedClass}" data-role="${role}" data-task-id="${task.id}">
              <span class="dot"></span>
              <div class="task-text">
                ${task.text}
                ${meta}
              </div>
              ${task.completed ? '<span class="todo-state-badge">✓ Done</span>' : '<button class="todo-complete-btn" type="button">Complete</button>'}
            </div>`;
        }).join('');
        bindTaskButtons();
      }

      function bindTaskButtons() {
        todoList.querySelectorAll('.todo-complete-btn').forEach(btn => {
          btn.addEventListener('click', function() {
            const item = btn.closest('.role-todo-item');
            if (!item) return;
            activeTask = {
              role: item.dataset.role,
              taskId: item.dataset.taskId
            };
            dateInput.value = formatToday();
            remarksInput.value = '';
            modal.classList.add('active');
            modal.setAttribute('aria-hidden', 'false');
            remarksInput.focus();
          });
        });
      }

      function closeModal() {
        modal.classList.remove('active');
        modal.setAttribute('aria-hidden', 'true');
        activeTask = null;
      }

      if (roleSelect) {
        roleSelect.addEventListener('change', function() {
          renderRoleTasks(this.value);
        });
      }

      cancelBtn.addEventListener('click', closeModal);
      modal.addEventListener('click', function(e) {
        if (e.target === modal) closeModal();
      });

      saveBtn.addEventListener('click', function() {
        if (!activeTask) return;
        const tasks = getRoleState(activeTask.role);
        const task = tasks.find(item => item.id === activeTask.taskId);
        if (task) {
          task.completed = true;
          task.completedOn = dateInput.value || formatToday();
          task.remarks = remarksInput.value.trim();
          persistRoleState(activeTask.role, tasks);
          renderRoleTasks(activeTask.role);
          fetch('/api/action-log', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              role: activeTask.role,
              taskId: task.id,
              taskText: task.text,
              completedOn: task.completedOn,
              remarks: task.remarks
            })
          }).catch(function(err) { console.error('Log save failed', err); });
        }
        closeModal();
      });

      renderRoleTasks('Supervisor');
    })();
    </script>
    """

    scripts = scripts.replace("__ROLE_TODOS_JSON__", role_todos_json)

    return render_template_string(
        BASE,
        title="Control Tower",
        css=_CSS,
        sidebar=_sidebar_html(""),
        content=content,
        scripts=scripts,
    )


@app.route("/profile")
def profile_page():
    content = f"""
    <div class="page-shell">
      <div class="page-card">
        <div class="page-title">Profile overview</div>
        <div class="page-subtitle">Current workspace and engagement context for the trade control tower.</div>
        <div class="page-grid">
          <div class="info-card"><div class="label">User</div><div class="value">Global Trade Operations</div></div>
          <div class="info-card"><div class="label">Persona</div><div class="value">Supervisor</div></div>
          <div class="info-card"><div class="label">Primary focus</div><div class="value">Escalations, risk mitigation, and decision support</div></div>
        </div>
      </div>
      <div class="page-card">
        <div class="page-title">Operational readiness</div>
        <div class="page-subtitle">A snapshot of the current coverage and handoff readiness.</div>
        <div class="page-grid">
          <div class="info-card"><div class="label">Live agents</div><div class="value">8 active workflows</div></div>
          <div class="info-card"><div class="label">Escalation queue</div><div class="value">13 high-priority issues</div></div>
          <div class="info-card"><div class="label">Last sync</div><div class="value">4 mins ago</div></div>
        </div>
      </div>
    </div>
    """
    return render_template_string(
        BASE,
        title="Profile",
        css=_CSS,
        sidebar=_sidebar_html("profile"),
        content=content,
        scripts="",
    )


@app.route("/settings")
def settings_page():
    content = f"""
    <div class="page-shell">
      <div class="page-card">
        <div class="page-title">Settings</div>
        <div class="page-subtitle">Tune how the control tower surfaces advice, tasks, and alerts.</div>
        <div class="page-grid">
          <div class="info-card"><div class="label">Alert cadence</div><div class="value">Every 10 min</div></div>
          <div class="info-card"><div class="label">Auto-escalation</div><div class="value">Enabled</div></div>
          <div class="info-card"><div class="label">Notifications</div><div class="value">Slack + email</div></div>
        </div>
      </div>
    </div>
    """
    return render_template_string(
        BASE,
        title="Settings",
        css=_CSS,
        sidebar=_sidebar_html("settings"),
        content=content,
        scripts="",
    )


@app.route("/action-logs")
def action_logs_page():
    rows = _render_action_log_rows()

    content = f"""
    <div class="page-shell">
      <div class="page-card">
        <div class="page-title">Action log</div>
        <div class="page-subtitle">A full, tabular audit trail of completed tasks and handoff notes.</div>
        <table class="log-table">
          <thead>
            <tr>
              <th>Role</th>
              <th>Task</th>
              <th>Status</th>
              <th>Completed on</th>
              <th>Remarks</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </div>
    """
    return render_template_string(
        BASE,
        title="Action Logs",
        css=_CSS,
        sidebar=_sidebar_html("action-logs"),
        content=content,
        scripts="",
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
