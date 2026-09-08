import asyncio
import concurrent.futures
import logging
import json
import os
import re
import secrets
import threading
import time

from flask import Flask, render_template_string, redirect, url_for, jsonify, request, session
from dotenv import load_dotenv
from air import AsyncAIRefinery

from agents_registry import AGENTS, CLUSTERS, get_agent, agents_by_cluster
from aggregator.feed_adapter import to_feed_entry as _agg_to_feed_entry
from data_simulator import get_kpis, get_tariff_sources, get_tariff_feed
from industry_catalog import get_industries, get_industry_profile, default_industry, classify_shipment
import fta_data_source
from fta_data_source import (SHIPMENT_TEMPLATE_CSV_WITH_DICT, COO_TEMPLATE_CSV,
                              BOM_TEMPLATE_CSV, ROO_TEMPLATE_CSV)
from fta_simulator import (COUNTRY_NAMES as _CTRY, PREF_STATUS_LABELS, ROO_STATUS_LABELS,
                           POO_STATUS_LABELS, FIELD_DICTIONARY as _FIELD_DICTIONARY)

load_dotenv()
_API_KEY = str(os.getenv("API_KEY"))

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "tnav-dev-secret-change-in-prod")

# Cache so repeated refreshes never trigger a second AI call
_ai_cache: dict = {"text": None, "ts": 0.0, "lock": threading.Lock(), "in_flight": False}
_AI_CACHE_TTL = 60  # seconds

_log = logging.getLogger(__name__)

# Aggregator agent — shared singleton, warmed at startup
# ---------------------------------------------------------------------------

_aggregator = None   # AggregatorAgent | None; set by _boot_aggregator if feed.enabled
_agg_max_entries: int = 25

_tariff_shock_agent = None   # TariffShockAgent | None; set by _boot_tariff_shock


def _boot_tariff_shock(scheduler) -> None:
    """
    Construct TariffShockAgent and subscribe it to the scheduler.

    Called from _boot_aggregator()'s warm-up thread, after the store warm-up and
    before scheduler.start(), so no rate-change events are missed.

    Single-switch rollback (Adjustment 1):
        tariff_shock.enabled=false  -> this function returns early
                                    -> _tariff_shock_agent stays None
                                    -> agent_detail() renders inactive placeholder
        No registry-status flip is needed to roll back — the flag alone controls it.
        Setting enabled=true re-activates by restarting the app.
    """
    global _tariff_shock_agent
    try:
        import yaml
        from tariff_shock.agent import TariffShockAgent

        with open("config.yaml") as _f:
            _cfg = yaml.safe_load(_f)

        ts_cfg = _cfg.get("tariff_shock", {})
        if not ts_cfg.get("enabled", False):
            _log.info(
                "TariffShockAgent disabled (tariff_shock.enabled=false) "
                "— tab will render inactive placeholder"
            )
            return

        stub_volumes = {
            k: float(v) for k, v in ts_cfg.get("stub_volumes", {}).items()
        }
        agent = TariffShockAgent(stub_volumes=stub_volumes)
        scheduler.subscribe(agent.on_rate_change)
        _tariff_shock_agent = agent
        _log.info(
            "TariffShockAgent started (%d stub lane(s)): %s",
            len(stub_volumes), list(stub_volumes.keys()),
        )
    except Exception:
        _log.exception("TariffShockAgent boot failed — tab will render inactive placeholder")


def _boot_aggregator() -> None:
    """
    Construct the shared AggregatorAgent and start the scheduler.

    Called once at module load.  The HTTP warm-up (refresh_lanes) runs on a
    daemon thread so Flask starts immediately and is never blocked by live
    connector calls.

    Startup-race behaviour (adjustment 4):
        _aggregator is set synchronously before the background thread fires.
        If the route is hit during warm-up, recent_feed() calls list_recent()
        on an empty store and returns [].  The route then falls back to the
        simulator — never a 500.

    File-backed store (adjustment 2):
        db_path comes from aggregator.store.db_path in config.yaml
        ("aggregator/data/rates.db") — never ":memory:".  RateStore opens a
        fresh sqlite3 connection per call (the WAL else-branch), so the
        scheduler thread (writer) and the route thread (reader) never share a
        connection object.  WAL allows them to proceed concurrently.
    """
    global _aggregator, _agg_max_entries
    try:
        import yaml
        from aggregator.aggregator_agent import AggregatorAgent
        from aggregator.scheduler import RateScheduler

        with open("config.yaml") as _f:
            _cfg = yaml.safe_load(_f)

        agg_cfg = _cfg.get("aggregator", {})
        feed_cfg = agg_cfg.get("feed", {})

        if not feed_cfg.get("enabled", False):
            _log.info("Aggregator feed disabled (aggregator.feed.enabled=false) — using simulator")
            return

        _agg_max_entries = int(feed_cfg.get("max_entries", 25))
        db_path: str = agg_cfg.get("store", {}).get("db_path", "aggregator/data/rates.db")
        # Adjustment 2 — the store-path line: file-backed, never :memory:
        lanes: list = agg_cfg.get("refresh", {}).get("lanes", [])

        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        _aggregator = AggregatorAgent(db_path=db_path, config=agg_cfg)
        scheduler = RateScheduler(_aggregator, config=agg_cfg)

        def _warm_and_start() -> None:
            try:
                _aggregator.refresh_lanes(lanes)
                _log.info("Aggregator: store warmed (%d lane(s))", len(lanes))
            except Exception:
                _log.warning(
                    "Aggregator warm-up incomplete — feed falls back to simulator "
                    "until the scheduler's first successful run"
                )
            # Subscribe TariffShockAgent before the first scheduler tick so no
            # rate-change events are missed.  Failure here is fully isolated.
            _boot_tariff_shock(scheduler)
            scheduler.start()

        threading.Thread(target=_warm_and_start, daemon=True, name="aggregator-warmup").start()

    except Exception:
        _log.exception("Aggregator boot failed — /api/tariff-feed will use the simulator")


_boot_aggregator()


def _boot_fta_sample() -> None:
    """Auto-load the sample ERP shipment file so the FTA page has data on startup."""
    import os
    sample = os.path.join(os.path.dirname(__file__), "samples", "sample_shipments_erp.csv")
    if not os.path.exists(sample):
        return
    try:
        with open(sample, "rb") as _f:
            _data = _f.read()
        result = fta_data_source.upload_shipment_data(_data, "sample_shipments_erp.csv")
        if result.get("ok"):
            _log.info("FTA sample auto-loaded: %s", sample)
        else:
            _log.warning("FTA sample auto-load failed: %s", result.get("errors"))
    except Exception:
        _log.exception("FTA sample auto-load error")


# _boot_fta_sample()  # disabled: FTA page starts empty; upload manually to load data

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

# Pending uploads waiting for column-mapping confirmation {key: {df, filename}}
_PENDING_UPLOADS: dict = {}

# ---------------------------------------------------------------------------
# AI Integration

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
    """Return live tariff events and source monitoring data, filtered by active industry.

    Routing rules:
      - specific industry + aggregator has matching lanes → filtered real feed
      - specific industry + 0 matching lanes OR aggregator down  → no_industry_coverage
        response with empty feed; never falls back to simulator for specific industries
        (simulator headlines are not HS-scoped so they would mislead under an industry label)
      - "all" + aggregator empty/down → system-health simulator fallback (unchanged)

    active_industry is an intentional part of the API response: it exposes the active
    scope to integrators and the ticker JS for empty-state rendering.
    """
    industry  = _current_industry()
    is_all    = industry.get("name") == "all"
    ind_name  = industry.get("name", "all")
    ind_label = industry.get("display_name", ind_name)

    def _no_coverage():
        return jsonify({
            "sources":               get_tariff_sources(),
            "feed":                  [],
            "active_industry":       ind_name,
            "industry_display_name": ind_label,
            "no_industry_coverage":  True,
        })

    if _aggregator is not None:
        try:
            raw = _aggregator.recent_raw(_agg_max_entries)
            if not is_all:
                raw = [r for r in raw
                       if classify_shipment({"hs_code": r.hs6}, industry)]
            feed = [_agg_to_feed_entry(r) for r in raw]
            if feed:
                return jsonify({
                    "sources":         get_tariff_sources(),
                    "feed":            feed,
                    "active_industry": ind_name,
                })
            if not is_all:
                # Aggregator running but 0 lanes match this industry → clean empty state
                return _no_coverage()
            # is_all + empty aggregator → fall through to system-health simulator
        except Exception:
            _log.exception("Aggregator feed path failed")
            if not is_all:
                return _no_coverage()
            # is_all + exception → fall through to system-health simulator

    if not is_all:
        # Aggregator not configured; still no simulator for specific industries
        return _no_coverage()

    # "all" + aggregator empty/down → system-health simulator fallback (unchanged behaviour)
    return jsonify({
        "sources":         get_tariff_sources(),
        "feed":            get_tariff_feed(),
        "active_industry": "all",
    })


@app.route("/api/tariff-shock")
def api_tariff_shock():
    """Return exposure reports and alerts from the TariffShockAgent, filtered by active industry.

    Filtering:
      - Reports (dict keyed by lane_key): filter on the structured hs6 field in each report.
      - Alerts: filter on the structured hs6 field added by alert_adapter.to_alert().
      - "all" → no filter, current behaviour unchanged.
      - Specific industry + 0 matching alerts AND 0 matching reports → no_industry_coverage.
        This mirrors /api/tariff-feed: never show all-industry data under an industry label.

    active_industry is an intentional API field exposing the active scope.
    All honesty flags (stub_label, review_flag, remedy_applicability, illustrative note
    in message text) are set before filtering and pass through unchanged.
    """
    industry  = _current_industry()
    is_all    = industry.get("name") == "all"
    ind_name  = industry.get("name", "all")
    ind_label = industry.get("display_name", ind_name)

    if _tariff_shock_agent is None:
        return jsonify({
            "enabled":         False,
            "alerts":          [],
            "reports":         {},
            "active_industry": ind_name,
        })

    try:
        alerts  = _tariff_shock_agent.latest_alerts(10)
        reports = _tariff_shock_agent.latest_report()

        if not is_all:
            alerts  = [a for a in alerts
                       if classify_shipment({"hs_code": a.get("hs6", "")}, industry)]
            reports = {k: v for k, v in reports.items()
                       if classify_shipment({"hs_code": v.get("hs6", "")}, industry)}
            if not alerts and not reports:
                return jsonify({
                    "enabled":               True,
                    "alerts":                [],
                    "reports":               {},
                    "active_industry":       ind_name,
                    "industry_display_name": ind_label,
                    "no_industry_coverage":  True,
                })

        return jsonify({
            "enabled":         True,
            "alerts":          alerts,
            "reports":         reports,
            "active_industry": ind_name,
        })
    except Exception:
        _log.exception("TariffShockAgent feed path failed")
        return jsonify({
            "enabled":         True,
            "alerts":          [],
            "reports":         {},
            "error":           "fetch failed",
            "active_industry": ind_name,
        })


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
html, body { height: 100%; overflow-x: auto; overflow-y: hidden; }
body {
    font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 13px;
    background: #F7F7F9;
    color: #1A1A22;
    display: flex;
    min-height: 100vh;
    min-width: 100%;
    line-height: 1.4;
}

/* ── Design tokens ────────────────────────────────────────────── */
:root {
    --accent:      #A100FF;
    --accent-dim:  rgba(161,0,255,0.07);
    --surface:     #FFFFFF;
    --surface-2:   #F7F7F9;
    --border:      #E2E2E8;
    --border-mid:  #CECECE;
    --text-1:      #1A1A22;
    --text-2:      #52525B;
    --text-3:      #8B8B96;
    --green:       #0D7A5B;
    --red:         #C0392B;
    --amber:       #B45309;
    --sidebar-w:   220px;
}

/* ── Sidebar ─────────────────────────────────────────────────── */
.sidebar {
    width: var(--sidebar-w);
    min-height: 100vh;
    background: #16161E;
    color: #fff;
    display: flex;
    flex-direction: column;
    position: fixed;
    top: 0; left: 0; bottom: 0;
    overflow-y: auto;
    z-index: 100;
    border-right: 1px solid #2A2A36;
}
.sidebar-brand {
    padding: 12px 14px 10px;
    border-bottom: 1px solid #2A2A36;
}
.sidebar-brand .logo-text {
    font-size: 0.8rem;
    font-weight: 700;
    color: #A100FF;
    letter-spacing: 0.2px;
    display: flex;
    align-items: center;
    gap: 7px;
}
.sidebar-brand .logo-sub {
    font-size: 0.6rem;
    color: rgba(255,255,255,0.32);
    text-transform: uppercase;
    letter-spacing: 1.1px;
    margin-top: 3px;
    padding-left: 20px;
}
.sidebar-nav { padding: 6px 0; flex: 1; }
.nav-home {
    display: flex; align-items: center; gap: 8px;
    padding: 7px 14px;
    color: rgba(255,255,255,0.88);
    text-decoration: none;
    font-weight: 600;
    font-size: 0.74rem;
    border-left: 2px solid #A100FF;
    background: rgba(161,0,255,0.10);
    margin-bottom: 4px;
}
.nav-home:hover { background: rgba(161,0,255,0.17); }
.cluster-label {
    padding: 8px 14px 3px;
    font-size: 0.59rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    opacity: 0.42;
    margin-top: 4px;
}
.nav-agent {
    display: flex; align-items: center; gap: 7px;
    padding: 5px 14px 5px 20px;
    color: rgba(255,255,255,0.62);
    text-decoration: none;
    font-size: 0.73rem;
    transition: background 0.1s;
    position: relative;
}
.nav-agent:hover { background: rgba(255,255,255,0.05); color: rgba(255,255,255,0.88); }
.nav-agent.active {
    color: #fff; font-weight: 600;
    border-left: 2px solid #A100FF;
    padding-left: 18px;
    background: rgba(161,0,255,0.08);
}
.nav-agent .soon-tag {
    margin-left: auto;
    font-size: 0.54rem;
    background: rgba(255,255,255,0.07);
    color: rgba(255,255,255,0.32);
    padding: 1px 5px;
    border-radius: 2px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.nav-agent.coming-soon { opacity: 0.38; }
.nav-icon {
    width: 13px; height: 13px;
    flex-shrink: 0; vertical-align: middle;
    stroke-width: 1.7;
    display: inline-block;
}

/* ── Main content ────────────────────────────────────────────── */
.main {
    margin-left: var(--sidebar-w);
    flex: 1; display: flex; flex-wrap: nowrap;
    height: 100vh; min-width: 720px; overflow: hidden;
}
.main-content {
    flex: 1 1 auto; min-width: 0;
    padding: 14px 20px 34px;
    overflow-y: auto; overflow-x: hidden;
    display: flex; flex-direction: column; gap: 10px;
    scrollbar-gutter: stable;
    padding-top: calc(40px + 14px);
}

/* ── Top bar ─────────────────────────────────────────────────── */
.topbar {
    position: fixed;
    top: 0; left: var(--sidebar-w); right: 0; height: 40px;
    background: #16161E;
    border-bottom: 1px solid #2A2A36;
    display: flex; align-items: center; justify-content: flex-end;
    padding: 0 14px; z-index: 110; gap: 8px;
}
.topbar-avatar-wrap { position: relative; display: flex; align-items: center; }
.topbar-avatar {
    width: 28px; height: 28px; border-radius: 3px;
    background: rgba(161,0,255,0.14);
    border: 1px solid rgba(161,0,255,0.38);
    color: rgba(255,255,255,0.78); cursor: pointer;
    display: flex; align-items: center; justify-content: center;
    padding: 0; outline: none; transition: background 0.12s;
}
.topbar-avatar svg, .topbar-avatar i { width: 14px; height: 14px; }
.topbar-avatar:hover { background: rgba(161,0,255,0.26); }
.topbar-avatar-tooltip {
    position: absolute; right: 34px; top: 50%; transform: translateY(-50%);
    background: #1A1A22; color: #fff;
    font-size: 0.66rem; font-weight: 600;
    padding: 3px 8px; border-radius: 3px;
    white-space: nowrap; pointer-events: none;
    opacity: 0; transition: opacity 0.12s;
    border: 1px solid #2A2A36;
}
.topbar-avatar-wrap:hover .topbar-avatar-tooltip { opacity: 1; }
.workspace-dropdown {
    position: absolute; top: calc(100% + 6px); right: 0;
    background: var(--surface);
    border: 1px solid var(--border); border-radius: 3px;
    box-shadow: 0 4px 14px rgba(0,0,0,0.13);
    min-width: 158px; overflow: hidden; z-index: 9999; display: none;
}
.workspace-dropdown.open { display: block; }
.ws-item {
    display: flex; align-items: center; gap: 8px;
    padding: 8px 13px;
    color: var(--text-1); text-decoration: none;
    font-size: 0.76rem; font-weight: 500;
    transition: background 0.1s;
    border-bottom: 1px solid var(--border);
}
.ws-item:last-child { border-bottom: none; }
.ws-item:hover { background: var(--surface-2); color: var(--accent); }
.ws-icon { display: flex; align-items: center; color: var(--text-3); width: 14px; }
.ws-icon i { width: 13px; height: 13px; }
@media (max-width: 760px) {
    .topbar { left: 0; }
    .main-content { padding-top: calc(40px + 12px); }
}

/* ── Cards & generic sections ────────────────────────────────── */
.page-shell { display: grid; gap: 10px; }
.page-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 12px 14px;
}
.page-title { font-size: 0.78rem; font-weight: 700; color: var(--text-1); margin-bottom: 4px; }
.page-subtitle { font-size: 0.72rem; color: var(--text-2); margin-bottom: 8px; }
.page-grid { display: grid; gap: 8px; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); }

.info-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 10px 12px;
}
.info-card .label {
    font-size: 0.66rem; text-transform: uppercase;
    letter-spacing: 0.7px; color: var(--text-3); font-weight: 600;
}
.info-card .value {
    font-size: 0.86rem; font-weight: 600;
    color: var(--text-1); margin-top: 3px;
}

/* ── KPI strip ───────────────────────────────────────────────── */
.kpi-strip {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    grid-template-rows: auto auto;
    gap: 8px;
}
.kpi-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 2px solid var(--accent);
    border-radius: 4px;
    padding: 10px 12px 12px;
}
.kpi-card.risk    { border-left-color: var(--red); }
.kpi-card.agility { border-left-color: var(--green); }
.kpi-label { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.7px; color: var(--text-3); font-weight: 600; }
.kpi-value { font-size: 1.3rem; font-weight: 700; color: var(--text-1); margin-top: 3px; }
.kpi-unit  { font-size: 0.68rem; color: var(--text-3); margin-top: 1px; }

/* ── AI summary ──────────────────────────────────────────────── */
.ai-summary {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 2px solid var(--accent);
    border-radius: 4px;
    padding: 10px 14px;
    display: flex; gap: 10px; align-items: flex-start;
}
.ai-summary .ai-icon { color: var(--accent); flex-shrink: 0; display: flex; align-items: flex-start; padding-top: 1px; }
.ai-summary .ai-icon i { width: 13px; height: 13px; }
.ai-summary .ai-label { font-size: 0.63rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.7px; color: var(--accent); }
.ai-summary .ai-text  { font-size: 0.78rem; color: var(--text-1); line-height: 1.55; margin-top: 2px; }
.ai-loading { color: var(--text-3); font-style: italic; font-size: 0.76rem; margin-top: 2px; }

/* ── Section card ────────────────────────────────────────────── */
.section-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    overflow: hidden;
}
.section-card-header {
    padding: 7px 12px;
    font-size: 0.68rem; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.7px;
    color: var(--text-2);
    border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 6px;
    background: var(--surface-2);
}

/* ── Tables ─────────────────────────────────────────────────── */
.log-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.75rem;
}
.log-table th, .log-table td {
    padding: 6px 10px;
    border-bottom: 1px solid var(--border);
    text-align: left;
    vertical-align: top;
}
.log-table thead th {
    background: var(--surface-2);
    color: var(--text-3);
    font-size: 0.64rem;
    text-transform: uppercase;
    letter-spacing: 0.65px;
    font-weight: 600;
}
.log-table tbody tr:hover { background: #FAFAFA; }

/* ── Alerts ──────────────────────────────────────────────────── */
.alert-row {
    display: flex; align-items: flex-start; gap: 10px;
    padding: 7px 12px;
    border-bottom: 1px solid var(--border);
    font-size: 0.75rem;
}
.alert-row:last-child { border-bottom: none; }
.sev-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; margin-top: 4px; }
.sev-high   { background: var(--red); }
.sev-medium { background: var(--amber); }
.sev-low    { background: var(--green); }
.alert-msg { flex: 1; color: var(--text-1); line-height: 1.4; }
.alert-ts  { font-size: 0.64rem; color: var(--text-3); white-space: nowrap; }

/* ── Badge / pill ─────────────────────────────────────────────── */
.badge-pill {
    display: inline-flex; align-items: center; gap: 4px;
    padding: 2px 6px; border-radius: 2px;
    background: rgba(13,122,91,0.10); color: var(--green);
    font-size: 0.64rem; font-weight: 600;
}

/* ── Agent grid ─────────────────────────────────────────────── */
.cluster-section { margin-bottom: 6px; }
.cluster-section h3 {
    font-size: 0.64rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1.1px; margin-bottom: 8px; padding-left: 2px;
    color: var(--text-3);
}
.agent-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(175px, 1fr)); gap: 8px; }
.agent-tile {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 2px solid;
    border-radius: 4px;
    padding: 11px 13px;
    text-decoration: none;
    color: var(--text-1);
    display: flex; flex-direction: column; gap: 5px;
    transition: box-shadow 0.1s, border-color 0.1s;
    position: relative;
}
.agent-tile:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.07); border-color: var(--border-mid); }
.agent-tile.coming-soon { opacity: 0.42; pointer-events: none; }
.agent-tile .tile-icon { display: flex; color: var(--text-2); }
.agent-tile .tile-icon i { width: 16px; height: 16px; }
.agent-tile .tile-name { font-size: 0.76rem; font-weight: 600; line-height: 1.3; }
.agent-tile .tile-tag  { font-size: 0.67rem; color: var(--text-3); line-height: 1.4; }
.tile-status {
    position: absolute; top: 7px; right: 7px;
    font-size: 0.54rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; padding: 1px 5px; border-radius: 2px;
}
.status-live { background: rgba(13,122,91,0.10); color: var(--green); }
.status-soon { background: var(--surface-2); color: var(--text-3); }

/* ── Agent detail header ─────────────────────────────────────── */
.agent-detail-header {
    border-radius: 4px; padding: 14px 18px; margin-bottom: 12px;
    color: #fff; display: flex; align-items: center; gap: 13px;
    background: #16161E !important;
    border: 1px solid #2A2A36;
}
.coming-soon-box {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 36px 28px; text-align: center;
}
.coming-soon-box .cs-icon { margin-bottom: 10px; color: var(--text-3); display: flex; justify-content: center; }
.coming-soon-box .cs-icon i { width: 26px; height: 26px; }
.coming-soon-box h2 { font-size: 0.9rem; font-weight: 700; margin-bottom: 5px; }
.coming-soon-box p  { font-size: 0.76rem; color: var(--text-2); max-width: 340px; margin: 0 auto; }

/* ── Page header (Control Tower) ─────────────────────────────── */
.page-header {
    background: #16161E;
    color: #fff;
    border-radius: 4px;
    padding: 14px 18px;
    display: flex; align-items: center; gap: 13px;
    border: 1px solid #2A2A36;
}
.page-header .header-icon { display: flex; color: var(--accent); }
.page-header .header-icon i { width: 20px; height: 20px; }
.page-header h1 { font-size: 1rem; font-weight: 700; }
.page-header .header-sub { font-size: 0.73rem; color: rgba(255,255,255,0.48); margin-top: 2px; }
.header-pills { display:flex; gap:5px; flex-wrap:wrap; margin-top: 5px; }
.pill {
    display:inline-flex; align-items:center; gap:4px;
    padding: 2px 7px; border-radius: 2px;
    font-size: 0.63rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.5px; background: rgba(255,255,255,0.08); color: rgba(255,255,255,0.78);
}
.pill.live { background: rgba(13,122,91,0.18); color: #4ade80; }
.accent-bar { height: 2px; border-radius: 1px; margin-bottom: 1px; }
.role-switcher {
    display: inline-flex; align-items: center; gap: 6px;
    background: rgba(255,255,255,0.07);
    border: 1px solid rgba(255,255,255,0.13);
    border-radius: 3px; padding: 4px 8px;
    font-size: 0.69rem; font-weight: 600; color: #fff;
}
.role-switcher select {
    background: transparent; color: #fff; border: 0; outline: none;
    font-size: 0.69rem; font-weight: 600;
}
.role-switcher select option { color: var(--text-1); }

/* ── Overview band ───────────────────────────────────────────── */
.overview-band { display:grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 8px; }
.overview-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 10px 12px;
}
.overview-label { font-size: 0.62rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.9px; color: var(--accent); }
.overview-title { font-size: 0.8rem; font-weight: 600; color: var(--text-1); margin-top: 3px; line-height: 1.4; }

/* ── Industry context bar ─────────────────────────────────────── */
.industry-context-bar {
    display: flex; align-items: center; gap: 6px;
    padding: 5px 11px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 2px solid var(--accent);
    border-radius: 3px;
    font-size: 0.73rem; color: var(--text-2);
}
.industry-context-bar strong { color: var(--accent); font-weight: 600; }
.industry-context-bar i { width: 12px; height: 12px; flex-shrink: 0; color: var(--text-3); }

/* ── Role to-do ──────────────────────────────────────────────── */
.role-todo {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 10px 12px;
}
.role-todo-title {
    font-size: 0.68rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.7px; color: var(--text-2); margin-bottom: 7px;
}
.role-todo-list { display: grid; gap: 5px; }
.role-todo-item {
    display: flex; align-items: flex-start; gap: 7px;
    padding: 6px 9px;
    background: var(--surface-2);
    border: 1px solid var(--border); border-radius: 3px;
    font-size: 0.74rem; color: var(--text-1);
}
.role-todo-item .dot {
    width: 6px; height: 6px; border-radius: 50%;
    background: var(--green); margin-top: 5px; flex-shrink: 0;
}
.role-todo-item.completed { border-color: rgba(13,122,91,0.18); }
.task-text { flex: 1; }
.task-meta { margin-top: 2px; font-size: 0.65rem; color: var(--text-3); }
.todo-complete-btn {
    background: var(--text-1); color: #fff; border: 0;
    border-radius: 2px; font-size: 0.65rem; font-weight: 600;
    padding: 3px 8px; cursor: pointer;
}
.todo-complete-btn:hover { background: #2d2d3a; }
.todo-state-badge {
    display: inline-flex; align-items: center; gap: 3px;
    font-size: 0.63rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.4px; color: var(--green); margin-left: 5px;
}
.todo-modal {
    position: fixed; inset: 0; background: rgba(0,0,0,0.4);
    display: none; align-items: center; justify-content: center;
    z-index: 200; padding: 20px;
}
.todo-modal.active { display: flex; }
.todo-modal-card {
    width: min(450px, 100%); background: var(--surface);
    border: 1px solid var(--border); border-radius: 4px;
    padding: 16px; box-shadow: 0 8px 24px rgba(0,0,0,0.14);
}
.todo-modal-title { font-size: 0.83rem; font-weight: 700; color: var(--text-1); margin-bottom: 10px; }
.todo-modal label { display: block; font-size: 0.72rem; font-weight: 600; color: var(--text-2); margin-bottom: 3px; }
.todo-modal textarea, .todo-modal input {
    width: 100%; border: 1px solid var(--border); border-radius: 3px;
    padding: 6px 10px; margin-bottom: 8px;
    font-size: 0.76rem; color: var(--text-1);
    font-family: inherit;
}
.todo-modal-actions { display: flex; justify-content: flex-end; gap: 6px; }
.todo-modal-actions button {
    border: 0; border-radius: 2px; padding: 5px 11px;
    cursor: pointer; font-weight: 600; font-size: 0.71rem;
}
.todo-modal-actions .save-btn   { background: var(--text-1); color: #fff; }
.todo-modal-actions .cancel-btn { background: var(--surface-2); color: var(--text-2); border: 1px solid var(--border); }

/* ── Misc ────────────────────────────────────────────────────── */
.back-link {
    display: inline-flex; align-items: center; gap: 5px;
    color: var(--accent); text-decoration: none; font-size: 0.74rem; font-weight: 600;
    margin-bottom: 10px;
}
.back-link:hover { text-decoration: underline; }
.footer {
    margin-top: 14px; padding-top: 10px;
    border-top: 1px solid var(--border);
    font-size: 0.63rem; color: var(--text-3); text-align: right;
}

@media (max-width: 760px) {
    .sidebar { position: static; width: 100%; min-height: auto; }
    .main { margin-left: 0; flex-direction: column; }
    .main-content { padding: 10px 12px 34px; }
    .page-header { flex-direction: column; align-items: flex-start; }
}

/* ── Bottom tariff ticker ─────────────────────────────────────── */
:root { --sidebar-w: 220px; }
@media (max-width: 760px) { :root { --sidebar-w: 0px; } }
.ticker-wrap {
    position: fixed; bottom: 0;
    left: var(--sidebar-w); right: 0; height: 28px;
    background: #0F0F17;
    border-top: 1px solid #2A2A36;
    z-index: 108;
    display: flex; align-items: center; overflow: hidden;
    font-size: 0.67rem; color: rgba(255,255,255,0.78);
    user-select: none;
}
.ticker-label {
    flex-shrink: 0; padding: 0 10px;
    font-size: 0.55rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1.2px; color: #A100FF;
    border-right: 1px solid #2A2A36;
    height: 100%; display: flex; align-items: center;
    background: #0B0B12; white-space: nowrap; gap: 6px;
}
.ticker-track { flex: 1; overflow: hidden; height: 100%; position: relative; }
.ticker-inner {
    display: inline-flex; align-items: center;
    white-space: nowrap; height: 100%; will-change: transform;
}
.ticker-wrap:hover .ticker-inner { animation-play-state: paused; }
@keyframes ticker-scroll {
    0%   { transform: translateX(0); }
    100% { transform: translateX(var(--tk-to, -50%)); }
}
.ticker-item {
    display: inline-flex; align-items: center; gap: 5px;
    margin-right: 26px; cursor: pointer;
}
.ticker-item:hover { opacity: 0.7; }
.tick-badge {
    font-size: 0.54rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.4px; padding: 1px 5px; border-radius: 2px; flex-shrink: 0;
}
.tick-cleared { background: rgba(13,122,91,0.22);  color: #4ade80; }
.tick-issued  { background: rgba(192,57,43,0.22);  color: #f87171; }
.tick-pending { background: rgba(180,83,9,0.22);   color: #fb923c; }
.tick-dot { width:6px; height:6px; border-radius:50%; border:none; padding:0;
    flex-shrink:0; cursor:pointer; display:inline-block; vertical-align:middle; }
.tick-dot.tick-cleared { background:#4ade80; }
.tick-dot.tick-issued  { background:#f87171; }
.tick-dot.tick-pending { background:#fb923c; }
.ticker-src { color: rgba(255,255,255,0.30); font-style: normal; }
.ticker-sep { color: rgba(161,0,255,0.32); margin: 0 8px; }
.ticker-label-btn { background:none; border:none; color:inherit; font:inherit;
    letter-spacing:inherit; text-transform:inherit; cursor:pointer; padding:0; }
.ticker-label-btn:hover { text-decoration:underline; }

/* ── Ticker live dot ──────────────────────────────────────────── */
.ticker-dot {
    display: inline-block; width: 5px; height: 5px;
    border-radius: 50%; background: #4ade80; flex-shrink: 0;
    animation: tdot-pulse 2.2s ease-in-out infinite;
}
@keyframes tdot-pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.35; }
}

/* ── Ticker feed dialog ────────────────────────────────────────── */
.tf-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.48);
    z-index:600; align-items:center; justify-content:center; }
.tf-overlay.open { display:flex; }
.tf-card { background:var(--surface); border:1px solid var(--border); border-radius:4px;
    width:min(510px,92vw); max-height:78vh; overflow-y:auto;
    box-shadow:0 8px 24px rgba(0,0,0,0.16); }
.tf-header { display:flex; align-items:center; justify-content:space-between;
    padding:9px 14px; border-bottom:1px solid var(--border);
    position:sticky; top:0; background:var(--surface); }
.tf-title { font-size:0.77rem; font-weight:700; color:var(--text-1); }
.tf-close { background:none; border:none; font-size:0.95rem; cursor:pointer;
    color:var(--text-3); padding:3px 7px; line-height:1; border-radius:2px; }
.tf-close:hover { background:var(--surface-2); }
.tf-entry { padding:9px 14px; border-bottom:1px solid var(--border); font-size:0.75rem; }
.tf-entry:last-child { border-bottom:none; }
.tf-illus { margin:5px 0 0; padding:5px 9px; background:#fffbeb;
    border-left:2px solid #d97706; border-radius:0 2px 2px 0;
    font-size:0.67rem; color:#78350f; }

/* ── Right tariff pane ─────────────────────────────────────────── */
.tariff-pane-header {
    padding: 9px 13px; border-bottom: 1px solid var(--border);
    background: var(--surface-2);
}
.tariff-pane-title { font-size: 0.68rem; font-weight: 700; color: var(--accent); text-transform: uppercase; letter-spacing: 0.7px; }
.tariff-pane-subtitle { font-size: 0.63rem; color: var(--text-3); margin-top: 2px; }
.sources-monitor { padding: 7px 11px; border-bottom: 1px solid var(--border); }
.sources-title { font-size: 0.58rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.9px; color: var(--text-3); margin-bottom: 5px; }
.source-row { display: flex; align-items: center; gap: 6px; padding: 3px 0; font-size: 0.71rem; }
.source-icon { display: flex; align-items: center; }
.source-icon i { width: 11px; height: 11px; color: var(--text-3); }
.source-name { flex: 1; color: var(--text-1); }
.source-badge { font-size: 0.57rem; background: rgba(13,122,91,0.10); color: var(--green);
    padding: 1px 5px; border-radius: 2px; font-weight: 600; text-transform: uppercase; }
.tariff-feed { flex: 1; overflow-y: auto; padding: 3px 0; }
.feed-section-label { font-size: 0.58rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.9px; color: var(--text-3); padding: 5px 11px 3px; }
.tariff-event { padding: 7px 11px; border-bottom: 1px solid var(--border); cursor: pointer; transition: background 0.1s; }
.tariff-event:hover { background: var(--surface-2); }
.tariff-event-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 5px; margin-bottom: 2px; }
.tariff-time { font-size: 0.63rem; color: var(--text-3); white-space: nowrap; }
.tariff-status-badge {
    font-size: 0.57rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px;
    padding: 1px 4px; border-radius: 2px; white-space: nowrap;
}
.status-cleared { background: rgba(13,122,91,0.10); color: var(--green); }
.status-issued  { background: rgba(192,57,43,0.10); color: var(--red); }
.status-pending { background: rgba(180,83,9,0.10);  color: var(--amber); }
.tariff-headline { font-size: 0.72rem; font-weight: 600; color: var(--text-1); line-height: 1.3; }
.tariff-detail { font-size: 0.63rem; color: var(--text-3); margin-top: 2px; }
.tariff-source { font-size: 0.6rem; color: var(--text-3); margin-top: 2px; font-style: italic; }
.tariff-pane-footer { padding: 7px 11px; border-top: 1px solid var(--border); font-size: 0.63rem; color: var(--text-3); text-align: center; }
.aggregator-badge {
    display: inline-flex; align-items: center; gap: 3px;
    background: rgba(0,80,179,0.07); color: #0050b3; padding: 1px 6px; border-radius: 2px;
    font-weight: 600; text-transform: uppercase; letter-spacing: 0.3px; font-size: 0.57rem;
}
.pulse-dot {
    display: inline-block; width: 5px; height: 5px; background: var(--green); border-radius: 50%;
    animation: pulse 2s infinite;
}
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.35; } }

/* ── AI Chat Widget ────────────────────────────────────────────── */
.ai-chat-trigger {
    margin: auto 12px 12px;
    background: #A100FF;
    border: none; border-radius: 3px;
    padding: 7px 11px;
    color: #fff; cursor: pointer;
    display: flex; align-items: center; gap: 8px;
    font-size: 0.73rem; font-weight: 600;
    transition: background 0.1s;
    width: calc(100% - 24px); text-align: left;
}
.ai-chat-trigger:hover { background: #8800e0; }
.ai-chat-trigger .ct-icon { display: flex; align-items: center; flex-shrink: 0; }
.ai-chat-trigger .ct-icon i { width: 13px; height: 13px; }
.ai-chat-trigger .ct-label { flex: 1; }
.ai-chat-trigger .ct-label small {
    display: block; font-size: 0.57rem; font-weight: 400;
    color: rgba(255,255,255,0.58); margin-top: 1px;
}
.ai-chat-trigger .ct-soon {
    font-size: 0.53rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; background: rgba(255,255,255,0.14);
    padding: 1px 5px; border-radius: 2px; flex-shrink: 0;
}
.ai-chat-overlay {
    display: none; position: fixed; inset: 0;
    background: rgba(0,0,0,0.48); backdrop-filter: blur(2px);
    z-index: 500; align-items: center; justify-content: center;
}
.ai-chat-overlay.open { display: flex; }
.ai-chat-panel {
    width: 390px; max-width: calc(100vw - 24px);
    height: 510px; max-height: calc(100vh - 50px);
    background: var(--surface);
    border: 1px solid var(--border); border-radius: 4px;
    box-shadow: 0 10px 36px rgba(0,0,0,0.20);
    display: flex; flex-direction: column; overflow: hidden;
    animation: chatSlideIn 0.18s ease;
}
@keyframes chatSlideIn {
    from { opacity: 0; transform: translateY(14px); }
    to   { opacity: 1; transform: translateY(0); }
}
.ai-chat-header {
    background: #16161E;
    padding: 10px 14px 9px;
    display: flex; align-items: center; gap: 9px; flex-shrink: 0;
    border-bottom: 1px solid #2A2A36;
}
.ai-chat-header .ch-avatar {
    width: 28px; height: 28px; border-radius: 3px;
    background: rgba(161,0,255,0.18);
    border: 1px solid rgba(161,0,255,0.38);
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0; color: #A100FF;
}
.ai-chat-header .ch-avatar i { width: 13px; height: 13px; }
.ai-chat-header .ch-title { flex: 1; }
.ai-chat-header .ch-title h3 { font-size: 0.8rem; font-weight: 700; color: #fff; margin: 0; }
.ai-chat-header .ch-title span { font-size: 0.6rem; color: rgba(255,255,255,0.42); }
.ai-chat-close {
    background: rgba(255,255,255,0.07); border: none;
    color: rgba(255,255,255,0.58); width: 24px; height: 24px;
    border-radius: 3px; cursor: pointer; font-size: 0.88rem;
    display: flex; align-items: center; justify-content: center;
    transition: background 0.1s;
}
.ai-chat-close:hover { background: rgba(255,255,255,0.14); color: #fff; }
.ai-chat-messages {
    flex: 1; overflow-y: auto; padding: 12px 11px 6px;
    display: flex; flex-direction: column; gap: 9px;
    background: var(--surface-2);
}
.chat-bubble-row { display: flex; align-items: flex-end; gap: 6px; }
.chat-bubble-row.bot { justify-content: flex-start; }
.chat-bubble-row.user { justify-content: flex-end; }
.cb-avatar {
    width: 21px; height: 21px; border-radius: 3px;
    background: rgba(161,0,255,0.13);
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0; color: #A100FF;
}
.cb-avatar i { width: 11px; height: 11px; }
.chat-bubble {
    max-width: 82%; padding: 7px 11px;
    border-radius: 3px; font-size: 0.76rem; line-height: 1.44;
}
.chat-bubble.bot {
    background: var(--surface); color: var(--text-1);
    border: 1px solid var(--border); border-bottom-left-radius: 0;
}
.chat-bubble.user { background: #A100FF; color: #fff; border-bottom-right-radius: 0; }
.chat-coming-soon-card {
    background: var(--surface); border: 1px solid var(--border);
    border-left: 2px solid var(--accent);
    border-radius: 3px; padding: 11px; font-size: 0.75rem;
}
.chat-coming-soon-card .cc-heading {
    font-weight: 700; color: var(--accent); margin-bottom: 7px;
    display: flex; align-items: center; gap: 5px; font-size: 0.78rem;
}
.chat-coming-soon-card ul {
    margin: 0; padding-left: 15px; color: var(--text-2);
    display: flex; flex-direction: column; gap: 3px;
}
.chat-coming-soon-card .cc-footer {
    margin-top: 9px; padding-top: 7px;
    border-top: 1px solid var(--border);
    font-size: 0.66rem; color: var(--text-3); font-style: italic;
}
.ai-chat-input-bar {
    padding: 9px 11px; background: var(--surface);
    border-top: 1px solid var(--border);
    display: flex; align-items: center; gap: 7px; flex-shrink: 0;
}
.ai-chat-input-bar input {
    flex: 1; border: 1px solid var(--border); border-radius: 3px;
    padding: 6px 11px; font-size: 0.76rem; outline: none;
    background: var(--surface-2); color: var(--text-1);
    transition: border-color 0.12s; font-family: inherit;
}
.ai-chat-input-bar input:focus { border-color: var(--accent); }
.ai-chat-send-btn {
    width: 28px; height: 28px; border-radius: 3px; border: none;
    background: #A100FF; color: #fff;
    display: flex; align-items: center; justify-content: center;
    cursor: pointer; font-size: 0.88rem; flex-shrink: 0;
    transition: background 0.1s;
}
.ai-chat-send-btn:hover { background: #8800e0; }
.ai-chat-typing { display: flex; align-items: center; gap: 4px; padding: 4px 9px; }
.ai-chat-typing span {
    width: 5px; height: 5px; border-radius: 50%;
    background: var(--accent); opacity: 0.38;
    animation: typingPulse 1.2s ease-in-out infinite;
}
.ai-chat-typing span:nth-child(2) { animation-delay: 0.2s; }
.ai-chat-typing span:nth-child(3) { animation-delay: 0.4s; }
@keyframes typingPulse {
    0%, 80%, 100% { opacity: 0.28; transform: scale(0.85); }
    40% { opacity: 1; transform: scale(1.1); }
}

/* ── Rate-source dialog ────────────────────────────────────────── */
.rs-overlay {
    display: none; position: fixed; inset: 0;
    background: rgba(0,0,0,0.42); z-index: 500;
    align-items: center; justify-content: center;
}
.rs-overlay.open { display: flex; }
.rs-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 4px; padding: 18px 20px; max-width: 390px; width: 90%;
    box-shadow: 0 8px 22px rgba(0,0,0,0.14); position: relative;
}
.rs-close {
    position: absolute; top: 10px; right: 13px;
    background: none; border: none; font-size: 1rem;
    color: var(--text-3); cursor: pointer; line-height: 1;
}
.rs-close:hover { color: var(--text-1); }
.rs-title { font-size: 0.83rem; font-weight: 700; color: var(--text-1); margin-bottom: 2px; }
.rs-lane  { font-size: 0.7rem; color: var(--text-3); margin-bottom: 10px; }
.rs-rates { display: flex; gap: 12px; margin-bottom: 10px; }
.rs-rate-box {
    flex: 1; background: var(--surface-2); border: 1px solid var(--border);
    border-radius: 3px; padding: 7px 11px; text-align: center;
}
.rs-rate-label { font-size: 0.59rem; text-transform: uppercase; letter-spacing: 0.7px; color: var(--text-3); margin-bottom: 3px; }
.rs-rate-val { font-size: 1.05rem; font-weight: 700; }
.rs-rate-val.mfn  { color: var(--red); }
.rs-rate-val.pref { color: var(--green); }
.rs-honesty {
    background: var(--surface-2); border: 1px solid var(--border);
    border-radius: 3px; padding: 7px 11px;
    font-size: 0.72rem; color: var(--text-2); line-height: 1.5;
}
.rs-honesty-label {
    font-size: 0.57rem; text-transform: uppercase; letter-spacing: 0.9px;
    font-weight: 700; color: var(--accent); margin-bottom: 3px;
}
.rs-src-badge {
    display: inline-block; margin-top: 7px;
    font-size: 0.61rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.4px; padding: 1px 6px; border-radius: 2px;
    background: rgba(13,122,91,0.10); color: var(--green);
}
.rate-cell-btn {
    background: none; border: none; padding: 0; margin: 0;
    cursor: pointer; font-size: inherit; color: inherit;
    text-align: left; display: inline-flex; align-items: center; gap: 3px;
}
.rate-cell-btn:hover { text-decoration: underline dotted var(--accent); }
.rate-cell-info { font-size: 0.61rem; color: rgba(161,0,255,0.5); }

/* ── Category filter select (topbar) ───────────────────────── */
.cat-filter-wrap {
    display: flex; align-items: center; gap: 6px;
    margin-right: 10px;
}
.cat-filter-label {
    font-size: 0.68rem; font-weight: 700; color: rgba(255,255,255,0.55);
    text-transform: uppercase; letter-spacing: 0.8px; white-space: nowrap;
}
.cat-filter-select {
    appearance: none; -webkit-appearance: none;
    background: rgba(161,0,255,0.18) url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%23A100FF'/%3E%3C/svg%3E") no-repeat right 8px center;
    background-size: 8px 5px;
    border: 1.5px solid rgba(161,0,255,0.45);
    border-radius: 6px;
    color: #fff;
    font-size: 0.76rem; font-weight: 600;
    padding: 5px 26px 5px 10px;
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s;
    min-width: 140px;
}
.cat-filter-select:hover { background-color: rgba(161,0,255,0.30); border-color: #A100FF; }
.cat-filter-select:focus { outline: none; border-color: #A100FF; }
.cat-filter-select option { background: #1a0533; color: #fff; }

/* ── Settings industry picker ───────────────────────────────── */
.settings-section { margin-bottom: 18px; }
.settings-section-title {
    font-size: 0.64rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1.1px; color: var(--accent); margin-bottom: 8px;
}
.settings-ip-item {
    display: flex; align-items: flex-start; gap: 9px;
    padding: 9px 12px;
    border: 1px solid var(--border); border-radius: 3px;
    margin-bottom: 5px; cursor: pointer;
    transition: border-color 0.12s, background 0.12s;
    text-decoration: none; color: var(--text-1);
}
.settings-ip-item:hover { background: var(--accent-dim); border-color: rgba(161,0,255,0.28); }
.settings-ip-item.active { background: var(--accent-dim); border-color: var(--accent); }
.settings-ip-check { font-size: 0.88rem; margin-top: 1px; flex-shrink: 0; }
.settings-ip-name { font-size: 0.78rem; font-weight: 600; }
.settings-ip-desc { font-size: 0.67rem; color: var(--text-3); margin-top: 2px; }

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
        <div class="logo-text"><i data-lucide="globe" class="nav-icon"></i> TradeNavigator AI</div>
        <div class="logo-sub">Powered by Accenture</div>
      </div>
      <div class="sidebar-nav">
        <a href="/" class="nav-home {home_active}"><i data-lucide="layout-dashboard" class="nav-icon"></i> Control Tower</a>
        {cluster_blocks}
      </div>
      <button class="ai-chat-trigger" onclick="openAIChat()">
        <span class="ct-icon"><i data-lucide="bot" class="nav-icon"></i></span>
        <span class="ct-label">
          AI FTA Advisor
          <small>Ask about FTA, tariffs &amp; RoO</small>
        </span>
      </button>
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://unpkg.com/lucide@latest/dist/umd/lucide.min.js"></script>
</head>
<body>
<!-- ── Workspace top bar ──────────────────────────────────────── -->
<div class="topbar">
  {% if topbar_extras is defined %}{{ topbar_extras | safe }}{% endif %}
  <div class="topbar-avatar-wrap">
    <button class="topbar-avatar" id="workspaceBtn" aria-label="Workspace">
      <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <circle cx="12" cy="8" r="4" fill="currentColor"/>
        <path d="M4 20c0-4 3.582-7 8-7s8 3 8 7" fill="currentColor"/>
      </svg>
    </button>
    <span class="topbar-avatar-tooltip">Workspace</span>
    <div class="workspace-dropdown" id="workspaceMenu">
      <a href="/profile" class="ws-item"><span class="ws-icon"><i data-lucide="user"></i></span>Profile</a>
      <a href="/settings" class="ws-item"><span class="ws-icon"><i data-lucide="settings"></i></span>Settings</a>
      <a href="/action-logs" class="ws-item"><span class="ws-icon"><i data-lucide="folder-open"></i></span>Action Logs</a>
    </div>
  </div>
</div>
{{ sidebar | safe }}
<main class="main">
  <div class="main-content">
    {% if industry.name != 'all' %}
    <div class="industry-context-bar"><i data-lucide="factory"></i><strong>{{ industry.display_name }}</strong> — {{ industry.descriptor }}</div>
    {% endif %}
    {{ content | safe }}
    <div class="footer">TradeNavigator AI &mdash; Accenture &copy; 2025</div>
  </div>
</main>
<div class="ticker-wrap" id="tariffTicker">
  <div class="ticker-label">
    <span class="ticker-dot" id="tickerDot" aria-hidden="true"></span>
    <button class="ticker-label-btn" onclick="openTfList()" title="Click to view all live rate entries" aria-label="View live rate feed">Live Rates</button>
  </div>
  <div class="ticker-track" id="tickerTrack">
    <div class="ticker-inner" id="tickerInner"{% if ticker_initial is defined %} style="animation:ticker-scroll 130s linear infinite"{% endif %}>
      {% if ticker_initial is defined %}{{ ticker_initial | safe }}{% else %}<span style="color:rgba(255,255,255,0.35);font-style:italic;padding-left:16px">Loading tariff intelligence&hellip;</span>{% endif %}
    </div>
  </div>
</div>

<!-- ── Ticker feed dialog ──────────────────────────────────────── -->
<div class="tf-overlay" id="tfOverlay" onclick="if(event.target===this)closeTfDialog()">
  <div class="tf-card">
    <div class="tf-header">
      <div class="tf-title" id="tfTitle">Live Rate Feed</div>
      <button class="tf-close" onclick="closeTfDialog()" aria-label="Close">&#x2715;</button>
    </div>
    <div id="tfBody"></div>
  </div>
</div>
{{ scripts | safe }}

<!-- ── Rate source dialog ──────────────────────────────────────── -->
<div class="rs-overlay" id="rsOverlay" onclick="if(event.target===this)closeRsDialog()">
  <div class="rs-card">
    <button class="rs-close" onclick="closeRsDialog()" aria-label="Close">&#x2715;</button>
    <div class="rs-title" id="rsTitle">Rate Source</div>
    <div class="rs-lane" id="rsLane"></div>
    <div class="rs-rates">
      <div class="rs-rate-box">
        <div class="rs-rate-label">MFN Tariff</div>
        <div class="rs-rate-val mfn" id="rsMfn">—</div>
      </div>
      <div class="rs-rate-box">
        <div class="rs-rate-label">Preferential</div>
        <div class="rs-rate-val pref" id="rsPref">—</div>
      </div>
    </div>
    <div class="rs-honesty">
      <div class="rs-honesty-label">&#x1F50D; Data Honesty</div>
      <div id="rsHonesty"></div>
      <div class="rs-src-badge" id="rsBadge"></div>
    </div>
  </div>
</div>

<!-- ── AI Assistant Chat Modal ──────────────────────────────────── -->
<div class="ai-chat-overlay" id="ai-chat-overlay" onclick="closeAIChatOverlay(event)">
  <div class="ai-chat-panel">

    <div class="ai-chat-header">
      <div class="ch-avatar"><i data-lucide="bot"></i></div>
      <div class="ch-title">
        <h3>AI Trade Assistant</h3>
        <span>Powered by TradeNavigator AI</span>
      </div>
      <button class="ai-chat-close" onclick="closeAIChat()" title="Close">&#x2715;</button>
    </div>

    <div class="ai-chat-messages" id="chatMessages">
      <div class="chat-bubble-row bot">
        <div class="cb-avatar"><i data-lucide="bot"></i></div>
        <div class="chat-bubble bot">
          Hi! I&#39;m your <strong>FTA Advisor</strong>. Ask me anything about
          Free Trade Agreements, Rules of Origin, preferential tariffs, or
          trade compliance — I&#39;ll help you navigate it.
        </div>
      </div>
    </div>

    <div class="ai-chat-input-bar">
      <input type="text" id="chatInput"
             placeholder="Ask about FTA eligibility, RoO, tariff rates…"
             autocomplete="off">
      <button class="ai-chat-send-btn" id="chatSend" title="Send">&#x2191;</button>
    </div>

  </div>
</div>

<script>
(function() {
  function openAIChat() {
    document.getElementById('ai-chat-overlay').classList.add('open');
  }
  function closeAIChat() {
    document.getElementById('ai-chat-overlay').classList.remove('open');
  }
  function closeAIChatOverlay(e) {
    if (e.target === e.currentTarget) closeAIChat();
  }
  // Expose to global scope so inline onclick="" handlers can reach them.
  window.openAIChat = openAIChat;
  window.closeAIChat = closeAIChat;
  window.closeAIChatOverlay = closeAIChatOverlay;

  // ── FTA Chat ──────────────────────────────────────────────────────────────
  function _appendMsg(role, html) {
    var box = document.getElementById('chatMessages');
    if (!box) return;
    var row = document.createElement('div');
    row.className = 'chat-bubble-row ' + (role === 'bot' ? 'bot' : 'user');
    if (role === 'bot') {
      row.innerHTML = '<div class="cb-avatar"><svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="10" x="3" y="11" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/><path d="M8 15h.01"/><path d="M16 15h.01"/></svg></div>'
        + '<div class="chat-bubble bot">' + html + '</div>';
    } else {
      row.innerHTML = '<div class="chat-bubble user">' + html + '</div>';
    }
    box.appendChild(row);
    box.scrollTop = box.scrollHeight;
    return row;
  }

  function _appendTyping() {
    var box = document.getElementById('chatMessages');
    if (!box) return null;
    var row = document.createElement('div');
    row.className = 'chat-bubble-row bot';
    row.innerHTML = '<div class="cb-avatar">🤖</div>'
      + '<div class="chat-bubble bot"><div class="ai-chat-typing">'
      + '<span></span><span></span><span></span></div></div>';
    box.appendChild(row);
    box.scrollTop = box.scrollHeight;
    return row;
  }

  function _escapeHtml(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
            .replace(/\\n/g,'<br>');
  }

  function sendChatMessage() {
    var inp = document.getElementById('chatInput');
    var btn = document.getElementById('chatSend');
    if (!inp) return;
    var msg = inp.value.trim();
    if (!msg) return;
    inp.value = '';
    _appendMsg('user', _escapeHtml(msg));
    if (btn) btn.disabled = true;
    var typing = _appendTyping();
    fetch('/api/fta-chat', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({message: msg})
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (typing) typing.remove();
      _appendMsg('bot', _escapeHtml(data.reply || '(no response)'));
    })
    .catch(function() {
      if (typing) typing.remove();
      _appendMsg('bot', 'Connection error — please try again.');
    })
    .finally(function() {
      if (btn) btn.disabled = false;
      if (inp) inp.focus();
    });
  }
  window.sendChatMessage = sendChatMessage;

  var _chatInp = document.getElementById('chatInput');
  var _chatBtn = document.getElementById('chatSend');
  if (_chatInp) _chatInp.addEventListener('keydown', function(e) {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChatMessage(); }
  });
  if (_chatBtn) _chatBtn.addEventListener('click', sendChatMessage);

  // Workspace avatar dropdown
  (function() {
    var btn  = document.getElementById('workspaceBtn');
    var menu = document.getElementById('workspaceMenu');
    if (!btn || !menu) return;
    btn.addEventListener('click', function(e) {
      e.stopPropagation();
      menu.classList.toggle('open');
    });
    document.addEventListener('click', function(e) {
      if (!btn.contains(e.target) && !menu.contains(e.target)) {
        menu.classList.remove('open');
      }
    });
  })();

  // Rate source info dialog
  var _RS_MSG = {
    'aggregator':         "Both MFN and FTA preferential rates sourced live from the USITC Harmonized Tariff Schedule via a configured connector. Rates reflect today's query.",
    'aggregator_mfn_only':'MFN rate sourced live from the USITC HTS. No FTA preferential rate is available — either no trade agreement covers this origin/destination pair, or the lane is outside connector scope. Savings shown as — (uncomputable without a preferential rate).',
    'no_aggregator_data': 'No live rate data was returned for this lane. The aggregator connector found no matching schedule entry. Rates are unavailable until a connector is configured for this destination.',
    'upload':             'MFN and preferential rates come from your uploaded file (columns MFN_RATE / PREF_RATE). These values are not cross-checked against a live tariff schedule.',
    'pending':            'Rate lookup is in progress. The aggregator is being queried for this lane — refresh to see updated values.'
  };
  function openRsDialog(el) {
    var src  = el.dataset.src  || 'pending';
    var mfn  = el.dataset.mfn;
    var pref = el.dataset.pref;
    var fta  = el.dataset.fta  || '—';
    var lane = el.dataset.lane || '';
    document.getElementById('rsTitle').textContent   = 'Rate Data — ' + fta;
    document.getElementById('rsLane').textContent    = lane;
    document.getElementById('rsMfn').textContent     = mfn  ? mfn  + '%' : '—';
    document.getElementById('rsPref').textContent    = pref ? pref + '%' : '—';
    document.getElementById('rsHonesty').textContent = _RS_MSG[src] || 'Source: ' + src;
    document.getElementById('rsBadge').textContent   = src.replace(/_/g,' ').toUpperCase();
    document.getElementById('rsOverlay').classList.add('open');
  }
  function closeRsDialog() {
    document.getElementById('rsOverlay').classList.remove('open');
  }
  window.openRsDialog  = openRsDialog;
  window.closeRsDialog = closeRsDialog;

  // ESC key closes all overlays
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      closeAIChat();
      closeRsDialog();
      closeTfDialog();
      var menu = document.getElementById('workspaceMenu');
      if (menu) menu.classList.remove('open');
    }
  });

  // ── Ticker feed dialog ────────────────────────────────────────────
  var _feedData = [];
  var _feedIndustryLabel = 'All Industries';

  function _tfEntryHtml(ev) {
    var illusHtml = ev.illustrative
      ? '<div class="tf-illus">&#x26A0;&#xFE0F; Illustrative — not live data. This entry is from the system-health simulator, not a live tariff connector.</div>'
      : '';
    var detailHtml = (ev.detail && ev.detail !== 'No additional details')
      ? '<div style="font-size:0.78rem;color:#555;line-height:1.5;margin-top:4px">' + ev.detail + '</div>'
      : '';
    return '<div class="tf-entry">'
      + '<div style="display:flex;align-items:center;gap:8px;margin-bottom:5px">'
      + '<button class="tick-dot tick-' + ev.status + '" title="' + ev.status.toUpperCase() + '" aria-label="Status: ' + ev.status + '" style="flex-shrink:0"></button>'
      + '<span style="font-size:0.64rem;color:#aaa">' + ev.timestamp + ' &middot; ' + ev.source + '</span>'
      + '</div>'
      + '<div style="font-size:0.83rem;font-weight:600;color:#1a0533">' + ev.headline + '</div>'
      + detailHtml
      + illusHtml
      + '</div>';
  }

  function openTfEntry(idx) {
    var ev = _feedData[idx];
    if (!ev) return;
    var t = document.getElementById('tfTitle');
    if (t) t.textContent = 'Rate Entry — ' + ev.source;
    var b = document.getElementById('tfBody');
    if (b) b.innerHTML = _tfEntryHtml(ev);
    document.getElementById('tfOverlay').classList.add('open');
  }

  function openTfList() {
    var t = document.getElementById('tfTitle');
    var b = document.getElementById('tfBody');
    if (t) t.textContent = _feedData.length
      ? 'Live Rates — ' + _feedIndustryLabel + ' (' + _feedData.length + ' entries)'
      : 'Live Rates — ' + _feedIndustryLabel;
    if (b) {
      b.innerHTML = _feedData.length
        ? _feedData.map(_tfEntryHtml).join('')
        : '<div style="padding:32px 20px;text-align:center;color:#999;font-size:0.88rem">'
          + '&#x2014; No live rate updates for <strong style="color:#1a0533">'
          + _feedIndustryLabel + '</strong> yet.</div>';
    }
    document.getElementById('tfOverlay').classList.add('open');
  }

  function closeTfDialog() {
    var el = document.getElementById('tfOverlay');
    if (el) el.classList.remove('open');
  }

  window.openTfEntry  = openTfEntry;
  window.openTfList   = openTfList;
  window.closeTfDialog = closeTfDialog;

  // Bind Live Rates label button via addEventListener (backup for inline onclick)
  var _lrBtn = document.querySelector('.ticker-label-btn');
  if (_lrBtn) _lrBtn.addEventListener('click', openTfList);

  // ── Bottom tariff ticker — polls /api/tariff-feed every 10s ───────
  var _tickerCache = '';
  function loadTicker() {
    fetch('/api/tariff-feed', {credentials: 'same-origin'})
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var inner = document.getElementById('tickerInner');
        var track = document.getElementById('tickerTrack');
        if (!inner) return;

        // Specific industry with no aggregator coverage → static notice, no scroll
        if (data.no_industry_coverage) {
          var lbl = data.industry_display_name || data.active_industry || 'this industry';
          _feedIndustryLabel = lbl;
          var noHtml = '<span style="color:rgba(255,255,255,0.45);font-style:italic;padding-left:16px">'
            + '&#x2014; No live rate updates for ' + lbl + ' yet &mdash; coverage expanding.'
            + '</span>';
          if (noHtml === _tickerCache) return;
          _tickerCache = noHtml;
          inner.innerHTML = noHtml;
          inner.style.animation = 'none';
          _feedData = [];
          return;
        }

        if (!data.feed || !data.feed.length) return;
        _feedData = data.feed;
        _feedIndustryLabel = data.industry_display_name || data.active_industry || 'All Industries';

        // Build one copy of ticker items (we'll double for seamless loop)
        var html = '';
        data.feed.forEach(function(ev, idx) {
          html += '<span class="ticker-item" onclick="openTfEntry(' + idx + ')" title="Click for details">'
            + '<button class="tick-dot tick-' + ev.status + '" title="' + ev.status.toUpperCase() + '" aria-label="Status: ' + ev.status + '"></button>'
            + ' ' + ev.headline
            + ' <span class="ticker-src">via ' + ev.source + '</span>'
            + '</span><span class="ticker-sep" aria-hidden="true">&middot;</span>';
        });

        if (html === _tickerCache) return;
        _tickerCache = html;

        // Double the content for seamless infinite loop
        inner.innerHTML = html + html;
        inner.offsetWidth; // force layout before measuring

        var trackW = track ? track.offsetWidth : Math.max(0, window.innerWidth - 260);
        var singleW = Math.ceil(inner.scrollWidth / 2); // width of one copy
        var dur = Math.max(20, Math.round(singleW / 25)); // ~25px/s, min 20s

        document.documentElement.style.setProperty('--tk-to', '-' + singleW + 'px');
        inner.style.animation = 'none';
        inner.offsetWidth; // force reflow to restart animation
        inner.style.animation = 'ticker-scroll ' + dur + 's linear infinite';
      })
      .catch(function(err) {
        var inner = document.getElementById('tickerInner');
        if (inner && !inner.querySelector('.ticker-item'))
          inner.innerHTML = '<span style="color:rgba(255,180,100,0.7);font-style:italic;padding-left:16px">&#9888; Feed temporarily unavailable — retrying</span>';
      });
  }
  loadTicker();
  setInterval(loadTicker, 10000);
  // Initialise Lucide icons after DOM is fully built
  if (window.lucide) lucide.createIcons();
})();
</script>

</body>
</html>"""

# ---------------------------------------------------------------------------
# Industry lens — session helper + route
# ---------------------------------------------------------------------------

def _current_industry() -> dict:
    """Return the active industry from session, falling back to the 'all' default."""
    name = session.get("industry", default_industry()["name"])
    return get_industry_profile(name) or default_industry()


@app.route("/set-industry")
def set_industry():
    """Set the industry lens for this session and redirect back."""
    name = request.args.get("name", "")
    if get_industry_profile(name):          # validates against catalog (includes 'all')
        session["industry"] = name
    next_url = request.args.get("next") or request.referrer or "/"
    return redirect(next_url)


# ---------------------------------------------------------------------------
# Control Tower (home) — renders instantly; AI summary fetched by JS
# ---------------------------------------------------------------------------

@app.route("/")
def control_tower():
    return redirect(url_for("agent_fta_preferential"))

@app.route("/control-tower")
def control_tower_page():
    industry  = _current_industry()
    is_all    = industry.get("name") == "all"
    ind_label = industry.get("display_name", industry.get("name", "all"))
    kpis      = get_kpis()

    if is_all:
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
    else:
        kpi_html = (
            '<div class="kpi-strip">'
            '<div class="kpi-card" style="grid-column:1/-1;display:flex;flex-direction:column;'
            'justify-content:center;align-items:center;text-align:center;'
            'min-height:110px;border-top-color:#bbb;">'
            f'<div class="kpi-label" style="font-size:0.75rem">Portfolio KPIs &mdash; {ind_label}</div>'
            '<div class="kpi-value" style="font-size:1rem;color:#aaa;font-weight:500;margin:10px 0 4px;">'
            'Industry-specific KPIs require client data (ERP integration)</div>'
            '<div class="kpi-unit">Switch to <strong>All Industries</strong> to see aggregated portfolio totals</div>'
            '</div></div>'
        )

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
      <div class="header-icon"><i data-lucide="layout-dashboard"></i></div>
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
    <details style="border-radius:10px;box-shadow:0 2px 8px rgba(26,5,51,0.07);background:#fff;overflow:hidden;margin-top:4px">
      <summary style="padding:14px 20px;font-size:0.78rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #f0eaf8;display:flex;align-items:center;gap:8px;cursor:pointer;list-style:none;-webkit-user-select:none;user-select:none;color:#1a0533">
        &#x1F4E1;&nbsp;Aggregator Data Sources
        <span style="margin-left:auto;font-size:0.7rem;font-weight:400;color:#A100FF;text-transform:none;letter-spacing:0">&#9660; expand</span>
      </summary>
      <div class="sources-monitor" id="sources-container">
        <div class="sources-title">Source Monitoring</div>
      </div>
      <div style="padding:8px 16px 10px;font-size:0.68rem;color:#aaa;border-top:1px solid #f0eaf8">
        <span class="aggregator-badge"><span class="pulse-dot"></span> Aggregator Active</span>
      </div>
    </details>
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
        industry=industry,
        all_industries=get_industries(),
    )


@app.route("/profile")
def profile_page():
    industry = _current_industry()
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
        industry=industry,
        all_industries=get_industries(),
    )


@app.route("/settings")
def settings_page():
    industry = _current_industry()
    ind_items = ""
    for ind in get_industries():
        active_cls = "active" if ind["name"] == industry["name"] else ""
        check_icon = "&#x2713;" if ind["name"] == industry["name"] else "&nbsp;"
        ind_items += (
            f'<a href="/set-industry?name={ind["name"]}&next=/settings%23industry" '
            f'class="settings-ip-item {active_cls}">'
            f'<span class="settings-ip-check">{check_icon}</span>'
            f'<div><div class="settings-ip-name">{ind["display_name"]}</div>'
            f'<div class="settings-ip-desc">{ind.get("descriptor","")}</div></div>'
            f'</a>'
        )
    content = f"""
    <div class="page-shell">
      <div class="page-card">
        <div class="page-title">Settings</div>
        <div class="page-subtitle">Tune how the control tower surfaces advice, tasks, and alerts.</div>

        <div class="settings-section" id="industry">
          <div class="settings-section-title">Industry Lens</div>
          <p style="font-size:0.8rem;color:#888;margin:0 0 14px">
            Filters all views to a specific industry. Data shown will only include shipments
            and rates matching the selected scope.
          </p>
          {ind_items}
        </div>

        <div class="settings-section">
          <div class="settings-section-title">Alerts &amp; Notifications</div>
          <div class="page-grid">
            <div class="info-card"><div class="label">Alert cadence</div><div class="value">Every 10 min</div></div>
            <div class="info-card"><div class="label">Auto-escalation</div><div class="value">Enabled</div></div>
            <div class="info-card"><div class="label">Notifications</div><div class="value">Slack + email</div></div>
          </div>
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
        industry=industry,
        all_industries=get_industries(),
    )


@app.route("/action-logs")
def action_logs_page():
    industry = _current_industry()
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
        industry=industry,
        all_industries=get_industries(),
    )


# ---------------------------------------------------------------------------
# FTA AI chat endpoint
# ---------------------------------------------------------------------------

def _build_fta_chat_context() -> str:
    """
    Assemble a compact plain-text snapshot of current dashboard data for the
    FTA chatbot system prompt.  Caps lane list at 6 rows.  Returns a fallback
    notice on any error so the chatbot degrades gracefully.
    """
    try:
        kpis  = fta_data_source.get_fta_kpis()
        lanes = fta_data_source.get_fta_lanes()
        ships = fta_data_source.get_fta_shipments()
        coos  = fta_data_source.get_coo_requests()
        src   = fta_data_source.get_source_info()
    except Exception:
        return "(Dashboard data temporarily unavailable — use general knowledge only.)"

    if src.get("shipment_mode") != "uploaded":
        return (
            "No shipment data uploaded yet — dashboard is in demo mode. "
            "For data-specific questions, advise the user to upload their FTA shipment "
            "data first. You can still answer general FTA knowledge questions."
        )

    period = kpis.get("period_label", "Uploaded data")
    total  = len(ships)

    # Claimed-status breakdown (E = eligible/claimed, U = unclaimed, N = not eligible)
    n_e = sum(1 for s in ships if s.get("claimed_status") == "E")
    n_u = sum(1 for s in ships if s.get("claimed_status") == "U")
    n_n = sum(1 for s in ships if s.get("claimed_status") == "N")

    # RoO breakdown derived per shipment row
    has_roo = src.get("has_roo", False)
    if has_roo:
        roo_q = sum(1 for s in ships if s.get("roo_status") == "Q")
        roo_m = sum(1 for s in ships if s.get("roo_status") == "M")
        roo_f = sum(1 for s in ships if s.get("roo_status") == "F")
        roo_str = f"{roo_q} Qualified, {roo_m} Near-Miss, {roo_f} Fail"
    else:
        roo_str = "RVC columns not in uploaded data (RoO thresholds unavailable)"

    # CoO pipeline breakdown
    coo_counts: dict = {"OVERDUE": 0, "PENDING": 0, "RECEIVED": 0, "VALIDATED": 0}
    for c in coos:
        st = c.get("status", "")
        if st in coo_counts:
            coo_counts[st] += 1

    # Top 6 lanes by unclaimed savings
    top_lanes = sorted(
        lanes,
        key=lambda ln: float(ln.get("unclaimed_savings_k") or 0),
        reverse=True,
    )[:6]
    lane_lines = [
        (
            f"  - {ln.get('fta_name', '?')} | {ln.get('origin', '?')}"
            f" -> {ln.get('destination', '?')}"
            f" | eligible ${float(ln.get('eligible_value_m') or 0):.2f}M"
            f" | claimed ${float(ln.get('claimed_value_m') or 0):.2f}M"
            f" | util {float(ln.get('utilization_pct') or 0):.1f}%"
            f" | unclaimed ${float(ln.get('unclaimed_savings_k') or 0):.1f}K"
        )
        for ln in top_lanes
    ]

    ctx = [
        f"Period: {period} | Shipments loaded: {total}",
        "Dashboard KPIs:",
        f"  - FTA utilization rate: {float(kpis.get('utilization_pct') or 0):.1f}%",
        f"  - Unclaimed savings opportunity: ${float(kpis.get('unclaimed_opportunity_m') or 0):.3f}M",
        f"  - CoOs outstanding: {kpis.get('coo_outstanding', 0)}",
        f"Shipment breakdown: {n_e} claimed/eligible (E) | {n_u} unclaimed (U) | {n_n} not eligible (N)",
        f"RoO assessments: {roo_str}",
        (
            f"CoO requests: {coo_counts['OVERDUE']} overdue"
            f" | {coo_counts['PENDING']} pending"
            f" | {coo_counts['RECEIVED']} received"
            f" | {coo_counts['VALIDATED']} validated"
        ),
    ]
    if lane_lines:
        ctx.append("Lanes sorted by unclaimed savings (highest first):")
        ctx.extend(lane_lines)
    else:
        ctx.append("Lanes: none derived yet.")

    return "\n".join(ctx)


@app.route("/api/fta-chat", methods=["POST"])
def api_fta_chat():
    data    = request.get_json(force=True) or {}
    message = data.get("message", "").strip()
    if not message:
        return jsonify({"reply": "Please enter a question."})

    # ── Build current-data context for this request ───────────────────────────
    dashboard_ctx = _build_fta_chat_context()

    # ── System prompt ─────────────────────────────────────────────────────────
    system = (
        "You are the FTA Advisor inside TradeNavigator AI, an Accenture"
        " trade-intelligence platform. You are a specialist in Free Trade"
        " Agreements and preferential-trade compliance.\n\n"
        "YOUR EXPERTISE:\n"
        "- FTA agreements and tariff schedules: USMCA/CUSMA, EVFTA, RCEP, KORUS,"
        "  CPTPP, CAFTA-DR, AUSFTA, GSP, and major bilateral US and EU FTAs\n"
        "- Rules of Origin: regional value content (RVC/QVC), tariff-shift (CTH/CTSH),"
        "  substantial transformation, wholly-obtained, cumulation, de minimis,"
        "  product-specific rules (PSRs), and how to close compliance gaps\n"
        "- Duty optimisation: MFN vs preferential rates, duty savings calculation,"
        "  retroactive/prior-period FTA claims, first-sale valuation, tariff engineering\n"
        "- Certificates of Origin and Proof of Origin: USMCA self-certification, EUR.1,"
        "  Form AK, back-to-back COOs, approved-exporter schemes, supplier declarations,"
        "  documentation timelines and retention requirements\n"
        "- HS tariff classification as it relates to FTA eligibility, chapter notes,"
        "  advance classification rulings, tariff-heading change rules\n"
        "- Trade compliance: customs entry, binding rulings, Section 301 / 232 / AD / CVD"
        "  remedies and their interaction with FTA preferences, audit risk\n"
        "- Dashboard metrics: utilization rate, unclaimed opportunity, near-miss products,"
        "  CoO pipeline status, RoO qualification by product, lane-level savings\n\n"
        "CURRENT DASHBOARD DATA (use these exact figures for any data question):\n"
        f"{dashboard_ctx}\n\n"
        "RULES YOU MUST FOLLOW:\n"
        "1. SCOPE: You answer ONLY questions about FTAs, preferential trade, customs"
        "   duties, rules of origin, HS codes, trade compliance, and the dashboard"
        "   data above. If the user asks about anything else — coding, general knowledge,"
        "   news, weather, opinions, or topics unrelated to trade — respond with:\n"
        "   I'm focused on FTA and preferential trade topics. I can help with questions"
        "   about trade agreements, rules of origin, duty savings, or the data on this"
        "   dashboard. What would you like to know?\n"
        "   Redirect politely every time; do not answer off-topic questions.\n"
        "2. DATA ACCURACY: When answering about the user's dashboard, use ONLY the"
        "   numbers from CURRENT DASHBOARD DATA. Never invent figures. If a specific"
        "   detail is absent from the data, say so clearly.\n"
        "3. DEPTH: Give accurate, practical answers. Explain the why, not just the what."
        "   Cite specific agreement names, article numbers, or rule types where helpful.\n"
        "4. FORMAT: Plain text with line breaks. Use - prefix for bullet lists."
        "   Do not use markdown bold or italic markers. Keep responses focused.\n"
        "5. NO REASONING OUTPUT: Respond directly. Do not emit thinking steps or"
        "   chain-of-thought text."
    )

    # ── Call LLM ──────────────────────────────────────────────────────────────
    try:
        reply = _run_ai_in_thread(system, message)
        reply = _strip_reasoning(reply)
        if not reply:
            raise ValueError("empty reply from LLM")
    except Exception:
        reply = (
            "I'm having trouble reaching the AI service right now. "
            "For a quick reference: check the Unclaimed Savings column in the"
            " lane table for your highest-value FTA opportunities, or try again"
            " in a moment."
        )
    return jsonify({"reply": reply})


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
    compliance_note   = data.get("compliance_note", "")    # from RoO assessment

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

    _action_verb = "qualifies for" if ro_status == "Qualified" else "does not qualify for" if ro_status == "Fail" else "is assessed under"
    _note_clause = (
        f" System compliance note for this product: {compliance_note}"
        if compliance_note else ""
    )
    user_prompt = (
        f"Assess whether shipment {shipment_id} ({product}, HS {hs_code}) from "
        f"{origin} to {destination} {_action_verb} {fta_name} preferential treatment. "
        f"Rules-of-origin: {roo_sentence} "
        f"Estimated duty saving if qualified: ${est_saving_k}K.{_note_clause} "
        f"State the recommended immediate action. Tone: actionable, CFO-ready."
    )

    try:
        raw  = _run_ai_in_thread(system_prompt, user_prompt)
        text = _strip_reasoning(raw)
        if not text:
            raise ValueError("empty response")
    except Exception:
        if ro_status == "Near-Miss":
            try:
                gap = round(float(rvc_threshold_pct) - float(rvc_pct), 1)
            except Exception:
                gap = "?"
            text = (
                f"TOR_ID {shipment_id} ({product}, CCNGN {hs_code}) has RVC_PCT {rvc_pct}%, "
                f"falling {gap} pts short of the {rvc_threshold_pct}% RVC_THRESHOLD for {fta_name}. "
                f"A targeted sourcing adjustment or supplier invoice restructure could close the gap. "
                f"The ${est_saving_k}K duty saving is at risk — request a revised proof-of-origin "
                f"from the supplier within 5 business days."
            )
        elif ro_status == "Qualified":
            text = (
                f"TOR_ID {shipment_id} ({product}, CCNGN {hs_code}) qualifies for {fta_name} "
                f"preferential treatment: RVC_PCT {rvc_pct}% clears the {rvc_threshold_pct}% "
                f"RVC_THRESHOLD. Claiming PREF_STATUS=E recovers an estimated ${est_saving_k}K in duty. "
                f"Immediate action: submit the {fta_name} Certificate of Origin to customs "
                f"within the current entry window."
            )
        elif ro_status == "Fail":
            text = (
                f"TOR_ID {shipment_id} ({product}, CCNGN {hs_code}) does NOT qualify for {fta_name} "
                f"preferential treatment: RVC_PCT {rvc_pct}% is below the {rvc_threshold_pct}% "
                f"RVC_THRESHOLD required. MFN duties apply. "
                f"To improve qualification, increase regional content — review sourcing with the supplier."
            )
        else:
            text = (
                f"TOR_ID {shipment_id} ({product}, CCNGN {hs_code}) — RoO qualification "
                f"cannot be confirmed under {fta_name} ({ro_status or 'status unknown'}). "
                f"Upload a complete BoM and RoO rules file to enable a full assessment."
            )

    return jsonify({"explanation": text})


# ---------------------------------------------------------------------------
# FTA data upload / reset / template routes
# ---------------------------------------------------------------------------

@app.route("/api/fta/upload/shipments", methods=["POST"])
def fta_upload_shipments():
    """
    Dual-mode upload endpoint — works both as a traditional form POST and via fetch().

    Detection: AJAX requests send Accept: application/json.
      AJAX + native format  → JSON {"ok": true, "redirect": ...}
      AJAX + needs mapping  → JSON {"needs_mapping": true, ...}
      AJAX + parse error    → JSON {"ok": false, "error": ...}
      Form POST (no JS)     → redirect after loading (native) or redirect with error msg
    """
    import fta_mapping as _fm

    is_ajax = "application/json" in request.headers.get("Accept", "")
    app.logger.info("[FTA upload] filename=%s  is_ajax=%s",
                    request.files.get("shipment_file", type(None)),
                    is_ajax)

    f = request.files.get("shipment_file")
    if not f or not f.filename:
        app.logger.warning("[FTA upload] No file in request (files=%s)", list(request.files.keys()))
        if is_ajax:
            return jsonify({"ok": False, "error": "No file selected."})
        return redirect(url_for("agent_fta_preferential"))

    app.logger.info("[FTA upload] Received file: %s  size=%s bytes", f.filename, f.seek(0, 2) or f.tell())
    f.seek(0)  # rewind after size check

    try:
        raw_bytes = f.read()
        app.logger.info("[FTA upload] Read %d bytes from %s", len(raw_bytes), f.filename)
        df, columns, samples = fta_data_source.parse_for_mapping(raw_bytes, f.filename)
        app.logger.info("[FTA upload] Parsed OK — %d rows, columns: %s", len(df), columns)
    except Exception as exc:
        app.logger.error("[FTA upload] Parse FAILED: %s", exc)
        if is_ajax:
            return jsonify({"ok": False, "error": f"Could not parse file: {exc}"})
        # Non-AJAX: store the error so the page can show it, then redirect
        import traceback as _tb
        fta_data_source._state["upload_shipment_msg"] = {
            "ok": False, "errors": [f"Could not parse '{f.filename}': {exc}"], "warnings": [],
        }
        return redirect(url_for("agent_fta_preferential"))

    # ── Native SAP format: load directly (works with or without JS) ──────────
    if fta_data_source.is_native_format(columns):
        app.logger.info("[FTA upload] Native SAP format detected — loading directly")
        result = fta_data_source.upload_shipment_data(raw_bytes, f.filename)
        app.logger.info("[FTA upload] upload_shipment_data result: %s", result)
        if is_ajax:
            return jsonify({"ok": result["ok"],
                            "errors": result.get("errors", []),
                            "warnings": result.get("warnings", []),
                            "redirect": url_for("agent_fta_preferential")})
        # Plain form POST — redirect so browser follows to the FTA page
        return redirect(url_for("agent_fta_preferential"))

    # ── Non-native columns — needs mapping ──────────────────────────────────
    app.logger.info("[FTA upload] Non-native columns, needs mapping: %s", columns)

    if not is_ajax:
        # JS is not available — we can't show the mapping modal.
        # Store a message instructing the user to use the SAP template or enable JS.
        fta_data_source._state["upload_shipment_msg"] = {
            "ok": False,
            "errors": [
                f"Your file '{f.filename}' uses non-SAP column names "
                f"({', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}). "
                "JavaScript must be enabled to use the column-mapping dialog, "
                "or download the SAP template and re-upload."
            ],
            "warnings": [],
        }
        return redirect(url_for("agent_fta_preferential"))

    # AJAX path — build mapping suggestions and return JSON
    local_suggestion = _fm.local_map(columns)
    try:
        async def _do_llm():
            return await _fm.llm_map(columns, samples, _call_ai)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(asyncio.run, _do_llm())
            suggested = future.result(timeout=25)
        app.logger.info("[FTA upload] LLM mapping complete")
    except Exception as exc:
        app.logger.warning("[FTA upload] LLM mapping failed (%s), using local fallback", exc)
        suggested = local_suggestion

    sw = _fm.sanity_warnings(suggested, samples)

    field_info: dict = {}
    for row in _FIELD_DICTIONARY:
        sap = row[0]
        field_info[sap] = {
            "univ": row[1],
            "note": row[4],
            "optional": sap in _fm.OPTIONAL_FIELDS,
        }

    key = secrets.token_urlsafe(16)
    _PENDING_UPLOADS[key] = {"df": df, "filename": f.filename}
    if len(_PENDING_UPLOADS) > 10:
        oldest = next(iter(_PENDING_UPLOADS))
        _PENDING_UPLOADS.pop(oldest, None)

    app.logger.info("[FTA upload] Returning needs_mapping JSON, key=%s", key)
    return jsonify({
        "needs_mapping":   True,
        "mapping_key":     key,
        "filename":        f.filename,
        "columns":         columns,
        "samples":         samples,
        "suggested":       suggested,
        "field_info":      field_info,
        "sanity_warnings": sw,
        "required_fields": [r[0] for r in _fm.REQUIRED_FIELDS],
        "optional_fields": list(_fm.OPTIONAL_FIELDS),
    })


@app.route("/api/fta/status")
def fta_status():
    """Diagnostic endpoint — shows current data-source mode and row counts."""
    import fta_data_source as _ds
    src = _ds.get_source_info()
    try:
        lanes = _ds.get_fta_lanes()
        ships = _ds.get_fta_shipments()
        kpis  = _ds.get_fta_kpis()
        # Inspect raw shipment_df for status-column diagnostics
        with _ds._lock:
            _df = _ds._state.get("shipment_df")
        _has_status = _df is not None and "shipment_status" in _df.columns
        _status_vals = (
            sorted(_df["shipment_status"].dropna().unique().tolist())
            if _has_status else []
        )
        _lanes_shipped   = _ds.get_fta_lanes_by_status("shipped")
        _lanes_intransit = _ds.get_fta_lanes_by_status("in_transit")
        return jsonify({
            "source":                src,
            "lane_count":            len(lanes),
            "shipment_count":        len(ships),
            "period_label":          kpis.get("period_label", ""),
            "utilization_pct":       kpis.get("utilization_pct", 0),
            "shipment_df_columns":   list(_df.columns) if _df is not None else [],
            "shipment_status_present": _has_status,
            "shipment_status_values":  _status_vals,
            "lanes_shipped_count":   len(_lanes_shipped),
            "lanes_intransit_count": len(_lanes_intransit),
        })
    except Exception as exc:
        return jsonify({"source": src, "error": str(exc)})


@app.route("/api/fta/upload/apply-mapping", methods=["POST"])
def fta_apply_mapping():
    """
    Receive user-confirmed column mapping, apply it to the pending DataFrame,
    validate, derive and load into _state.
    Returns {"ok": true, "redirect": ...} or {"ok": false, "errors": [...]}
    """
    data    = request.get_json(force=True) or {}
    key     = data.get("mapping_key", "")
    mapping = data.get("mapping", {})

    pending = _PENDING_UPLOADS.get(key)
    if not pending:
        return jsonify({
            "ok": False,
            "errors": ["Upload session expired — please re-upload the file."],
        })

    result = fta_data_source.apply_mapping_and_load_shipments(pending["df"], mapping)

    if result["ok"]:
        _PENDING_UPLOADS.pop(key, None)
        return jsonify({
            "ok":       True,
            "warnings": result.get("warnings", []),
            "redirect": url_for("agent_fta_preferential"),
        })

    return jsonify({
        "ok":       False,
        "errors":   result.get("errors", []),
        "warnings": result.get("warnings", []),
    })


@app.route("/api/fta/upload/coo", methods=["POST"])
def fta_upload_coo():
    f = request.files.get("coo_file")
    if f and f.filename:
        fta_data_source.upload_coo_data(f.read(), f.filename)
    return redirect(url_for("agent_fta_preferential"))


@app.route("/api/fta/upload/bom", methods=["POST"])
def fta_upload_bom():
    f = request.files.get("bom_file")
    if f and f.filename:
        fta_data_source.upload_bom_data(f.read(), f.filename)
    return redirect(url_for("agent_fta_preferential"))


@app.route("/api/fta/upload/roo", methods=["POST"])
def fta_upload_roo():
    f = request.files.get("roo_file")
    if f and f.filename:
        fta_data_source.upload_roo_data(f.read(), f.filename)
    return redirect(url_for("agent_fta_preferential"))


@app.route("/api/fta/reset", methods=["POST"])
def fta_reset():
    fta_data_source.reset_to_empty()
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
    if which == "bom":
        return Response(
            BOM_TEMPLATE_CSV,
            mimetype="text/csv",
            headers={"Content-Disposition":
                     "attachment; filename=fta_bom_template.csv"},
        )
    if which == "roo":
        return Response(
            ROO_TEMPLATE_CSV,
            mimetype="text/csv",
            headers={"Content-Disposition":
                     "attachment; filename=fta_roo_rules_template.csv"},
        )
    return ("Not found", 404)


@app.route("/agent/fta_preferential")
def agent_fta_preferential():
    industry = _current_industry()
    kpis            = fta_data_source.get_fta_kpis()
    lanes           = fta_data_source.get_fta_lanes()
    lanes_shipped   = fta_data_source.get_fta_lanes_by_status("shipped")
    lanes_intransit = fta_data_source.get_fta_lanes_by_status("in_transit")
    shipments    = fta_data_source.get_fta_shipments()
    coo_requests = fta_data_source.get_coo_requests()
    roo_items    = fta_data_source.get_roo_assessments()
    roadmap      = fta_data_source.get_qualification_roadmap()
    source       = fta_data_source.get_source_info()
    s_msg, c_msg, b_msg, r_msg = fta_data_source.take_upload_messages()

    # ── Category filter param (URL query string: ?cat=laptops etc.) ─────────
    cat = request.args.get("cat", "all").strip().lower()

    # ── Industry filtering ──────────────────────────────────────────────────
    # Apply BEFORE HTML rendering so KPIs, tables, and empty states all reflect
    # the same filtered view. "all" → no filter (current behavior unchanged).
    # Filtering selects which rows surface; row content (honesty flags) is never modified.
    is_all    = industry.get("name") == "all"
    ind_label = industry.get("display_name", industry.get("name", "All Industries"))
    _is_empty = (source["shipment_mode"] == "empty")   # also re-set in header section below
    _no_industry_match = False   # distinct from _is_empty: data uploaded, none in this industry

    if not is_all and not _is_empty:
        # Lanes — classify by representative HS code.
        # Preserves FIX-3 caveat: representative HS may not cover all products on the lane.
        lanes = [
            l for l in lanes
            if classify_shipment(
                {"hs_code": l.get("representative_lane", {}).get("hs_code", "")},
                industry,
            )
        ]
        # Shipments — classify by per-row hs_code (exact product match)
        shipments = [
            s for s in shipments
            if classify_shipment({"hs_code": s.get("hs_code", "")}, industry)
        ]
        # RoO assessments — classify by hs_code; unavailable-dict passes through unchanged
        if isinstance(roo_items, list):
            roo_items = [
                r for r in roo_items
                if classify_shipment({"hs_code": r.get("hs_code", "")}, industry)
            ]
        # CoO requests — no HS code in schema; filter by trade lane referenced.
        # A CoO for "KR → US" belongs to that lane: if that lane isn't in the selected
        # industry, the CoO is not industry-relevant. We parse our own internal format.
        _lane_od = {(l["origin"], l["destination"]) for l in lanes}
        coo_filtered = []
        for _c in coo_requests:
            _parts = _c.get("lane", "").split(" → ")
            if len(_parts) == 2 and (_parts[0].strip(), _parts[1].strip()) in _lane_od:
                coo_filtered.append(_c)
        coo_requests = coo_filtered
        # Roadmap — filter by lane_id; roadmap items reference their source lane directly
        _lane_ids = {l["lane_id"] for l in lanes}
        roadmap = [item for item in roadmap if item.get("lane_id") in _lane_ids]

        _no_industry_match = not lanes and not shipments
        # Apply same HS-based industry filter to status-split lane sets
        _ind_lane_filter = lambda l: classify_shipment(
            {"hs_code": l.get("representative_lane", {}).get("hs_code", "")}, industry
        )
        lanes_shipped   = [l for l in lanes_shipped   if _ind_lane_filter(l)]
        lanes_intransit = [l for l in lanes_intransit if _ind_lane_filter(l)]

        # Re-derive KPIs from filtered data — never all-industry totals under an industry label
        if not kpis.get("empty") and not _no_industry_match:
            _f_elig  = sum(l["eligible_value_m"] for l in lanes)
            _f_claim = sum(l["claimed_value_m"]  for l in lanes)
            _f_uncl  = sum(
                l["unclaimed_savings_k"] for l in lanes
                if l["unclaimed_savings_k"] is not None
            )
            _f_coo_o = sum(
                1 for c in coo_requests if c["status"] in ("pending", "overdue", "received")
            )
            kpis = dict(kpis)
            kpis["utilization_pct"]         = round(_f_claim / _f_elig * 100, 1) if _f_elig else 0.0
            kpis["unclaimed_opportunity_m"] = round(_f_uncl / 1_000, 2)
            kpis["coo_outstanding"]         = _f_coo_o
        elif _no_industry_match and not kpis.get("empty"):
            kpis = dict(kpis)
            kpis["utilization_pct"]         = None
            kpis["unclaimed_opportunity_m"] = None
            kpis["coo_outstanding"]         = None

    # ── Category filtering ─────────────────────────────────────────────────
    # Applied after industry filtering.  cat="all" or "" → no additional filter.
    _cat_active = cat not in ("", "all")
    if _cat_active and not _is_empty:
        shipments = [
            s for s in shipments
            if s.get("product_category", "").lower() == cat
        ]
        if isinstance(roo_items, list):
            roo_items = [
                r for r in roo_items
                if r.get("product_category", "").lower() == cat
            ]
        # Keep only lanes that have at least one matching shipment
        _cat_lane_keys = {
            (s["fta_name"], s["origin"], s["destination"]) for s in shipments
        }
        lanes = [
            l for l in lanes
            if (l["fta_name"], l["origin"], l["destination"]) in _cat_lane_keys
        ]
        _lane_ids_cat = {l["lane_id"] for l in lanes}
        roadmap = [item for item in roadmap if item.get("lane_id") in _lane_ids_cat]
        # Propagate category filter to status-split lane sets (by lane key)
        _cat_keys_fin = {(l["fta_name"], l["origin"], l["destination"]) for l in lanes}
        lanes_shipped   = [l for l in lanes_shipped   if (l["fta_name"], l["origin"], l["destination"]) in _cat_keys_fin]
        lanes_intransit = [l for l in lanes_intransit if (l["fta_name"], l["origin"], l["destination"]) in _cat_keys_fin]
        # Re-derive KPIs from category-filtered lanes
        if not kpis.get("empty") and lanes:
            _cf_elig  = sum(l["eligible_value_m"] for l in lanes)
            _cf_claim = sum(l["claimed_value_m"]  for l in lanes)
            _cf_uncl  = sum(
                l["unclaimed_savings_k"] for l in lanes
                if l["unclaimed_savings_k"] is not None
            )
            kpis = dict(kpis)
            kpis["utilization_pct"]         = round(_cf_claim / _cf_elig * 100, 1) if _cf_elig else 0.0
            kpis["unclaimed_opportunity_m"] = round(_cf_uncl / 1_000, 2)
        elif not kpis.get("empty") and not lanes:
            kpis = dict(kpis)
            kpis["utilization_pct"]         = None
            kpis["unclaimed_opportunity_m"] = None

    # Period label comes from the data source (dynamic for uploaded data)
    period_label = kpis.get("period_label", "")

    # ── Header ──────────────────────────────────────────────────────────
    _is_empty = (source["shipment_mode"] == "empty")
    if _is_empty:
        _subtitle = 'Upload your shipment data to begin — tariff rates sourced from the live aggregator'
    elif _no_industry_match:
        _subtitle = (
            f'No {ind_label} shipments in your uploaded data'
            ' — switch to All Industries to see your full upload'
        )
    else:
        _subtitle = 'Showing uploaded data — figures derived from your shipment and CoO files'
    header_html = (
        '<h2 style="font-size:1rem;font-weight:700;color:#1a0533;margin-bottom:16px;">'
        'FTA &amp; Preferential Trade Agent</h2>'
    )

    # ── Category filter dropdown (injected into the topbar via topbar_extras) ─
    _cat_opts_map = [
        ("all",              "All Categories"),
        ("laptops",          "Laptops"),
        ("peripherals",      "Peripherals"),
        ("server equipment", "Server Equipment"),
    ]
    _cat_opts_html = "".join(
        f'<option value="{_cv}"{" selected" if cat == _cv else ""}>{_cl}</option>'
        for _cv, _cl in _cat_opts_map
    )
    topbar_extras = (
        '<div class="cat-filter-wrap">'
        '<span class="cat-filter-label">Category</span>'
        '<select class="cat-filter-select" '
        "onchange=\"window.location.href='?cat='+encodeURIComponent(this.value)\">"
        + _cat_opts_html +
        '</select></div>'
    )

    # ── Upload / data-source section ─────────────────────────────────────
    # Build source badge — covers all four upload types
    _badge_parts = []
    if source["shipment_mode"] == "uploaded":
        _badge_parts.append(f'Shipments: {source["shipment_filename"]}')
    if source["coo_mode"] == "uploaded":
        _badge_parts.append(f'CoO: {source["coo_filename"]}')
    if source.get("bom_mode") == "uploaded":
        _badge_parts.append(f'BoM: {source.get("bom_filename", "")}')
    if source.get("roo_mode") == "uploaded":
        _badge_parts.append(f'RoO: {source.get("roo_filename", "")}')

    if not _badge_parts:
        _src_badge = (
            '<span style="padding:2px 10px;border-radius:4px;font-size:0.68rem;'
            'font-weight:700;background:#f0eaf8;color:#999;letter-spacing:1px">'
            'NO DATA</span>'
        )
    else:
        _src_badge = (
            '<span style="padding:2px 10px;border-radius:4px;font-size:0.68rem;'
            'font-weight:700;background:#A100FF;color:#fff;letter-spacing:1px">'
            f'UPLOADED · {" | ".join(_badge_parts)}</span>'
        )

    # Build upload feedback banners — Shipment, CoO, BoM, RoO
    _feedback = ""
    for _msg, _label in [(s_msg, "Shipment"), (c_msg, "CoO"),
                         (b_msg, "BoM"), (r_msg, "RoO Rules")]:
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

    # Auto-open the panel when there's a message or any upload is active
    _panel_open = "open" if (
        _feedback
        or source["shipment_mode"] == "uploaded"
        or source.get("bom_mode") == "uploaded"
        or source.get("roo_mode") == "uploaded"
    ) else ""

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
    _bom_cols_str = ", ".join(fta_data_source.BOM_REQUIRED)
    _roo_cols_str = ", ".join(fta_data_source.ROO_REQUIRED)

    _up_body_style = '' if _panel_open else 'display:none'
    _up_arrow      = '▼' if _panel_open else '▶'
    upload_section = (
        '<div id="upPanel" style="margin-bottom:16px;border-radius:10px;'
        'box-shadow:0 4px 16px rgba(26,5,51,0.12);background:#fff;'
        'border:1px solid rgba(161,0,255,0.13)">'
        '<div onclick="(function(h){'
        'var b=document.getElementById(\'upBody\');'
        'var open=b.style.display===\'none\';'
        'b.style.display=open?\'\':\'none\';'
        'h.querySelector(\'.up-arr\').textContent=open?\'▼\':\'▶\';'
        '})(this)" '
        'style="padding:12px 20px;font-size:0.8rem;font-weight:700;'
        'text-transform:uppercase;letter-spacing:1px;'
        'display:flex;align-items:center;gap:10px;cursor:pointer;'
        'user-select:none;border-bottom:1px solid #f0eaf8">'
        '\U0001f4c2 Data Source'
        f'<span style="margin-left:8px">{_src_badge}</span>'
        f'<span class="up-arr" style="margin-left:auto;font-size:0.72rem;font-weight:400;'
        f'color:#A100FF;text-transform:none;letter-spacing:0">{_up_arrow}</span>'
        '</div>'
        f'<div id="upBody" style="padding:16px 20px;{_up_body_style}">'
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
        '<form id="shp-upload-form" method="post" action="/api/fta/upload/shipments" '
        'onsubmit="ftaUploadShipments(event)" '
        'enctype="multipart/form-data" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
        '<input type="file" name="shipment_file" accept=".csv,.xlsx" '
        'style="font-size:0.78rem;flex:1;min-width:0">'
        '<button id="shp-upload-btn" type="submit" style="padding:6px 14px;background:#A100FF;color:#fff;'
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
        # ── BoM upload ──
        '<div style="border:1px solid #ede8f8;border-radius:8px;padding:14px">'
        '<div style="font-size:0.82rem;font-weight:700;margin-bottom:4px">'
        '\U0001f9e9 Bill of Materials <span style="font-size:0.7rem;font-weight:400;color:#999">(optional)</span>'
        '</div>'
        '<div style="font-size:0.72rem;color:#888;margin-bottom:10px">'
        'Powers: BoM-based RVC computation — product_id must match shipment upload'
        '</div>'
        f'<details style="margin-bottom:10px"><summary style="font-size:0.7rem;'
        f'color:#A100FF;cursor:pointer">Expected columns</summary>'
        f'<div style="font-size:0.68rem;color:#666;margin-top:6px;line-height:1.5">'
        f'<strong>Required:</strong> {_bom_cols_str}</div></details>'
        '<form action="/api/fta/upload/bom" method="post" '
        'enctype="multipart/form-data" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
        '<input type="file" name="bom_file" accept=".csv,.xlsx" '
        'style="font-size:0.78rem;flex:1;min-width:0">'
        '<button type="submit" style="padding:6px 14px;background:#A100FF;color:#fff;'
        'border:none;border-radius:6px;font-size:0.78rem;font-weight:600;cursor:pointer">'
        'Upload</button>'
        '<a href="/api/fta/template/bom" '
        'style="font-size:0.75rem;color:#A100FF;text-decoration:none;white-space:nowrap">'
        '\U0001f4e5 Template</a>'
        '</form>'
        '</div>'
        # ── RoO Rules upload ──
        '<div style="border:1px solid #ede8f8;border-radius:8px;padding:14px">'
        '<div style="font-size:0.82rem;font-weight:700;margin-bottom:4px">'
        '\U0001f4dc Rules of Origin <span style="font-size:0.7rem;font-weight:400;color:#999">(optional)</span>'
        '</div>'
        '<div style="font-size:0.72rem;color:#888;margin-bottom:10px">'
        'Powers: RoO threshold lookup — replaces roo_threshold_pct column when uploaded'
        '</div>'
        f'<details style="margin-bottom:10px"><summary style="font-size:0.7rem;'
        f'color:#A100FF;cursor:pointer">Expected columns</summary>'
        f'<div style="font-size:0.68rem;color:#666;margin-top:6px;line-height:1.5">'
        f'<strong>Required:</strong> {_roo_cols_str}</div></details>'
        '<form action="/api/fta/upload/roo" method="post" '
        'enctype="multipart/form-data" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
        '<input type="file" name="roo_file" accept=".csv,.xlsx" '
        'style="font-size:0.78rem;flex:1;min-width:0">'
        '<button type="submit" style="padding:6px 14px;background:#A100FF;color:#fff;'
        'border:none;border-radius:6px;font-size:0.78rem;font-weight:600;cursor:pointer">'
        'Upload</button>'
        '<a href="/api/fta/template/roo" '
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
            '↩ Clear uploads</button>'
            '</form>'
            '<span style="font-size:0.68rem;color:#bbb">'
            'Removes all uploaded files and returns to the empty state</span>'
            '</div>'
            if _badge_parts else ""
        )
        + '</div></div>'
    )

    # ── KPI strip (4 cards) ──────────────────────────────────────────────
    _util_val = f'{kpis["utilization_pct"]}%' if kpis["utilization_pct"] is not None else "—"
    _uncl_val = f'${kpis["unclaimed_opportunity_m"]}M' if kpis["unclaimed_opportunity_m"] is not None else "—"
    _coo_val  = str(kpis["coo_outstanding"]) if kpis["coo_outstanding"] is not None else "—"
    # FIX A: retroactive claims — None means uncomputable, not zero
    if kpis["retroactive_claims_k"] is None:
        _retro_val   = "Not available"
        _retro_unit  = "Requires retro-eligibility data"
        _retro_style = "font-size:0.9rem;color:#bbb;font-weight:400"
    else:
        _retro_val   = f'${kpis["retroactive_claims_k"]}K'
        _retro_unit  = f'{kpis["retro_window_label"]} · {period_label}'
        _retro_style = ""
    _period_suffix = f' · {period_label}' if period_label and period_label != "—" else ""
    kpi_html = (
        f'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">'
        f'{prov_legend}'
        f'{_sap_badge("SAP TM + GTS")}</div>'
        '<div style="display:grid;grid-template-columns:repeat(4,1fr);'
        'gap:16px;margin-bottom:28px">'
        f'<div class="kpi-card" style="border-top:3px solid #A100FF">'
        + _kpi_lbl("Utilization Rate", ["PREF_STATUS", "CUSVAL"]) +
        f'<div class="kpi-value">{_util_val}</div>'
        f'<div class="kpi-unit">{period_label}</div></div>'
        f'<div class="kpi-card" style="border-top:3px solid #F76C6C">'
        + _kpi_lbl("Unclaimed Opportunity", ["CUSVAL", "MFN_RATE", "PREF_RATE"]) +
        f'<div class="kpi-value">{_uncl_val}</div>'
        f'<div class="kpi-unit">Duty savings · {period_label}</div></div>'
        f'<div class="kpi-card" style="border-top:3px solid #A100FF">'
        + _kpi_lbl("Retroactive Claims", ["ENTRY_DATE", "CUSVAL", "MFN_RATE", "PREF_RATE"]) +
        f'<div class="kpi-value" style="{_retro_style}">{_retro_val}</div>'
        f'<div class="kpi-unit">{_retro_unit}{_period_suffix}</div></div>'
        f'<div class="kpi-card" style="border-top:3px solid #F5A623">'
        + _kpi_lbl("CoOs Outstanding", ["POO_STATUS"]) +
        f'<div class="kpi-value">{_coo_val}</div>'
        f'<div class="kpi-unit">Pending + Overdue + Received</div></div>'
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
    def _mk_lane_tbody(lane_list, empty_msg):
        if not lane_list:
            return (
                '<tr><td colspan="7" style="padding:32px;text-align:center;'
                f'color:#999;font-size:0.85rem">{empty_msg}</td></tr>'
            )
        rows = ""
        for lane in lane_list:
            util      = lane["utilization_pct"]
            uncl_k    = lane["unclaimed_savings_k"]
            sc        = "#c0392b" if (uncl_k or 0) > 200 else "#1a0533"
            orig_name = _ctry(lane["origin"])
            dest_name = _ctry(lane["destination"])
            mfn_str   = f'{lane["mfn_rate_pct"]}%'          if lane["mfn_rate_pct"]          is not None else "—"
            pref_str  = f'{lane["preferential_rate_pct"]}%' if lane["preferential_rate_pct"] is not None else "—"
            uncl_html = f'${uncl_k}K' if uncl_k is not None else '<span style="color:#bbb">—</span>'
            _rs_mfn   = str(lane["mfn_rate_pct"])          if lane["mfn_rate_pct"]          is not None else ""
            _rs_pref  = str(lane["preferential_rate_pct"]) if lane["preferential_rate_pct"] is not None else ""
            _rs_lane  = f'{orig_name} → {dest_name}'
            rows += (
                '<tr style="border-bottom:1px solid #f5f3fa">'
                f'<td style="padding:10px 12px;font-size:0.82rem">'
                f'{orig_name} → {dest_name}</td>'
                f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600">'
                f'{lane["fta_name"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.82rem">'
                f'${lane["eligible_value_m"]}M</td>'
                f'<td style="padding:10px 12px;font-size:0.82rem">'
                f'${lane["claimed_value_m"]}M</td>'
                f'<td style="padding:10px 12px;font-size:0.78rem;color:#555">'
                f'<button class="rate-cell-btn" '
                f'data-src="{lane["rates_source"]}" data-mfn="{_rs_mfn}" '
                f'data-pref="{_rs_pref}" data-fta="{lane["fta_name"]}" '
                f'data-lane="{_rs_lane}" onclick="openRsDialog(this)" '
                f'title="Click to see rate source &amp; data honesty">'
                f'<span style="color:#c0392b;font-weight:600">{mfn_str}</span>'
                f' MFN → '
                f'<span style="color:#12B3A3;font-weight:600">{pref_str}</span>'
                f' pref'
                f'<span class="rate-cell-info">&#x24D8;</span>'
                f'</button></td>'
                f'<td style="padding:10px 12px;font-size:0.82rem">'
                f'<div style="background:#e8e0f0;border-radius:3px;height:6px;'
                f'width:80px;display:inline-block">'
                f'<div style="background:#A100FF;height:6px;border-radius:3px;'
                f'width:{util}%"></div></div>'
                f'<span style="margin-left:6px;font-size:0.75rem">{util}%</span></td>'
                f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:{sc}">'
                f'{uncl_html}</td>'
                '</tr>'
            )
        return rows

    _lane_empty_all = (
        f'No {ind_label} shipments in your uploaded data'
        if _no_industry_match else
        '\U0001f4c2 No shipment data — upload a file above to populate this table'
    )
    _tbody_all      = _mk_lane_tbody(lanes,         _lane_empty_all)
    _tbody_shipped  = _mk_lane_tbody(lanes_shipped,  "No Shipped / Delivered shipments in this view")
    _tbody_intransit= _mk_lane_tbody(lanes_intransit,"No In-Transit shipments in this view")

    _lane_thead = (
        '<thead><tr>'
        + _pth("Trade Lane", ["CTYDP", "CTYAR"])
        + _pth("FTA Agreement", ["AGREEMENT"])
        + _pth("Eligible Value", ["CUSVAL", "PREF_STATUS"])
        + _pth("Claimed", ["CUSVAL", "PREF_STATUS"])
        + _pth("Rate Differential", ["MFN_RATE", "PREF_RATE"])
        + _pth("Utilization", ["PREF_STATUS", "CUSVAL"])
        + _pth("Unclaimed Savings", ["CUSVAL", "MFN_RATE", "PREF_RATE"])
        + '</tr></thead>'
    )

    lane_section = (
        '<div class="section-card">'
        f'<div class="section-card-header">\U0001f310 FTA Lane Utilization Gap'
        f'<span style="font-size:0.72rem;font-weight:400;color:#888;'
        f'margin-left:10px;letter-spacing:0">({period_label})</span>'
        f'{_sap_badge("SAP TM + GTS")}</div>'
        # ── Status toggle bar ─────────────────────────────────────────────
        '<div style="padding:10px 20px;display:flex;align-items:center;gap:8px;'
        'border-bottom:1px solid #f0eaf8;flex-wrap:wrap">'
        '<span style="font-size:0.72rem;font-weight:600;color:#888;'
        'text-transform:uppercase;letter-spacing:0.5px;margin-right:4px">Filter:</span>'
        '<button id="ltog-all" onclick="switchLaneFilter(\'all\')" '
        'style="padding:3px 12px;border-radius:12px;border:1px solid #A100FF;'
        'background:#A100FF;color:#fff;font-size:0.75rem;font-weight:600;cursor:pointer">All</button>'
        '<button id="ltog-shipped" onclick="switchLaneFilter(\'shipped\')" '
        'style="padding:3px 12px;border-radius:12px;border:1px solid #ddd;'
        'background:#fff;color:#555;font-size:0.75rem;font-weight:600;cursor:pointer">Shipped</button>'
        '<button id="ltog-intransit" onclick="switchLaneFilter(\'intransit\')" '
        'style="padding:3px 12px;border-radius:12px;border:1px solid #ddd;'
        'background:#fff;color:#555;font-size:0.75rem;font-weight:600;cursor:pointer">In-Transit</button>'
        '<span id="lane-status-note" style="font-size:0.72rem;color:#888;margin-left:8px"></span>'
        '</div>'
        '<div style="overflow-x:auto;max-height:300px;overflow-y:auto">'
        '<table style="width:100%;border-collapse:collapse">'
        + _lane_thead
        + f'<tbody id="lane-tbody-all">{_tbody_all}</tbody>'
        + f'<tbody id="lane-tbody-shipped" style="display:none">{_tbody_shipped}</tbody>'
        + f'<tbody id="lane-tbody-intransit" style="display:none">{_tbody_intransit}</tbody>'
        + '</table></div></div>'
    )

    # ── Merged Shipment + RoO table (replaces both original tables) ─────────
    roo_badge_styles = {
        "Q": "background:#e6fff9;color:#12B3A3",
        "M": "background:#fff3cd;color:#856404",
        "F": "background:#fde8e8;color:#c0392b",
    }

    # Build RoO lookup keyed by (product, hs_code, fta_name)
    _roo_lut: dict = {}
    if isinstance(roo_items, list):
        for _r in roo_items:
            _roo_lut[(_r["product"], _r["hs_code"], _r["fta_name"])] = _r

    unclaimed    = [s for s in shipments if s["claimed_status"] == "U"]
    merged_rows  = ""
    if not unclaimed:
        if _is_empty:
            _m_empty = '\U0001f4c2 No shipment data — upload a file above'
        elif _no_industry_match:
            _m_empty = f'No {ind_label} unclaimed shipments in your uploaded data'
        else:
            _m_empty = '✓ No eligible-unclaimed shipments in the uploaded data'
        merged_rows = (
            '<tr><td colspan="15" style="padding:32px;text-align:center;'
            f'color:#999;font-size:0.85rem">{_m_empty}</td></tr>'
        )

    for s in unclaimed:
        roo_code  = s["roo_status"]
        roo_label = ROO_STATUS_LABELS.get(roo_code) or "—"
        row_bg    = "background:#fff8e6;" if roo_code == "M" else ""
        ro_badge  = roo_badge_styles.get(roo_code, "color:#999;background:#f0f0f0")
        tor_id    = s["shipment_id"]
        saving_k  = s.get("est_saving_k") or 0.0
        origin_n  = _ctry(s["origin"])
        dest_n    = _ctry(s["destination"])
        entry_d   = _dats_display(s["entry_date"])
        s_cat     = s.get("product_category") or "—"

        # Merge in RoO assessment data for this product/lane
        _rk  = (s["product"], s["hs_code"], s["fta_name"])
        _rd  = _roo_lut.get(_rk)
        if _rd:
            _roo_test = _rd.get("roo_test_type", "—")
            _rvc      = _rd.get("rvc_pct")
            _thr      = _rd.get("roo_threshold_pct")
            _rvc_cell = f'{_rvc}% / {_thr}%' if _rvc is not None and _thr is not None else "—"
            _gap      = _rd.get("gap_pct", 0)
            _gap_html = (
                f'<span style="color:#c0392b;font-weight:600">−{_gap} pts</span>'
                if _gap > 0 else
                '<span style="color:#12B3A3;font-weight:600">—</span>'
            )
            _note         = _rd.get("compliance_note", "")
            _note_escaped = _note.replace("&", "&amp;").replace('"', "&quot;")
        else:
            _roo_test = _rvc_cell = "—"
            _gap_html = "—"
            _note = _note_escaped = ""

        merged_rows += (
            f'<tr style="{row_bg}cursor:pointer;border-bottom:1px solid #f5f3fa" '
            f"onclick=\"fetchFTAExplain(this, '{tor_id}')\" "
            f'data-shipment-id="{tor_id}" '
            f'data-product="{s["product"]}" '
            f'data-hs-code="{s["hs_code"]}" '
            f'data-origin="{origin_n}" '
            f'data-destination="{dest_n}" '
            f'data-fta-name="{s["fta_name"]}" '
            f'data-value-k="{s["value_k"]}" '
            f'data-eligibility="eligible-unclaimed" '
            f'data-est-saving-k="{saving_k}" '
            f'data-ro-status="{roo_label}" '
            f'data-rvc-pct="{s["rvc_pct"] if s["rvc_pct"] is not None else 0}" '
            f'data-rvc-threshold="{s["roo_threshold_pct"] if s["roo_threshold_pct"] is not None else 0}" '
            f'data-compliance-note="{_note_escaped}">'
            # Shipment columns
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;white-space:nowrap">{tor_id}</td>'
            f'<td style="padding:10px 12px;font-size:0.78rem;color:#666;font-family:monospace;white-space:nowrap">{entry_d}</td>'
            f'<td style="padding:10px 12px;font-size:0.78rem;color:#555">{s_cat}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">{s["product"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.78rem;color:#555">{s.get("supplier_name") or "—"}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-family:monospace">{s["hs_code"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;white-space:nowrap">{origin_n} → {dest_n}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:#A100FF">{s["fta_name"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:#A100FF;white-space:nowrap">${saving_k}K</td>'
            # RoO assessment columns
            f'<td style="padding:10px 12px;font-size:0.78rem;color:#555">{_roo_test}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;text-align:center;white-space:nowrap">{_rvc_cell}</td>'
            f'<td style="padding:10px 12px">'
            f'<span style="padding:2px 8px;border-radius:4px;font-size:0.72rem;font-weight:600;{ro_badge}">{roo_label}</span>'
            + (
                f'<div style="font-size:0.65rem;color:#999;margin-top:3px">'
                + (f'RVC {s["rvc_pct"]}% / {s["roo_threshold_pct"]}% req\'d'
                   if s["rvc_pct"] is not None and s["roo_threshold_pct"] is not None
                   else "RVC data incomplete")
                + '</div>'
            ) +
            f'</td>'
            f'<td style="padding:10px 12px;text-align:center">{_gap_html}</td>'
            f'<td style="padding:10px 12px;font-size:0.75rem;color:#666;max-width:180px">{_note}</td>'
            f'<td style="padding:10px 12px;font-size:0.78rem;color:#A100FF;font-weight:600;white-space:nowrap">▶ Explain</td>'
            '</tr>'
        )

    merged_section = (
        '<div class="section-card">'
        '<div class="section-card-header">'
        '\U0001f4cb Shipment Eligibility &amp; RoO Assessment — Unclaimed (PREF_STATUS=U)'
        f'<span style="font-size:0.72rem;font-weight:400;color:#888;margin-left:10px;letter-spacing:0">({period_label})</span>'
        f'{_sap_badge("SAP TM + GTS")}</div>'
        '<div style="overflow-x:auto;max-height:420px;overflow-y:auto">'
        '<table style="width:100%;border-collapse:collapse">'
        '<thead><tr style="position:sticky;top:0;z-index:2;background:#fff">'
        + _pth("Freight Order", ["TOR_ID"])
        + _pth("Entry Date", ["ENTRY_DATE"])
        + _pth("Category", ["PRODUCT_CATEGORY"])
        + _pth("Product", ["PRODUCT_TEXT"])
        + _pth("Supplier", ["SUPPLIER_NAME"])
        + _pth("HS Code", ["CCNGN"])
        + _pth("Lane", ["CTYDP", "CTYAR"])
        + _pth("Agreement", ["AGREEMENT"])
        + _pth("Est. Saving", ["CUSVAL", "MFN_RATE", "PREF_RATE"])
        + _pth("RoO Test", [])
        + _pth("RVC Actual / Required", ["RVC_PCT", "RVC_THRESHOLD"])
        + _pth("RoO Status", ["ROO_STATUS"])
        + _pth("Gap", ["RVC_PCT", "RVC_THRESHOLD"])
        + _pth("Compliance Note", [])
        + _pth("Action", []) +
        '</tr></thead>'
        f'<tbody>{merged_rows}</tbody>'
        '</table></div>'
        '<div id="fta-ai-box" class="ai-summary" style="display:none;margin:16px 20px"></div>'
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
    if not coo_requests:
        _coo_empty = (
            f'No {ind_label} CoO requests in your uploaded data'
            if _no_industry_match else
            '\U0001f4c2 No CoO data — upload a CoO requests file above'
        )
        coo_rows = (
            '<tr><td colspan="4" style="padding:32px;text-align:center;'
            f'color:#999;font-size:0.85rem">{_coo_empty}</td></tr>'
        )
    for req in coo_requests:
        poo_s     = req["status"]
        poo_label = POO_STATUS_LABELS.get(poo_s, poo_s.title())
        badge     = coo_badge_styles.get(poo_s, "")
        lane_disp = f'{_ctry(req["origin"])} → {_ctry(req["destination"])}'
        deadline_d = _dats_display(req["deadline"])
        coo_rows += (
            '<tr style="border-bottom:1px solid #f5f3fa">'
            f'<td style="padding:10px 12px;font-size:0.82rem">{req["supplier_name"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">{lane_disp}</td>'
            f'<td style="padding:10px 12px;font-size:0.75rem;color:#555">'
            f'{req["poo_type"]}</td>'
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
        _roo_reason = roo_items.get("reason", "")
        if _roo_reason == "no_upload":
            roo_body = (
                '<div style="padding:32px 20px;text-align:center;color:#999">'
                '\U0001f4c2 Upload shipment data to view RoO compliance assessment'
                '</div>'
            )
        else:
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
        if not roo_items:
            _roo_empty = (
                f'No {ind_label} products in your uploaded data'
                if _no_industry_match else
                '✓ All assessed products are compliant — no gaps to report'
            )
            roo_rows = (
                f'<tr><td colspan="8" style="padding:32px;text-align:center;'
                f'color:#999;font-size:0.85rem">{_roo_empty}</td></tr>'
            )
        for p in roo_items:
            roo_code = p.get("roo_status")
            verdict  = p.get("verdict", "")
            if roo_code in ("Q", "M", "F"):
                roo_label = ROO_STATUS_LABELS.get(roo_code, roo_code)
                badge     = roo_badge_styles_assess.get(roo_code, "")
            else:
                roo_label = verdict if verdict else "—"
                badge     = "background:#f0f0f4;color:#52525B"

            # RVC column — show value + source provenance
            _rvc     = p.get("rvc_pct")
            _thr     = p.get("roo_threshold_pct")
            _rvc_src = p.get("rvc_source", "")
            _thr_src = p.get("threshold_source", "")
            if _rvc is not None:
                _prov = (
                    ' <span style="font-size:0.62rem;color:#888">(BoM)</span>'
                    if _rvc_src == "computed_from_bom"
                    else ' <span style="font-size:0.62rem;color:#888">(client)</span>'
                    if _rvc_src == "client_column"
                    else ""
                )
                _rvc_cell = f'{_rvc}%{_prov}'
            else:
                _rvc_cell = "—"
            if _thr is not None:
                _tprov = (
                    ' <span style="font-size:0.62rem;color:#888">(rule)</span>'
                    if _thr_src == "uploaded_roo_rules" else ""
                )
                _thr_cell = f'{_thr}%{_tprov}'
            else:
                _thr_cell = "—"

            gap_cell = (
                f'<span style="color:#c0392b;font-weight:600">−{p["gap_pct"]} pts</span>'
                if p.get("gap_pct", 0) > 0 else
                '<span style="color:#12B3A3;font-weight:600">—</span>'
            )
            roo_rows += (
                '<tr style="border-bottom:1px solid #f5f3fa">'
                f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600">{p["product"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.78rem;font-family:monospace">{p["hs_code"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:#A100FF">{p["fta_name"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.78rem;color:#555">{p["roo_test_type"]}</td>'
                f'<td style="padding:10px 12px;font-size:0.82rem;text-align:center">'
                f'{_rvc_cell} / {_thr_cell}</td>'
                f'<td style="padding:10px 12px;text-align:center">'
                f'<span style="padding:2px 8px;border-radius:4px;font-size:0.72rem;'
                f'font-weight:600;{badge}">{roo_label}</span></td>'
                f'<td style="padding:10px 12px;text-align:center">{gap_cell}</td>'
                f'<td style="padding:10px 12px;font-size:0.78rem;color:#666">{p["compliance_note"]}</td>'
                '</tr>'
            )

        # Footer — describe active data sources
        _roo_src_parts: list[str] = []
        if source.get("bom_mode") == "uploaded":
            _roo_src_parts.append("RVC: uploaded BoM")
        if source.get("roo_mode") == "uploaded":
            _roo_src_parts.append("thresholds: uploaded RoO rules")
        if not _roo_src_parts:
            _roo_src_parts.append("RVC + thresholds: client-provided shipment columns")
        _roo_footer_text = "  ·  ".join(_roo_src_parts)

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
            f'<div style="padding:8px 16px;font-size:0.68rem;color:#bbb;border-top:1px solid #f0eaf8">'
            f'{_roo_footer_text}'
            '</div>'
        )

    # roo_section removed — its data is now merged into merged_section above.

    # ── Qualification Roadmap (collapsible) ──────────────────────────────
    roadmap_rows = ""
    if not roadmap:
        if _is_empty:
            _rm_msg = '\U0001f4c2 No shipment data — qualification roadmap is generated from lane data'
        elif _no_industry_match:
            _rm_msg = f'No {ind_label} lanes in your uploaded data'
        else:
            _rm_msg = '✓ All lanes are at or above 75% utilization — no under-utilized lanes to action'
        roadmap_rows = (
            '<tr><td colspan="7" style="padding:32px;text-align:center;'
            f'color:#999;font-size:0.85rem">{_rm_msg}</td></tr>'
        )
    for item in roadmap:
        effort_badge = effort_styles.get(item["effort"], "")
        _rm_sav = item["unclaimed_savings_k"]
        sc = "#c0392b" if (_rm_sav or 0) > 200 else "#1a0533"
        _rm_sav_html = f'${_rm_sav}K' if _rm_sav is not None else '<span style="color:#bbb">—</span>'
        _rm_lane_disp = f'{_ctry(item["origin"])} → {_ctry(item["destination"])}'
        roadmap_rows += (
            '<tr style="border-bottom:1px solid #f5f3fa">'
            f'<td style="padding:10px 12px;font-size:0.82rem">{_rm_lane_disp}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:#A100FF">'
            f'{item["fta_name"]}</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem">{item["utilization_pct"]}%</td>'
            f'<td style="padding:10px 12px;font-size:0.82rem;font-weight:600;color:{sc}">'
            f'{_rm_sav_html}</td>'
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
        '<details open style="margin-bottom:12px;border-radius:10px;'
        'box-shadow:0 2px 8px rgba(26,5,51,0.07);background:#fff">'
        '<summary style="padding:14px 20px;font-size:0.8rem;font-weight:700;'
        'text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #f0eaf8;'
        'display:flex;align-items:center;gap:8px;cursor:pointer;list-style:none;'
        'user-select:none;color:#1a0533">'
        '\U0001f5fa Qualification Roadmap — Under-Utilised Lanes'
        f'{_sap_badge("SAP GTS")}'
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
        'Derived from uploaded shipment data.'
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

    # ── Column-mapping modal HTML ─────────────────────────────────────────
    mapping_modal_html = (
        '<div id="fta-mapping-modal" '
        'style="display:none;position:fixed;inset:0;z-index:10001;'
        'background:rgba(26,5,51,0.52);align-items:center;justify-content:center" '
        'onclick="if(event.target===this)closeMappingModal()">'
        '<div style="background:#fff;border-radius:14px;max-width:740px;width:96%;'
        'box-shadow:0 16px 56px rgba(26,5,51,0.28);position:relative;'
        'max-height:90vh;display:flex;flex-direction:column">'
        # ── Modal header ──
        '<div style="padding:18px 24px 14px;border-bottom:1px solid #f0eaf8;'
        'display:flex;align-items:flex-start;gap:10px;flex-shrink:0">'
        '<div style="flex:1">'
        '<div style="font-size:0.95rem;font-weight:700;color:#1a0533">'
        'Column Mapping — confirm before loading</div>'
        '<div style="font-size:0.75rem;color:#888;margin-top:3px">'
        'Your file uses different column names. Map each field below, then click '
        '<strong>Apply &amp; Load</strong>.</div>'
        '</div>'
        '<button onclick="closeMappingModal()" '
        'style="background:#f5f3fa;border:none;width:28px;height:28px;border-radius:50%;'
        'font-size:0.85rem;color:#888;cursor:pointer;flex-shrink:0;'
        'display:flex;align-items:center;justify-content:center" '
        'title="Cancel (Esc)">&#x2715;</button>'
        '</div>'
        # ── Modal body (populated by JS) ──
        '<div id="mapping-modal-body" '
        'style="padding:16px 24px;overflow-y:auto;flex:1"></div>'
        # ── Modal footer ──
        '<div style="padding:14px 24px;border-top:1px solid #f0eaf8;'
        'display:flex;gap:10px;align-items:center;flex-shrink:0">'
        '<button id="apply-mapping-btn" onclick="applyMapping()" '
        'style="padding:7px 18px;background:#A100FF;color:#fff;border:none;'
        'border-radius:6px;font-size:0.8rem;font-weight:700;cursor:pointer">'
        'Apply &amp; Load</button>'
        '<button onclick="closeMappingModal()" '
        'style="padding:7px 14px;background:#f0eaf8;color:#555;border:none;'
        'border-radius:6px;font-size:0.8rem;cursor:pointer">Cancel</button>'
        '<span id="mapping-status" '
        'style="font-size:0.74rem;color:#888;flex:1;text-align:right"></span>'
        '</div>'
        '</div></div>'
    )

    # ── Compose page ─────────────────────────────────────────────────────
    content = (
        header_html + upload_section + kpi_html
        + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px">'
        + lane_section
        + coo_section
        + '</div>'
        + '<div style="margin-bottom:20px">' + merged_section + '</div>'
        + roadmap_section
        + prov_modal_html
        + mapping_modal_html
    )

    # ── Scripts ──────────────────────────────────────────────────────────
    scripts = """
<script>
// ── Lane status toggle ────────────────────────────────────────────────────
function switchLaneFilter(bucket) {
  var buckets = ['all', 'shipped', 'intransit'];
  var notes = {
    shipped:   'Already cleared — unclaimed savings on these were not captured.',
    intransit: 'Still capturable — these orders haven\'t cleared customs yet.',
    all:       ''
  };
  buckets.forEach(function(b) {
    var tbody = document.getElementById('lane-tbody-' + b);
    var btn   = document.getElementById('ltog-' + b);
    if (tbody) tbody.style.display = (b === bucket) ? '' : 'none';
    if (btn) {
      if (b === bucket) {
        btn.style.background = '#A100FF';
        btn.style.color = '#fff';
        btn.style.borderColor = '#A100FF';
      } else {
        btn.style.background = '#fff';
        btn.style.color = '#555';
        btn.style.borderColor = '#ddd';
      }
    }
  });
  var note = document.getElementById('lane-status-note');
  if (note) note.textContent = notes[bucket] || '';
}

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
        rvc_threshold_pct: parseFloat(row.dataset.rvcThreshold),
        compliance_note:  row.dataset.complianceNote || ''
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
    if (e.key === 'Escape') { closeSapProv(); closeMappingModal(); }
});

// ── FTA Column-Mapping Upload Flow ──────────────────────────────────────────

function ftaUploadShipments(event) {
    event.preventDefault();
    var form      = document.getElementById('shp-upload-form');
    var btn       = document.getElementById('shp-upload-btn');
    var fileInput = form ? form.querySelector('input[type="file"]') : null;
    if (!fileInput || !fileInput.files.length) {
        alert('Please select a file first.');
        return;
    }

    var origText = btn ? btn.textContent : 'Upload';
    if (btn) { btn.textContent = 'Uploading…'; btn.disabled = true; }

    var fd = new FormData();
    fd.append('shipment_file', fileInput.files[0]);

    fetch('/api/fta/upload/shipments', {
        method:  'POST',
        headers: {'Accept': 'application/json'},
        body:    fd
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (btn) { btn.textContent = origText; btn.disabled = false; }
        if (data.needs_mapping) {
            showMappingModal(data);
        } else if (data.ok) {
            window.location.reload();
        } else {
            alert('Upload error: ' + (data.error || 'Unknown error'));
        }
    })
    .catch(function(e) {
        if (btn) { btn.textContent = origText; btn.disabled = false; }
        alert('Upload failed: ' + e.message);
    });
}

function showMappingModal(data) {
    var modal = document.getElementById('fta-mapping-modal');
    var body  = document.getElementById('mapping-modal-body');
    if (!modal || !body) return;

    // Stash data for applyMapping()
    modal._mapData = data;

    var required = data.required_fields || [];
    var optional = new Set(data.optional_fields || []);

    // Split into mapped (suggestion != null) and unmapped
    var mapped   = required.filter(function(s) { return data.suggested[s]; });
    var unmapped = required.filter(function(s) { return !data.suggested[s]; });

    // Unused uploaded columns (not in any suggestion)
    var usedCols = new Set(Object.values(data.suggested).filter(Boolean));
    var unused   = (data.columns || []).filter(function(c) { return !usedCols.has(c); });

    var html = '';

    // File badge
    html += '<div style="font-size:0.74rem;color:#555;margin-bottom:14px;'
          + 'padding:7px 12px;background:#f8f6fc;border-radius:6px">'
          + '📄 <strong>' + escH(data.filename) + '</strong>'
          + ' &nbsp;·&nbsp; ' + (data.columns || []).length + ' columns detected'
          + '</div>';

    // Section A: Suggested mappings
    if (mapped.length > 0) {
        html += '<div style="margin-bottom:18px">'
              + '<div style="font-size:0.74rem;font-weight:700;color:#0a7060;'
              + 'display:flex;align-items:center;gap:6px;margin-bottom:8px">'
              + '<span style="width:9px;height:9px;border-radius:50%;'
              + 'background:#12B3A3;display:inline-block"></span>'
              + 'Suggested Mappings (' + mapped.length + ') &mdash; review and adjust if needed'
              + '</div>'
              + buildMappingTable(mapped, data, optional)
              + '</div>';
    }

    // Section B: Unmapped fields
    if (unmapped.length > 0) {
        var unmappedReq = unmapped.filter(function(s) { return !optional.has(s); });
        var unmappedOpt = unmapped.filter(function(s) { return optional.has(s); });
        if (unmappedReq.length > 0) {
            html += '<div style="margin-bottom:18px">'
                  + '<div style="font-size:0.74rem;font-weight:700;color:#c0392b;'
                  + 'display:flex;align-items:center;gap:6px;margin-bottom:6px">'
                  + '<span style="font-size:1rem">⚠️</span>'
                  + ' Unmapped Required Fields (' + unmappedReq.length + ')'
                  + ' &mdash; assign a column or the upload will fail'
                  + '</div>'
                  + buildMappingTable(unmappedReq, data, optional)
                  + '</div>';
        }
        if (unmappedOpt.length > 0) {
            html += '<div style="margin-bottom:18px">'
                  + '<div style="font-size:0.74rem;font-weight:700;color:#888;'
                  + 'display:flex;align-items:center;gap:6px;margin-bottom:6px">'
                  + '<span style="font-size:0.85rem">ℹ️</span>'
                  + ' Unmapped Optional Fields (' + unmappedOpt.length + ')'
                  + ' &mdash; those dashboard sections will be disabled'
                  + '</div>'
                  + buildMappingTable(unmappedOpt, data, optional)
                  + '</div>';
        }
    }

    // Section C: Unused columns
    if (unused.length > 0) {
        html += '<div style="margin-bottom:4px">'
              + '<div style="font-size:0.72rem;font-weight:700;color:#aaa;margin-bottom:5px">'
              + '🗂️ Unused columns in your file</div>'
              + '<div style="display:flex;flex-wrap:wrap;gap:5px">';
        unused.forEach(function(c) {
            html += '<span style="background:#f5f5f5;color:#888;padding:2px 8px;'
                  + 'border-radius:3px;font-size:0.7rem;font-family:monospace">'
                  + escH(c) + '</span>';
        });
        html += '</div></div>';
    }

    body.innerHTML = html;
    modal.style.display = 'flex';
    document.body.style.overflow = 'hidden';
    if (document.getElementById('mapping-status')) {
        document.getElementById('mapping-status').textContent = '';
    }
}

function buildMappingTable(sapFields, data, optionalSet) {
    var colOpts = (data.columns || []).map(function(c) {
        return '<option value="' + escAttr(c) + '">' + escH(c) + '</option>';
    }).join('');

    var rows = sapFields.map(function(sap) {
        var info    = data.field_info[sap] || {};
        var isOpt   = optionalSet.has(sap);
        var sugCol  = data.suggested[sap] || '';
        var warn    = (data.sanity_warnings || {})[sap] || '';

        // Dropdown options with suggested col pre-selected
        var opts = '<option value="">— none —</option>'
                 + (data.columns || []).map(function(c) {
                     var sel = (c === sugCol) ? ' selected' : '';
                     return '<option value="' + escAttr(c) + '"' + sel + '>'
                          + escH(c) + '</option>';
                 }).join('');

        // Sample values for the currently suggested column
        var sampleVals = sugCol && data.samples && data.samples[sugCol]
            ? data.samples[sugCol].slice(0, 3).join(', ')
            : '—';

        return '<tr style="border-bottom:1px solid #faf6ff">'
             // SAP field name
             + '<td style="padding:7px 8px;vertical-align:top;white-space:nowrap">'
             + '<span style="font-family:monospace;font-size:0.74rem;font-weight:700;color:#A100FF">'
             + escH(sap) + '</span>'
             + (isOpt ? ' <span style="font-size:0.64rem;color:#ccc">(opt)</span>' : '')
             + '</td>'
             // Universal name + note
             + '<td style="padding:7px 8px;vertical-align:top;max-width:140px">'
             + '<div style="font-size:0.74rem;font-weight:600;color:#333">' + escH(info.univ || sap) + '</div>'
             + '<div style="font-size:0.65rem;color:#aaa;line-height:1.3;margin-top:1px">' + escH(info.note || '') + '</div>'
             + '</td>'
             // Dropdown + sanity warning
             + '<td style="padding:7px 8px;vertical-align:top">'
             + '<select id="map_' + sap + '" '
             + `onchange="onMappingChange(this,'${sap}')" `
             + 'style="font-size:0.74rem;padding:3px 6px;border:1px solid #ddd;'
             + 'border-radius:4px;max-width:170px;cursor:pointer;width:100%">'
             + opts + '</select>'
             + (warn ? '<div style="font-size:0.64rem;color:#F5A623;margin-top:3px">⚠ ' + escH(warn) + '</div>' : '')
             + '</td>'
             // Sample values (updates on dropdown change)
             + '<td style="padding:7px 8px;vertical-align:top;max-width:140px">'
             + '<span id="smp_' + sap + '" style="font-size:0.67rem;color:#999;'
             + 'font-family:monospace;word-break:break-all">' + escH(sampleVals) + '</span>'
             + '</td>'
             + '</tr>';
    }).join('');

    return '<table style="width:100%;border-collapse:collapse;font-size:0.8rem">'
         + '<thead><tr style="border-bottom:2px solid #f0eaf8">'
         + '<th style="padding:5px 8px;text-align:left;font-size:0.66rem;'
         + 'text-transform:uppercase;color:#aaa;font-weight:700">SAP Field</th>'
         + '<th style="padding:5px 8px;text-align:left;font-size:0.66rem;'
         + 'text-transform:uppercase;color:#aaa;font-weight:700">Description</th>'
         + '<th style="padding:5px 8px;text-align:left;font-size:0.66rem;'
         + 'text-transform:uppercase;color:#aaa;font-weight:700">Your Column</th>'
         + '<th style="padding:5px 8px;text-align:left;font-size:0.66rem;'
         + 'text-transform:uppercase;color:#aaa;font-weight:700">Sample Values</th>'
         + '</tr></thead>'
         + '<tbody>' + rows + '</tbody>'
         + '</table>';
}

function onMappingChange(sel, sapField) {
    var modal = document.getElementById('fta-mapping-modal');
    var data  = modal && modal._mapData;
    var col   = sel.value;
    var sp    = document.getElementById('smp_' + sapField);
    if (sp && data) {
        var vals = col && data.samples && data.samples[col]
            ? data.samples[col].slice(0, 3).join(', ')
            : '—';
        sp.textContent = vals;
    }
}

function applyMapping() {
    var modal    = document.getElementById('fta-mapping-modal');
    var data     = modal && modal._mapData;
    var statusEl = document.getElementById('mapping-status');
    var btn      = document.getElementById('apply-mapping-btn');
    if (!data) return;

    // Collect current dropdown values into {SAP_NAME: col_or_null}
    var mapping = {};
    (data.required_fields || []).forEach(function(sap) {
        var sel = document.getElementById('map_' + sap);
        mapping[sap] = (sel && sel.value) ? sel.value : null;
    });

    if (btn) { btn.disabled = true; btn.textContent = 'Applying…'; }
    if (statusEl) statusEl.textContent = 'Processing your data…';

    fetch('/api/fta/upload/apply-mapping', {
        method:  'POST',
        headers: {'Content-Type': 'application/json'},
        body:    JSON.stringify({ mapping_key: data.mapping_key, mapping: mapping }),
    })
    .then(function(r) { return r.json(); })
    .then(function(result) {
        if (result.ok) {
            if (statusEl) statusEl.textContent = '✓ Success! Loading dashboard…';
            setTimeout(function() { window.location.reload(); }, 350);
        } else {
            if (btn) { btn.disabled = false; btn.textContent = 'Apply & Load'; }
            var msg = (result.errors || []).join(' | ') || 'Mapping failed.';
            if (statusEl) statusEl.textContent = '✗ ' + msg;
            if (statusEl) statusEl.style.color = '#c0392b';
        }
    })
    .catch(function(e) {
        if (btn) { btn.disabled = false; btn.textContent = 'Apply & Load'; }
        if (statusEl) statusEl.textContent = '✗ Request failed: ' + e.message;
        if (statusEl) statusEl.style.color = '#c0392b';
    });
}

function closeMappingModal() {
    var modal = document.getElementById('fta-mapping-modal');
    if (modal) modal.style.display = 'none';
    document.body.style.overflow = '';
}

// HTML escaping helpers used by mapping modal JS
function escH(s) {
    return String(s)
        .replace(/&/g,'&amp;').replace(/</g,'&lt;')
        .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function escAttr(s) { return escH(s); }
</script>
"""

    # ── Server-side ticker pre-render (bypasses any client-side fetch issues) ──
    _ticker_init = ''
    try:
        _t_raw = _aggregator.recent_raw(_agg_max_entries) if _aggregator is not None else []
        if not is_all:
            _t_raw = [r for r in _t_raw if classify_shipment({"hs_code": r.hs6}, industry)]
        _t_feed = (
            [_agg_to_feed_entry(r) for r in _t_raw]
            if _t_raw else (get_tariff_feed() if is_all else [])
        )
        if _t_feed:
            _t_parts = []
            for _ti, _tev in enumerate(_t_feed):
                _ts = _tev.get('status') or 'cleared'
                _t_parts.append(
                    f'<span class="ticker-item" onclick="openTfEntry({_ti})" title="Click for details">'
                    f'<button class="tick-dot tick-{_ts}" title="{_ts.upper()}" aria-label="Status: {_ts}"></button>'
                    f' {_tev.get("headline", "")}'
                    f' <span class="ticker-src">via {_tev.get("source", "")}</span>'
                    f'</span><span class="ticker-sep" aria-hidden="true">&middot;</span>'
                )
            _single = ''.join(_t_parts)
            _ticker_init = _single + _single  # doubled for seamless CSS animation loop
    except Exception:
        pass  # feed unavailable — ticker shows "Loading..." default

    _extra_kw = {"ticker_initial": _ticker_init} if _ticker_init else {}
    return render_template_string(
        BASE,
        title="FTA & Preferential Trade Agent",
        css=_CSS,
        sidebar=_sidebar_html("fta_preferential"),
        content=content,
        scripts=scripts,
        industry=industry,
        all_industries=get_industries(),
        topbar_extras=topbar_extras,
        **_extra_kw,
    )


# ---------------------------------------------------------------------------
# Agent detail page
# ---------------------------------------------------------------------------

@app.route("/agent/<agent_id>")
def agent_detail(agent_id):
    industry = _current_industry()
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

    elif agent_id == "tariff_shock":
        # ----------------------------------------------------------------
        # Tariff Shock & Resilience Agent
        # SINGLE-SWITCH ROLLBACK (Adjustment 1):
        #   _tariff_shock_agent is None  iff  tariff_shock.enabled=false in config.yaml
        #   When None -> inactive placeholder rendered HERE, flag alone controls it.
        #   No registry-status flip is needed for rollback.
        # ----------------------------------------------------------------
        if _tariff_shock_agent is None:
            body_html = """
            <div class="coming-soon-box">
              <div class="cs-icon">⚡</div>
              <h2>Tariff Shock Agent &mdash; Inactive</h2>
              <p>Set <code>tariff_shock.enabled: true</code> in
                 <strong>config.yaml</strong> and restart the app to activate
                 real-time exposure quantification for this agent.</p>
            </div>
            """
            scripts = ""
        else:
            body_html = """
            <div class="section-card" style="margin-bottom:20px;border-top:3px solid #12B3A3;">
              <div class="section-card-header" style="color:#12B3A3;">
                ⚡ Exposure Quantification &mdash; Step 2 (Live)
              </div>
              <div style="padding:14px 20px;font-size:0.78rem;color:#666;
                          border-bottom:1px solid #f0eaf8;">
                <strong>Data source:</strong> aggregator rate-change events
                &nbsp;|&nbsp;
                <strong>Volume:</strong>
                <span style="color:#F76C6C;font-weight:600;">ILLUSTRATIVE stub values
                &mdash; not live ERP data</span>
              </div>
              <div id="ts-alerts-container" style="padding:12px 20px;">
                <em style="color:#aaa;font-size:0.82rem;">
                  Waiting for aggregator rate-change events&hellip;
                  Alerts appear here when the scheduler detects a material change.
                </em>
              </div>
            </div>

            <div class="section-card">
              <div class="section-card-header">
                📊 Per-Lane Exposure Report
              </div>
              <div id="ts-report-container" style="padding:12px 20px;">
                <em style="color:#aaa;font-size:0.82rem;">
                  No exposure data yet.
                </em>
              </div>
            </div>

            <div class="section-card" style="opacity:0.55;">
              <div class="section-card-header">
                🔮 Step 1 &mdash; NLP Signal Ingestion <span style="font-weight:400;color:#aaa;">(stub)</span>
              </div>
              <div style="padding:20px;color:#aaa;font-size:0.82rem;">
                TODO: parse news/policy signals to anticipate rate changes before
                the scheduler detects them.
              </div>
            </div>
            <div class="section-card" style="opacity:0.55;">
              <div class="section-card-header">
                🌏 Step 3 &mdash; Sourcing / China+1 Modelling <span style="font-weight:400;color:#aaa;">(stub)</span>
              </div>
              <div style="padding:20px;color:#aaa;font-size:0.82rem;">
                TODO: propose alternative origin countries for high-exposure lanes.
              </div>
            </div>
            <div class="section-card" style="opacity:0.55;">
              <div class="section-card-header">
                📋 Step 4 &mdash; Playbooks / Filings <span style="font-weight:400;color:#aaa;">(stub)</span>
              </div>
              <div style="padding:20px;color:#aaa;font-size:0.82rem;">
                TODO: generate binding rulings, FTA cert renewals, duty-relief petitions.
              </div>
            </div>
            """
            scripts = """
            <script>
            (function() {
              function renderAlerts(alerts) {
                var c = document.getElementById('ts-alerts-container');
                if (!c) return;
                if (!alerts || alerts.length === 0) {
                  c.innerHTML = '<em style="color:#aaa;font-size:0.82rem;">No alerts yet — waiting for rate-change events.</em>';
                  return;
                }
                var html = '';
                alerts.forEach(function(a) {
                  var dot = 'sev-' + a.severity;
                  html += '<div class="alert-row">'
                    + '<div class="sev-dot ' + dot + '"></div>'
                    + '<div class="alert-msg">' + a.message + '</div>'
                    + '<div class="alert-ts">' + a.timestamp + '</div>'
                    + '</div>';
                });
                c.innerHTML = html;
              }

              function renderReport(reports) {
                var c = document.getElementById('ts-report-container');
                if (!c) return;
                var keys = Object.keys(reports || {});
                if (keys.length === 0) {
                  c.innerHTML = '<em style="color:#aaa;font-size:0.82rem;">No exposure data yet.</em>';
                  return;
                }
                var html = '<table style="width:100%;font-size:0.78rem;border-collapse:collapse;">';
                html += '<tr style="background:#f9f6ff;font-weight:700;">'
                  + '<th style="padding:8px;text-align:left;">Lane</th>'
                  + '<th style="padding:8px;text-align:right;">Old eff.%</th>'
                  + '<th style="padding:8px;text-align:right;">New eff.%</th>'
                  + '<th style="padding:8px;text-align:right;">Delta pp</th>'
                  + '<th style="padding:8px;text-align:right;">Exposure/yr</th>'
                  + '<th style="padding:8px;text-align:left;">Note</th>'
                  + '</tr>';
                keys.forEach(function(k) {
                  var r = reports[k];
                  var dirColor = r.direction === 'increase' ? '#F76C6C'
                               : r.direction === 'decrease' ? '#12B3A3' : '#888';
                  var expStr = r.review_flag
                    ? '<span style="color:#F5A623;">Manual review</span>'
                    : (r.exposure_amount !== null
                        ? '<span style="color:' + dirColor + ';font-weight:600;">$'
                          + (r.exposure_amount / 1000).toFixed(0) + 'K</span>'
                        : '—');
                  var delta = r.delta_pct !== null
                    ? (r.delta_pct > 0 ? '+' : '') + r.delta_pct.toFixed(2)
                    : '—';
                  html += '<tr style="border-top:1px solid #f0eaf8;">'
                    + '<td style="padding:8px;">' + r.hs6 + ' ' + r.origin + '->' + r.destination + '</td>'
                    + '<td style="padding:8px;text-align:right;">' + (r.old_effective_rate !== null ? r.old_effective_rate.toFixed(2) + '%' : '—') + '</td>'
                    + '<td style="padding:8px;text-align:right;">' + (r.new_effective_rate !== null ? r.new_effective_rate.toFixed(2) + '%' : '—') + '</td>'
                    + '<td style="padding:8px;text-align:right;color:' + dirColor + ';">' + delta + '</td>'
                    + '<td style="padding:8px;text-align:right;">' + expStr + '</td>'
                    + '<td style="padding:8px;color:#aaa;">' + (r.review_reason || r.applicable_fta || '') + '</td>'
                    + '</tr>';
                });
                html += '</table>';
                html += '<div style="font-size:0.65rem;color:#F76C6C;margin-top:8px;">'
                  + '* ILLUSTRATIVE stub volumes — not live ERP data. '
                  + '<a href="/api/tariff-shock" style="color:#0050b3;">Raw JSON</a></div>';
                c.innerHTML = html;
              }

              function loadTariffShock() {
                fetch('/api/tariff-shock', {credentials: 'same-origin'})
                  .then(function(r) { return r.json(); })
                  .then(function(data) {
                    if (data.no_industry_coverage) {
                      var lbl = data.industry_display_name || data.active_industry || 'this industry';
                      var ac = document.getElementById('ts-alerts-container');
                      var rc = document.getElementById('ts-report-container');
                      if (ac) ac.innerHTML = '<em style="color:#aaa;font-size:0.82rem;">No exposure alerts for ' + lbl + ' yet — coverage expanding.</em>';
                      if (rc) rc.innerHTML = '<em style="color:#aaa;font-size:0.82rem;">No exposure data for ' + lbl + '.</em>';
                      return;
                    }
                    renderAlerts(data.alerts);
                    renderReport(data.reports);
                  })
                  .catch(function(e) { console.error('tariff-shock fetch error:', e); });
              }

              // Load tariff feed (right pane)
              function loadTariffFeed() {
                fetch('/api/tariff-feed', {credentials: 'same-origin'})
                  .then(function(r) { return r.json(); })
                  .then(function(data) {
                    var sc = document.getElementById('sources-container');
                    if (sc && data.sources.length > 0) {
                      var sh = '<div class="sources-title">Source Monitoring</div>';
                      data.sources.forEach(function(s) {
                        sh += '<div class="source-row"><span class="source-icon">' + s.icon + '</span>'
                          + '<span class="source-name">' + s.name + '</span>'
                          + '<span class="source-badge">' + s.status + '</span></div>';
                      });
                      sc.innerHTML = sh;
                    }
                    var fc = document.getElementById('tariff-feed-container');
                    if (fc && data.feed.length > 0) {
                      var fh = '<div class="feed-section-label">Recent Feed</div>';
                      data.feed.forEach(function(ev) {
                        var sc2 = 'status-' + ev.status;
                        fh += '<div class="tariff-event"><div class="tariff-event-header">'
                          + '<div class="tariff-headline">' + ev.headline + '</div>'
                          + '<span class="tariff-status-badge ' + sc2 + '">' + ev.status + '</span>'
                          + '</div><div class="tariff-time">' + ev.time_short + '</div>'
                          + '<div class="tariff-detail">' + ev.detail + '</div>'
                          + '<div class="tariff-source">via ' + ev.source + '</div></div>';
                      });
                      fc.innerHTML = fh;
                    }
                  });
              }

              loadTariffShock();
              loadTariffFeed();
              setInterval(loadTariffShock, 10000);
              setInterval(loadTariffFeed, 10000);
            })();
            </script>
            """

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
          <div class="ai-icon"><i data-lucide="bot"></i></div>
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
        industry=industry,
        all_industries=get_industries(),
    )


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    app.logger.setLevel(logging.INFO)
    app.run(debug=False, port=5000, threaded=True, use_reloader=False)
