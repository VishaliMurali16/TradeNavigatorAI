#!/usr/bin/env python3
"""
Demo dashboard runner for TradeNavigatorAI.

Usage (from project root):
    python scripts/demo_dashboard.py

What it does
------------
1. Starts the real Flask app on port 5000 with NO live connector calls.
2. Pre-seeds the rate store with 3 lanes (via DemoConnector) so the feed
   shows data immediately on page load, with no 30-second warm-up delay.
3. Opens the Tariff Shock Agent tab in the browser.
4. ~15 s later, sets the DemoConnector to return Section 301 25% on the
   KR->US lane, then calls scheduler.refresh_now() — the REAL scheduler
   change-detection path:
       DemoConnector.fetch() -> reconciler -> store.upsert()
       -> _rate_data_changed() -> _notify() -> TariffShockAgent.on_rate_change()
       -> compute_exposure() -> alert cached
5. The browser's 10-second poll cycle picks up the alert automatically.

No existing files are modified. Press Ctrl+C to stop.
"""

from __future__ import annotations

import os
import re
import sys
import time
import threading
import webbrowser
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# 0. Paths — app.py opens "config.yaml" with a bare relative path, so cwd
#    must be the "Global trade agent" directory before any app import.
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent          # .../TradeNavigatorAI/scripts
APP_DIR = HERE.parent / "Global trade agent"    # .../TradeNavigatorAI/Global trade agent

if not APP_DIR.is_dir():
    sys.exit(f"ERROR: app directory not found at {APP_DIR}")

os.chdir(APP_DIR)
sys.path.insert(0, str(APP_DIR))


def _ts(msg: str) -> None:
    """Print a timestamped status line to stdout."""
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


_ts("=== TradeNavigator AI  Demo Dashboard ===")
_ts("Offline mode — DemoConnector replaces all live USITC / WITS calls")

# ---------------------------------------------------------------------------
# 1. Define DemoConnector and patch the aggregator BEFORE importing app.
#
#    app.py calls _boot_aggregator() at module load time, which instantiates
#    AggregatorAgent via _build_connectors(). Patching _build_connectors here
#    ensures the demo connector is used from the first warm-up refresh.
#
#    _DEMO_PHASE controls what DemoConnector.fetch() returns:
#      0 = baseline (no remedies)
#      1 = KR->US lane now carries Section 301 25%
#    It is read (not written) inside fetch(), so no `global` needed there.
# ---------------------------------------------------------------------------

from aggregator.connectors.base import BaseConnector  # noqa: E402
from aggregator.models import RawRate                  # noqa: E402

_DEMO_PHASE = 0  # written once at injection time; read by DemoConnector.fetch()


class DemoConnector(BaseConnector):
    """
    Deterministic offline connector serving pre-baked CanonicalRate data.

    Named "Demo HTS" so the UI shows an honest source label rather than
    pretending to be the real US HTS connector.

    is_authoritative_for("US") returns True so the reconciler assigns the
    authoritative_present confidence band (0.90) — the same band the real
    US HTS connector earns.
    """

    name = "Demo HTS"
    _AUTH_DESTINATIONS = {"US"}

    def __init__(self) -> None:
        pass  # no HTTP setup; override BaseConnector.__init__

    def is_authoritative_for(self, destination: str) -> bool:
        return destination.upper() in self._AUTH_DESTINATIONS

    def fetch(self, hs_code, origin, destination, effective_date) -> list[RawRate]:
        hs6 = re.sub(r"[^\d]", "", hs_code)[:6]
        o = origin.upper()
        d = destination.upper()
        now = datetime.now(UTC)

        # HS 8471.30 VN->US  MFN 0.0%  (laptop, no FTA or remedy in baseline)
        if hs6 == "847130" and o == "VN" and d == "US":
            return [RawRate(
                hs_code="847130", origin="VN", destination="US",
                effective_date=effective_date, mfn_rate=0.0,
                source="Demo HTS", fetched_at=now,
            )]

        # HS 8471.30 CN->US  MFN 0.0%
        if hs6 == "847130" and o == "CN" and d == "US":
            return [RawRate(
                hs_code="847130", origin="CN", destination="US",
                effective_date=effective_date, mfn_rate=0.0,
                source="Demo HTS", fetched_at=now,
            )]

        # HS 0406.90 KR->US  MFN 7.2%, KORUS preferential 0.0%
        # Phase 1: Section 301 25% is added, triggering the Tariff Shock alert.
        if hs6 == "040690" and o == "KR" and d == "US":
            remedies = ["Section 301 25%"] if _DEMO_PHASE >= 1 else []
            return [RawRate(
                hs_code="040690", origin="KR", destination="US",
                effective_date=effective_date,
                mfn_rate=7.2,
                preferential_rate=0.0, applicable_fta="KORUS",
                active_remedies=remedies,
                source="Demo HTS", fetched_at=now,
            )]

        # Unknown lane — reachable but no data (treated as empty, not an error)
        return []


# Patch _build_connectors before app.py's module-level _boot_aggregator() runs.
import aggregator.aggregator_agent as _agg_mod  # noqa: E402

_agg_mod._build_connectors = lambda _cfg: [DemoConnector()]

_ts("DemoConnector patched into aggregator (no HTTP calls will be made)")

# ---------------------------------------------------------------------------
# 2. Capture the RateScheduler instance when it calls .start().
#
#    app.py creates the scheduler inside a background thread; we need a
#    reference to call refresh_now() during the injection step.
#    The mutable list _sched_ref avoids a closure/global assignment problem.
# ---------------------------------------------------------------------------

_sched_ref: list = [None]  # _sched_ref[0] = RateScheduler once started

import aggregator.scheduler as _sched_mod  # noqa: E402

_original_start = _sched_mod.RateScheduler.start


def _capture_start(self) -> None:           # replaces RateScheduler.start
    _sched_ref[0] = self
    _original_start(self)


_sched_mod.RateScheduler.start = _capture_start

# ---------------------------------------------------------------------------
# 2b. Stub the 'air.AsyncAIRefinery' symbol if the installed 'air' package
#     does not export it.  app.py uses it only for the /api/posture-summary
#     AI call, which runs in a background thread and gracefully falls back to
#     a static summary — so a no-op stub is safe for the demo.
# ---------------------------------------------------------------------------

import air as _air_pkg  # noqa: E402

if not hasattr(_air_pkg, "AsyncAIRefinery"):
    # app.py calls:  client = AsyncAIRefinery(api_key=...)
    #                await client.chat.completions.create(...)
    # That call runs in a background thread whose exception is caught and
    # falls back to _fallback_summary().  A stub that raises is safe.
    class _AsyncAIRefineryStub:
        def __init__(self, api_key=None):
            self.chat = self
            self.completions = self
        async def create(self, **kwargs):
            raise RuntimeError("AI endpoint stubbed in demo mode")

    _air_pkg.AsyncAIRefinery = _AsyncAIRefineryStub
    _ts("Stubbed air.AsyncAIRefinery (AI endpoint not needed for demo)")

# ---------------------------------------------------------------------------
# 3. Import app — triggers _boot_aggregator() at module load, which:
#      a. Instantiates AggregatorAgent (using patched DemoConnector)
#      b. Starts a daemon thread (_warm_and_start) that:
#           - calls refresh_lanes(): DemoConnector seeds the store with 3 lanes
#           - subscribes TariffShockAgent to the scheduler
#           - calls scheduler.start() (captured above)
# ---------------------------------------------------------------------------

_ts("Importing Flask app (this starts the aggregator warm-up thread) ...")

import app as flask_app  # noqa: E402  — must come AFTER all patches

_ts("App module loaded — warm-up thread is running")

# ---------------------------------------------------------------------------
# 4. Start Flask in a background thread
# ---------------------------------------------------------------------------

_flask_thread = threading.Thread(
    target=lambda: flask_app.app.run(
        debug=False, port=5000, threaded=True, use_reloader=False,
    ),
    daemon=True,
    name="demo-flask",
)
_flask_thread.start()

# ---------------------------------------------------------------------------
# 5. Wait for Flask to bind, then wait for warm-up + TariffShockAgent init
# ---------------------------------------------------------------------------

_ts("Waiting for Flask to bind on port 5000 ...")
_flask_ready = False
for _ in range(120):           # up to 60 s
    try:
        urllib.request.urlopen("http://localhost:5000/", timeout=1)
        _flask_ready = True
        break
    except Exception:
        time.sleep(0.5)

if not _flask_ready:
    _ts("ERROR: Flask did not start within 60 s. Is port 5000 already in use?")
    sys.exit(1)

_ts("Flask live at http://localhost:5000")

_ts("Waiting for aggregator warm-up to finish and TariffShockAgent to initialise ...")
_boot_deadline = time.monotonic() + 20
while True:
    ready = (
        flask_app._tariff_shock_agent is not None   # TariffShockAgent subscribed
        and _sched_ref[0] is not None               # scheduler captured
    )
    if ready:
        break
    if time.monotonic() > _boot_deadline:
        _ts("ERROR: warm-up did not complete within 20 s — check app logs")
        sys.exit(1)
    time.sleep(0.2)

_ts("Seeded 3 lanes via DemoConnector:")
_ts("  HS 8471.30 VN->US  MFN 0.0%")
_ts("  HS 8471.30 CN->US  MFN 0.0%")
_ts("  HS 0406.90 KR->US  MFN 7.2%, preferential 0.0% under KORUS (no remedies yet)")
_ts("TariffShockAgent active and subscribed to the rate scheduler")

# ---------------------------------------------------------------------------
# 6. Open browser to the Tariff Shock Agent tab
# ---------------------------------------------------------------------------

_TARGET_URL = "http://localhost:5000/agent/tariff_shock"
_ts(f"Opening browser: {_TARGET_URL}")
webbrowser.open(_TARGET_URL)

# ---------------------------------------------------------------------------
# 7. Wait, then inject the rate change via the REAL scheduler change path:
#
#    _DEMO_PHASE = 1
#      -> DemoConnector.fetch() for KR now returns ["Section 301 25%"]
#
#    scheduler.refresh_now()
#      -> _refresh_all() -> _refresh_lane() per lane (blocking in this thread)
#      -> agent.query("0406.90", "KR", "US", today)
#      -> DemoConnector.fetch() returns new rate WITH Section 301
#      -> reconciler.reconcile() produces updated CanonicalRate
#      -> store.upsert() persists it
#      -> _rate_data_changed(new, old) == True  (active_remedies changed)
#      -> _notify(new, old)
#      -> TariffShockAgent.on_rate_change(new, old)
#      -> compute_exposure() -> $200K cost increase (ILLUSTRATIVE)
#      -> alert + report cached; browser picks them up on next 10 s poll
# ---------------------------------------------------------------------------

_INJECT_DELAY = 15
_ts(f"Waiting {_INJECT_DELAY} s before injecting rate change ...")
_ts("(Watch the Tariff Shock tab — the UI polls /api/tariff-shock every 10 s)")
time.sleep(_INJECT_DELAY)

_ts("---")
_ts("Injecting rate change: HS 040690 KR->US — Section 301 25% added")
_DEMO_PHASE = 1                     # DemoConnector now returns KR with Section 301

_sched_ref[0].refresh_now()         # blocking: runs the full scheduler refresh cycle

_ts("Rate change processed by scheduler")
_ts("TariffShockAgent produced an alert:")
_ts("  HS 040690 KR->US: 0.00% -> 25.00% (+25.00pp)")
_ts("  Estimated annual COST INCREASE of $200K  [ILLUSTRATIVE — $0.8M stub volume]")
_ts("  Severity: medium | remedy_applicability: assumed_unverified")
_ts("---")
_ts("Alert will appear in the browser within the next 10 s poll cycle.")
_ts(f"If the tab has not auto-updated, refresh: {_TARGET_URL}")
_ts("")
_ts("Press Ctrl+C to stop the demo server.")

# ---------------------------------------------------------------------------
# 8. Keep the process alive (Flask thread is daemon, so we must stay alive)
# ---------------------------------------------------------------------------

try:
    while True:
        time.sleep(5)
except KeyboardInterrupt:
    _ts("Ctrl+C received — shutting down. Goodbye.")
