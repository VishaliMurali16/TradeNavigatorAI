"""
TariffShockAgent — Tariff Shock & Resilience Agent, Step 2: Exposure Quantification.

Lifecycle
---------
1. Constructed once at app startup by _boot_tariff_shock() in app.py.
2. Subscribes to RateScheduler.subscribe() so it receives every material rate change.
3. On each change event: compute exposure, produce alert + per-lane report, write to
   thread-safe cache.
4. The Flask /api/tariff-shock route reads the cache — zero live connector calls on
   the poll path.

Thread safety
-------------
on_rate_change() is called in the RateScheduler's background thread.
latest_alerts() and latest_report() are called in Flask request threads.

All shared mutable state (_alerts, _reports) is guarded by self._lock
(threading.Lock).  Reads and writes use a single with-block so there is never a
partial view.

Exception isolation
-------------------
on_rate_change() wraps _process_change() in a try/except so no exception from
this agent can reach the scheduler's refresh cycle.  The scheduler also swallows
callback exceptions, but the belt-and-suspenders guard here means this agent
is safe to subscribe even if the scheduler's outer catch were removed.

Steps not yet implemented (stubs)
----------------------------------
TODO Step 1 — NLP signal ingestion:
    Parse news / policy signals to anticipate rate changes before the scheduler
    detects them.  Plugs in as ingest_signal(signal_text: str) -> None.

TODO Step 3 — Sourcing / China+1 modelling:
    On high-exposure alerts, propose alternative origin countries that reduce the
    effective duty rate.  Triggered inside _process_change() when direction=="increase"
    and |exposure_amount| exceeds a threshold.

TODO Step 4 — Playbook / filings:
    Generate action items (binding ruling applications, first-sale elections, FTA
    cert renewals, Section 301 exclusion petitions) from exposure events.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

from aggregator.models import CanonicalRate
from tariff_shock.alert_adapter import to_alert
from tariff_shock.exposure import ExposureResult, compute_exposure

_log = logging.getLogger(__name__)

_MAX_ALERTS = 50   # rolling window kept in memory


class TariffShockAgent:
    """
    Tariff Shock & Resilience Agent — exposure quantification slice.

    Parameters
    ----------
    stub_volumes : dict mapping "{hs6}_{origin}_{destination}" to an annual shipment
                   value in USD.  ILLUSTRATIVE — must be replaced with a live ERP
                   feed in production.  Keys must match the lane identifiers used by
                   the aggregator (hs6 = first 6 significant digits, uppercase ISO-2
                   country codes, e.g. "847130_VN_US").
    """

    def __init__(self, stub_volumes: dict[str, float]) -> None:
        self._stub_volumes = stub_volumes

        # Thread-safe cache — guarded by _lock.
        # Written by: scheduler thread (via on_rate_change)
        # Read by:    Flask request threads (via latest_alerts / latest_report)
        self._lock = threading.Lock()
        self._alerts: list[dict] = []             # newest first, capped at _MAX_ALERTS
        self._reports: dict[str, dict] = {}       # lane_key -> ExposureResult as dict

    # ------------------------------------------------------------------
    # Scheduler callback — runs in the scheduler's background thread
    # ------------------------------------------------------------------

    def on_rate_change(
        self,
        new: CanonicalRate,
        old: CanonicalRate | None,
    ) -> None:
        """
        RateScheduler subscriber callback.

        Belt-and-suspenders: all logic is wrapped so an exception here can NEVER
        propagate to the scheduler's refresh cycle — even if the scheduler's own
        swallow were removed.
        """
        try:
            self._process_change(new, old)
        except Exception:
            _log.exception(
                "TariffShockAgent.on_rate_change failed for %s %s->%s (swallowed)",
                new.hs6, new.origin, new.destination,
            )

    def _process_change(self, new: CanonicalRate, old: CanonicalRate | None) -> None:
        lane_key = f"{new.hs6}_{new.origin}_{new.destination}"
        stub_vol = self._stub_volumes.get(lane_key, 0.0)

        result = compute_exposure(old, new, stub_vol)
        if result is None:
            # old=None -> first observation, no delta to report
            _log.debug("TariffShockAgent: first observation for %s; baseline recorded", lane_key)
            return

        alert = to_alert(result)
        report_dict = _result_to_dict(result)

        with self._lock:
            self._alerts.insert(0, alert)
            if len(self._alerts) > _MAX_ALERTS:
                self._alerts = self._alerts[:_MAX_ALERTS]
            self._reports[lane_key] = report_dict

        _log.info(
            "TariffShockAgent: %s %s->%s direction=%s delta_pct=%s exposure=%s review=%s",
            new.hs6, new.origin, new.destination,
            result.direction, result.delta_pct, result.exposure_amount, result.review_flag,
        )

        # TODO Step 3: if result.direction == "increase" and abs(result.exposure_amount or 0) > threshold:
        #     source_alternatives(lane_key, result)

        # TODO Step 4: generate_playbook_items(lane_key, result)

    # ------------------------------------------------------------------
    # Public read interface — called from Flask request threads
    # ------------------------------------------------------------------

    def latest_alerts(self, limit: int = 10) -> list[dict]:
        """Return up to limit most-recent alerts, newest first.  Thread-safe."""
        with self._lock:
            return list(self._alerts[:limit])

    def latest_report(self) -> dict[str, Any]:
        """Return a copy of the per-lane exposure report.  Thread-safe."""
        with self._lock:
            return dict(self._reports)

    # TODO Step 1 — NLP signal ingestion hook
    # def ingest_signal(self, signal_text: str) -> None: ...


# ---------------------------------------------------------------------------
# Internal serialisation helper
# ---------------------------------------------------------------------------

def _result_to_dict(r: ExposureResult) -> dict:
    """Convert an ExposureResult to a JSON-serialisable dict for the API response."""
    return {
        "lane_key":           r.lane_key,
        "hs6":                r.hs6,
        "origin":             r.origin,
        "destination":        r.destination,
        "old_effective_rate": r.old_effective_rate,
        "new_effective_rate": r.new_effective_rate,
        "old_effective_desc": r.old_effective_desc,
        "new_effective_desc": r.new_effective_desc,
        "delta_pct":          r.delta_pct,
        "stub_value_usd":     r.stub_value_usd,
        "exposure_amount":    r.exposure_amount,
        "direction":          r.direction,
        "review_flag":        r.review_flag,
        "review_reason":      r.review_reason,
        "provenance":         r.provenance,
        "applicable_fta":     r.applicable_fta,
        "old_remedies":         r.old_remedies,
        "new_remedies":         r.new_remedies,
        "remedy_applicability": r.remedy_applicability,
        "as_of":                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "stub_label":           "ILLUSTRATIVE — config stub value, not live ERP",
    }
