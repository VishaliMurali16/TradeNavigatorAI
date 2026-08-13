"""
RateScheduler — background refresh of configured tariff lanes.

Responsibilities
----------------
1. At startup and on every configured interval, re-query each lane via
   AggregatorAgent.query() so the store stays fresh.
2. Diff the new CanonicalRate against the previously stored one (comparing
   rate fields, not provenance/timestamps).  Notify all subscribed callbacks
   when substantive rate data changes.
3. When a query fails and the stored record is older than
   staleness_threshold_days, mark it stale (is_stale=True) and upsert.

Subscriber contract
-------------------
    def my_callback(new: CanonicalRate, old: CanonicalRate | None) -> None:
        ...
    scheduler.subscribe(my_callback)

    new  — the freshly reconciled rate
    old  — the previously stored rate, or None for first-ever fetch

Callbacks are called synchronously in the scheduler's background thread.
An exception in a callback is logged and swallowed — it must not abort the
refresh cycle.

WITS note
---------
WITS is enabled in config but its API endpoint returns 403 from corporate
networks (see connectors/wits.py).  The scheduler's default lane list
contains US HTS–reachable lanes only (destination=US).  Do not add WITS-
only lanes until access is confirmed.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler

from aggregator.aggregator_agent import AggregatorAgent
from aggregator.models import CanonicalRate
from aggregator.reconciler import load_config

_log = logging.getLogger(__name__)

# Fields that constitute a meaningful rate change.  Provenance fields
# (sources_consulted, confidence, fetched_at) change on every refresh and
# must not trigger spurious callbacks.
_RATE_FIELDS = ("mfn_rate", "duty_expression", "preferential_rate",
                "preferential_expression", "applicable_fta", "active_remedies")


def _rate_data_changed(new: CanonicalRate, old: CanonicalRate | None) -> bool:
    """True when substantive rate fields differ between new and old."""
    if old is None:
        return True  # first-ever fetch for this lane
    return any(getattr(new, f) != getattr(old, f) for f in _RATE_FIELDS)


class RateScheduler:
    """
    Periodically refreshes configured lanes and notifies subscribers of changes.

    Usage
    -----
        agent     = AggregatorAgent(db_path="aggregator/data/rates.db")
        scheduler = RateScheduler(agent)
        scheduler.subscribe(on_rate_change)
        scheduler.start()
        ...
        scheduler.stop()
    """

    def __init__(
        self,
        agent: AggregatorAgent,
        config: dict | None = None,
    ) -> None:
        """
        Parameters
        ----------
        agent  : fully configured AggregatorAgent instance.
        config : aggregator config dict.  When None, loaded from config.yaml.
        """
        if config is None:
            config = load_config()
        refresh_cfg = config.get("refresh", {})
        self._agent = agent
        self._lanes: list[dict] = refresh_cfg.get("lanes", [])
        self._interval_hours: int = int(refresh_cfg.get("interval_hours", 24))
        self._staleness_days: int = int(refresh_cfg.get("staleness_threshold_days", 7))
        self._callbacks: list[Callable[[CanonicalRate, CanonicalRate | None], None]] = []
        self._scheduler = BackgroundScheduler(daemon=True)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def subscribe(
        self,
        callback: Callable[[CanonicalRate, CanonicalRate | None], None],
    ) -> None:
        """Register a callback to be called when a lane's rate data changes."""
        self._callbacks.append(callback)

    def start(self) -> None:
        """Start the background refresh loop.  Returns immediately."""
        self._scheduler.add_job(
            self._refresh_all,
            trigger="interval",
            hours=self._interval_hours,
            id="rate_refresh",
            replace_existing=True,
        )
        self._scheduler.start()
        _log.info(
            "RateScheduler started: %d lane(s), interval=%dh, staleness=%dd",
            len(self._lanes), self._interval_hours, self._staleness_days,
        )

    def stop(self) -> None:
        """Shut down the background scheduler gracefully."""
        self._scheduler.shutdown(wait=False)
        _log.info("RateScheduler stopped")

    def refresh_now(self) -> None:
        """Trigger an immediate refresh of all lanes (blocking, for startup/testing)."""
        self._refresh_all()

    # ------------------------------------------------------------------
    # Internal refresh logic (called from the scheduler thread)
    # ------------------------------------------------------------------

    def _refresh_all(self) -> None:
        _log.debug("Rate refresh cycle starting (%d lanes)", len(self._lanes))
        for lane in self._lanes:
            try:
                self._refresh_lane(lane)
            except Exception:
                _log.exception("Unhandled error refreshing lane %r", lane)

    def _refresh_lane(self, lane: dict) -> None:
        hs_code = lane.get("hs_code", "")
        origin = lane.get("origin", "")
        destination = lane.get("destination", "")
        # effective_date defaults to today; lanes may override with a fixed date.
        raw_date = lane.get("effective_date")
        if isinstance(raw_date, date):
            eff_date = raw_date
        else:
            eff_date = date.today()

        # Capture stored rate BEFORE the refresh so we can diff afterwards.
        old = self._agent.store.get(hs_code, origin, destination, eff_date)

        new = self._agent.query(hs_code, origin, destination, eff_date)

        if new is not None:
            if _rate_data_changed(new, old):
                _log.info(
                    "Rate change detected: %s %s→%s (was %s, now %s)",
                    hs_code, origin, destination,
                    old.mfn_rate if old else "—", new.mfn_rate,
                )
                self._notify(new, old)
        else:
            # Query produced no result — check if stored record is now stale.
            if old is not None and not old.is_stale:
                age = datetime.now(UTC) - old.fetched_at
                if age >= timedelta(days=self._staleness_days):
                    stale = old.model_copy(update={"is_stale": True})
                    self._agent.store.upsert(stale)
                    _log.warning(
                        "Marked stale: %s %s→%s (age %dd)",
                        hs_code, origin, destination, age.days,
                    )

    def _notify(
        self,
        new: CanonicalRate,
        old: CanonicalRate | None,
    ) -> None:
        for cb in self._callbacks:
            try:
                cb(new, old)
            except Exception:
                _log.exception("Subscriber callback %r raised an exception", cb)
