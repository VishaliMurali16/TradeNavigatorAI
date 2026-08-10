"""
AggregatorAgent — the single public interface for tariff reference data.

All other agents (compliance, duty-optimisation, supply-chain) call
AggregatorAgent.query() instead of touching connectors or the store directly.

Responsibilities
----------------
1. Instantiate enabled connectors from config.yaml at startup.
2. On each query: run every enabled connector, catch ConnectorError per-
   connector (never crashing the whole request), pass results + errors to
   the Reconciler, upsert the CanonicalRate to the store, return it.
3. Return None only when every connector produced no data for this lane.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from aggregator.connectors.base import BaseConnector, ConnectorError
from aggregator.connectors.eu_taric import EUTARICConnector
from aggregator.connectors.macmap import MacMapConnector
from aggregator.connectors.sap_gts import SAPGTSConnector
from aggregator.connectors.us_hts import USHTSConnector
from aggregator.connectors.wits import WITSConnector
from aggregator.feed_adapter import to_feed_entry
from aggregator.models import CanonicalRate
from aggregator.reconciler import Reconciler, load_config
from aggregator.store import RateStore

_log = logging.getLogger(__name__)

# Maps the config `sources` key to the concrete connector class.
# All connector constructors share the same signature:
#   (base_url: str, timeout_seconds: int, authoritative_for: list[str])
_CONNECTOR_REGISTRY: dict[str, type[BaseConnector]] = {
    "us_hts": USHTSConnector,
    "wits": WITSConnector,
    "eu_taric": EUTARICConnector,
    "macmap": MacMapConnector,
    "sap_gts": SAPGTSConnector,
}


def _build_connectors(sources_cfg: dict[str, Any]) -> list[BaseConnector]:
    """Instantiate every connector whose `enabled` flag is True in config."""
    connectors: list[BaseConnector] = []
    for key, cfg in sources_cfg.items():
        if not cfg.get("enabled", False):
            continue
        cls = _CONNECTOR_REGISTRY.get(key)
        if cls is None:
            _log.warning("Unknown connector %r in config.yaml sources — skipped", key)
            continue
        try:
            connectors.append(cls(
                base_url=cfg.get("base_url", ""),
                timeout_seconds=int(cfg.get("timeout_seconds", 30)),
                authoritative_for=list(cfg.get("authoritative_for", [])),
            ))
        except Exception:
            _log.exception("Failed to instantiate connector %r — skipped", key)
    return connectors


class AggregatorAgent:
    """
    Single query interface for tariff reference data.

    Typical usage
    -------------
        agent = AggregatorAgent(db_path="aggregator/data/rates.db")
        rate  = agent.query("8471.30", "VN", "US", date.today())
        # rate is CanonicalRate | None

    The agent is designed to be constructed once and reused across many
    queries.  Thread-safe: query() is re-entrant because the store's
    file-backed _connect() opens a fresh connection per call, and the
    reconciler carries no mutable state.
    """

    def __init__(
        self,
        db_path: str,
        config: dict | None = None,
    ) -> None:
        """
        Parameters
        ----------
        db_path : path to the SQLite rate store, or ':memory:' for tests.
        config  : aggregator config dict (the `aggregator:` section of
                  config.yaml).  When None, loaded from the file on disk.
        """
        if config is None:
            config = load_config()
        self._config = config
        self._connectors = _build_connectors(config.get("sources", {}))
        self._reconciler = Reconciler(config=config)
        self._store = RateStore(db_path)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def query(
        self,
        hs_code: str,
        origin: str,
        destination: str,
        effective_date: date,
    ) -> CanonicalRate | None:
        """
        Fetch, reconcile, store, and return the canonical tariff rate for a lane.

        Each connector is run independently.  ConnectorError (and any other
        unexpected exception) from one connector is caught and logged; the
        query continues with the remaining connectors.  A partial result from
        fewer sources is always better than a crash.

        Parameters
        ----------
        hs_code        : HTS/HS code in any valid format ('8471.30', '847130', …)
        origin         : ISO 3166-1 alpha-2 or alpha-3 origin country
        destination    : ISO 3166-1 alpha-2 or alpha-3 destination country
        effective_date : rate effective date

        Returns
        -------
        CanonicalRate | None
            None only when every connector returned no data for this lane.
        """
        results: dict[str, list] = {}
        errors: dict[str, ConnectorError] = {}

        for connector in self._connectors:
            try:
                rates = connector.fetch(
                    hs_code=hs_code,
                    origin=origin,
                    destination=destination,
                    effective_date=effective_date,
                )
                results[connector.name] = rates
                _log.debug(
                    "Connector %r returned %d rate(s) for %s %s→%s",
                    connector.name, len(rates), hs_code, origin, destination,
                )
            except ConnectorError as exc:
                _log.warning("Connector %r failed: %s", connector.name, exc)
                errors[connector.name] = exc
            except Exception as exc:
                # Wrap unexpected errors so the reconciler sees a typed failure.
                _log.exception("Unexpected error from connector %r", connector.name)
                errors[connector.name] = ConnectorError(connector.name, exc)

        canonical = self._reconciler.reconcile(
            hs_code=hs_code,
            origin=origin,
            destination=destination,
            effective_date=effective_date,
            results=results,
            errors=errors,
            connectors=self._connectors,
        )

        if canonical is not None:
            self._store.upsert(canonical)

        return canonical

    def refresh_lanes(self, lanes: list[dict]) -> None:
        """Query each lane and upsert the result.  One lane's failure never aborts the rest.

        Used at startup (warm the store before the first scheduler tick) and
        can be called manually to force an immediate refresh.  Each lane dict
        must have keys ``hs_code``, ``origin``, ``destination``.  The
        ``effective_date`` key is optional; if absent, ``date.today()`` is used.
        """
        for lane in lanes:
            try:
                eff_date = lane.get("effective_date") or date.today()
                self.query(lane["hs_code"], lane["origin"], lane["destination"], eff_date)
            except Exception:
                _log.exception("refresh_lanes: lane %r failed", lane)

    def recent_feed(self, limit: int) -> list[dict]:
        """Return up to limit recent CanonicalRates as feed-adapter dicts.

        Reads from the on-disk store (file-backed WAL path) so the Flask
        route thread sees rates that the scheduler thread has written.  An
        empty list is returned when the store has no records yet — the caller
        should fall back to the simulator in that case.
        """
        return [to_feed_entry(r) for r in self._store.list_recent(limit)]

    def recent_raw(self, limit: int) -> list:
        """Return up to `limit` recent CanonicalRate objects, unformatted.

        Used by the tariff-feed route to filter by industry on structured HS
        fields (rate.hs6) BEFORE calling to_feed_entry() — avoids string-
        parsing the feed headline to infer HS scope.
        """
        return list(self._store.list_recent(limit))

    @property
    def store(self) -> RateStore:
        """Direct access to the rate store (used by the scheduler)."""
        return self._store

    @property
    def connectors(self) -> list[BaseConnector]:
        """Enabled connectors, in config order (used for testing/introspection)."""
        return list(self._connectors)
