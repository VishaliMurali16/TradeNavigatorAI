"""
MacMap connector stub — not implemented in Phase 1.

Global aggregator (no authoritative destinations by default). fetch() raises
NotImplementedError until access credentials and endpoint are available.
"""

from __future__ import annotations

from datetime import date

from aggregator.connectors.base import BaseConnector
from aggregator.models import RawRate


class MacMapConnector(BaseConnector):
    """Stub connector for the ITC MacMap tariff database. Phase 2."""

    name = "MacMap"

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int,
        authoritative_for: list[str],
    ) -> None:
        self._authoritative_for = {d.upper() for d in authoritative_for}

    def is_authoritative_for(self, destination: str) -> bool:
        return destination.upper() in self._authoritative_for

    def fetch(
        self,
        hs_code: str,
        origin: str,
        destination: str,
        effective_date: date,
    ) -> list[RawRate]:
        raise NotImplementedError(
            "MacMap connector is not implemented in Phase 1. "
            "Enable after ITC access credentials are provisioned."
        )
