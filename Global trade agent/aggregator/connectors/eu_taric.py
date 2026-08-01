"""
EU TARIC connector stub — not implemented in Phase 1.

Authoritative for EU destinations. fetch() raises NotImplementedError until
the official TARIC data endpoint is confirmed and the connector is built.
"""

from __future__ import annotations

from datetime import date

from aggregator.connectors.base import BaseConnector
from aggregator.models import RawRate


class EUTARICConnector(BaseConnector):
    """Stub connector for the EU TARIC database. Phase 2."""

    name = "EU TARIC"

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
            "EU TARIC connector is not implemented in Phase 1. "
            "Enable after the official TARIC endpoint is confirmed."
        )
