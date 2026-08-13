"""
SAP GTS connector stub — client-licensed, not available in Phase 1.

When present, SAP GTS is the highest-precedence source (client's own system of
record). fetch() raises NotImplementedError until client credentials and the
production GTS endpoint are configured.
"""

from __future__ import annotations

from datetime import date

from aggregator.connectors.base import BaseConnector
from aggregator.models import RawRate


class SAPGTSConnector(BaseConnector):
    """Stub connector for the client's SAP Global Trade Services instance. Production only."""

    name = "SAP GTS"

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
            "SAP GTS connector is not implemented in Phase 1. "
            "Enable in production after client credentials and endpoint are configured."
        )
