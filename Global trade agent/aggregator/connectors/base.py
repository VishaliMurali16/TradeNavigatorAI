"""
Abstract base for all tariff-data connectors.

Public API
----------
    ConnectorError  — raised when a source is unreachable or returns an HTTP error
    BaseConnector   — abstract class all connectors must implement
"""

from __future__ import annotations

import abc
from datetime import date

from aggregator.models import RawRate


class ConnectorError(Exception):
    """
    Raised when a connector cannot reach its source or receives an HTTP error.

    Distinct from an empty result (source reachable, no rate for this lane).
    The reconciler uses this to lower confidence and record provenance rather
    than silently treating a failed source as returning no data.

    Attributes
    ----------
    source : str         connector name, e.g. 'US HTS'
    cause  : Exception   the underlying exception
    """

    def __init__(self, source: str, cause: Exception) -> None:
        self.source = source
        self.cause = cause
        super().__init__(f"[{source}] {cause}")


class BaseConnector(abc.ABC):
    """
    Interface every tariff-data connector must implement.

    Connectors are stateless; configuration (URL, timeout, authoritative
    destinations) is injected at construction. Tests mock the HTTP layer
    (requests) rather than sub-classing connectors.

    Subclasses must set the class-level ``name`` attribute.
    """

    name: str  # e.g. "US HTS" — class-level attribute, set in each subclass

    @abc.abstractmethod
    def fetch(
        self,
        hs_code: str,
        origin: str,
        destination: str,
        effective_date: date,
    ) -> list[RawRate]:
        """
        Fetch tariff rates for the given lane.

        Parameters
        ----------
        hs_code        : any valid HS code format (4-10 digits, dotted or plain)
        origin         : ISO 3166-1 alpha-2 or alpha-3 country code
        destination    : ISO 3166-1 alpha-2 or alpha-3 country code
        effective_date : the rate's effective date

        Returns
        -------
        list[RawRate]
            May be empty when the source has no rate for this lane.
            Each element is a fully-validated RawRate.

        Raises
        ------
        ConnectorError
            When the source is unreachable, returns an HTTP error status,
            or times out. The reconciler — not the connector — decides how
            to handle a failed source; a downed authoritative source lowers
            confidence and is recorded in disagreement_details.
        """
        ...

    @abc.abstractmethod
    def is_authoritative_for(self, destination: str) -> bool:
        """
        Return True when this connector is the authoritative source for
        the given destination (ISO 3166-1 alpha-2).

        Used by the reconciler for precedence scoring: a national tariff
        schedule (US HTS, EU TARIC) is authoritative for its destination,
        while a global aggregator (WITS) is authoritative for none.
        """
        ...
