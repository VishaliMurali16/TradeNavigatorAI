"""
TradeNavigator Aggregator Agent.

Single source of truth for external tariff reference data: rates, rules of origin,
and trade remedies. Business agents (FTA, Tariff Shock, Duty Recovery, Control Tower)
query this package rather than fetching and interpreting external sources independently.

Public API
----------
    RawRate       — output of one connector, before reconciliation
    CanonicalRate — reconciled best call, with confidence and provenance
"""

from aggregator.models import CanonicalRate, RawRate

__all__ = ["RawRate", "CanonicalRate"]
