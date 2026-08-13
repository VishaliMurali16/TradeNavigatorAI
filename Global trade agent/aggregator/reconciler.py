"""
Reconciler — merges RawRates from multiple connectors into a single CanonicalRate.

Confidence is weighted by WHICH sources survived, not just how many:

    authoritative source present (alone or agreeing with non-auth)
        → config band: authoritative_present  (~0.90)

    authoritative + non-auth present but disagree
        → config band: sources_disagree       (~0.45)
          pick the authoritative value; record both in disagreement_details

    multiple non-auth sources agree, authoritative unreachable
        → config band: non_authoritative_agree (~0.75)

    only one fallback/global source, authoritative unreachable
        → config band: fallback_only           (~0.50)
          add a note: "authoritative source '<name>' unavailable"

    any non-auth sources disagree (no authoritative present)
        → config band: sources_disagree        (~0.45)
          pick the highest-precedence non-auth value

Cross-reference caveat (US HTS):
    When US HTS is the only source and returns preferential_rate=None,
    the absence may mean an unresolved "See 99xx.xx" quota-schedule cross-
    reference rather than a confirmed absence of FTA benefit.  A note is added
    to disagreement_details so downstream consumers do not assume certainty.

All confidence thresholds and precedence weights are read from the
aggregator: section of config.yaml — nothing is hardcoded.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import yaml

from aggregator.connectors.base import BaseConnector, ConnectorError
from aggregator.models import CanonicalRate, RawRate

# MFN rates within this many percentage points are treated as agreeing.
_RATE_TOLERANCE = 0.1

_US_HTS_XREF_NOTE = (
    "US HTS: preferential rate unavailable — may be defined via a "
    "quota-schedule cross-reference (See 99xx.xx) not resolved in Phase 1; "
    "do not assume no FTA benefit applies."
)


def _config_key(connector_name: str) -> str:
    """Normalise a connector name to the config.yaml precedence key format.

    "US HTS"  → "us_hts"
    "EU TARIC" → "eu_taric"
    "WITS"    → "wits"
    """
    return re.sub(r"[\s\-]+", "_", connector_name).lower()


def load_config() -> dict:
    """Load the aggregator: section from config.yaml (adjacent to this package)."""
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path, encoding="utf-8") as fh:
        full = yaml.safe_load(fh)
    return full.get("aggregator", {})


class Reconciler:
    """
    Combines connector results into one authoritative CanonicalRate per lane.

    Designed to be constructed once (reads config) and called many times.
    All HTTP interaction happens in connectors; the reconciler is pure logic.
    """

    def __init__(self, config: dict | None = None) -> None:
        if config is None:
            config = load_config()
        self._precedence: dict[str, int] = config.get("precedence", {})
        self._confidence: dict[str, float] = config.get("confidence", {})

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def reconcile(
        self,
        hs_code: str,
        origin: str,
        destination: str,
        effective_date: date,
        results: dict[str, list[RawRate]],
        errors: dict[str, ConnectorError],
        connectors: list[BaseConnector],
    ) -> CanonicalRate | None:
        """
        Reconcile connector results into a single CanonicalRate.

        Parameters
        ----------
        hs_code        : queried HS code (any valid format)
        origin         : ISO 3166-1 alpha-2 origin country
        destination    : ISO 3166-1 alpha-2 destination country
        effective_date : effective date for the rates
        results        : {connector_name: [RawRate, ...]} successful fetches
                         (may include empty lists — source reached, no lane data)
        errors         : {connector_name: ConnectorError} failed fetches
        connectors     : all configured connectors (for authority + precedence lookup)

        Returns
        -------
        CanonicalRate | None
            None only when every connector returned [] or errored with no data.
        """
        connector_map: dict[str, BaseConnector] = {c.name: c for c in connectors}

        # Take the first (most specific) RawRate from each successful connector.
        # A connector that returned [] is reachable but had no data for this lane.
        available: dict[str, RawRate] = {
            name: rates[0] for name, rates in results.items() if rates
        }

        if not available:
            return None

        # Audit: every attempted source (success + error + reached-but-empty).
        all_sources_consulted = sorted(set(list(results.keys()) + list(errors.keys())))

        # Partition available data into authoritative vs. non-authoritative.
        auth_data: dict[str, RawRate] = {}
        nonauth_data: dict[str, RawRate] = {}
        for name, rate in available.items():
            conn = connector_map.get(name)
            if conn is not None and conn.is_authoritative_for(destination):
                auth_data[name] = rate
            else:
                nonauth_data[name] = rate

        # Identify authoritative connectors that were attempted but failed.
        auth_errored: list[str] = [
            name for name, conn in connector_map.items()
            if conn.is_authoritative_for(destination) and name in errors
        ]

        disagreement_details: list[str] = []
        best_name: str
        best_rate: RawRate
        confidence: float

        # --- Decision tree ---

        if auth_data:
            best_name, best_rate = self._highest_precedence(auth_data)

            if nonauth_data:
                disagreeing = [
                    name for name, rate in nonauth_data.items()
                    if not self._rates_agree(best_rate, rate)
                ]
                if disagreeing:
                    # Authoritative present but non-auth disagrees.
                    for name in disagreeing:
                        disagreement_details.append(
                            self._format_disagreement(
                                best_name, best_rate, name, nonauth_data[name]
                            )
                        )
                    confidence = self._conf("sources_disagree")
                else:
                    # Authoritative + all non-auth agree (or only auth present).
                    confidence = self._conf("authoritative_present")
            else:
                # Authoritative source alone.
                confidence = self._conf("authoritative_present")

        else:
            # No authoritative data — note any that errored.
            for name in auth_errored:
                disagreement_details.append(
                    f"authoritative source '{name}' unavailable"
                )

            if len(nonauth_data) >= 2:
                primary_name, primary_rate = self._highest_precedence(nonauth_data)
                all_agree = all(
                    self._rates_agree(primary_rate, nonauth_data[n])
                    for n in nonauth_data
                    if n != primary_name
                )
                if all_agree:
                    confidence = self._conf("non_authoritative_agree")
                else:
                    confidence = self._conf("sources_disagree")
                    for name, rate in nonauth_data.items():
                        if name != primary_name and not self._rates_agree(primary_rate, rate):
                            disagreement_details.append(
                                self._format_disagreement(
                                    primary_name, primary_rate, name, rate
                                )
                            )
                best_name, best_rate = primary_name, primary_rate
            else:
                # Single fallback/global source.
                confidence = self._conf("fallback_only")
                best_name, best_rate = self._highest_precedence(nonauth_data)

        # Cross-reference caveat: US HTS only, preferential fields absent.
        if (
            len(available) == 1
            and best_name == "US HTS"
            and best_rate.preferential_rate is None
            and best_rate.preferential_expression is None
        ):
            disagreement_details.append(_US_HTS_XREF_NOTE)

        # MFN backfill: if the winning source is completely silent on MFN
        # (mfn_rate=None AND duty_expression=None) but another source has a
        # real value, take the highest-precedence real value rather than emitting
        # a high-confidence record with no rate in it.
        #
        # Confidence adjustment: when the fill source is non-authoritative, the
        # auth source contributed no rate data — clamp to fallback_only.  If the
        # fill source is itself authoritative (rare; requires two auth connectors
        # for the same destination), the original confidence band stands.
        out_mfn = best_rate.mfn_rate
        out_duty = best_rate.duty_expression
        mfn_fill_source: str | None = None

        if out_mfn is None and out_duty is None:
            mfn_candidates = sorted(
                ((n, r) for n, r in available.items() if n != best_name),
                key=lambda kv: self._get_precedence(kv[0]),
                reverse=True,
            )
            for fill_name, fill_rate in mfn_candidates:
                if fill_rate.mfn_rate is not None or fill_rate.duty_expression is not None:
                    out_mfn = fill_rate.mfn_rate
                    out_duty = fill_rate.duty_expression
                    mfn_fill_source = fill_name
                    break

        if mfn_fill_source is not None:
            fill_conn = connector_map.get(mfn_fill_source)
            fill_is_auth = fill_conn is not None and fill_conn.is_authoritative_for(destination)
            if not fill_is_auth:
                confidence = self._conf("fallback_only")

        # Preferential backfill: if the winning source has no preferential data
        # but another source does, use it rather than silently dropping the FTA
        # rate.  Confidence is not lowered — the MFN is settled; the pref is a
        # supplement, not a contradiction.
        pref_rate = best_rate.preferential_rate
        pref_expr = best_rate.preferential_expression
        pref_fta = best_rate.applicable_fta
        pref_fill_source: str | None = None

        if pref_rate is None and pref_expr is None:
            # Scan all other available sources by descending precedence.
            pref_candidates = sorted(
                ((n, r) for n, r in available.items() if n != best_name),
                key=lambda kv: self._get_precedence(kv[0]),
                reverse=True,
            )
            for fill_name, fill_rate in pref_candidates:
                if fill_rate.preferential_rate is not None or fill_rate.preferential_expression is not None:
                    pref_rate = fill_rate.preferential_rate
                    pref_expr = fill_rate.preferential_expression
                    pref_fta = fill_rate.applicable_fta
                    pref_fill_source = fill_name
                    break

        # If a pref backfill occurred, remove the cross-reference caveat — we now
        # have an actual preferential value and the caveat no longer applies.
        if pref_fill_source is not None:
            disagreement_details = [
                d for d in disagreement_details if d != _US_HTS_XREF_NOTE
            ]

        return CanonicalRate(
            hs_code=best_rate.hs_code,
            origin=best_rate.origin,
            destination=best_rate.destination,
            effective_date=best_rate.effective_date,
            mfn_rate=out_mfn,
            duty_expression=out_duty,
            preferential_rate=pref_rate,
            preferential_expression=pref_expr,
            applicable_fta=pref_fta,
            rules_of_origin=best_rate.rules_of_origin,
            active_remedies=list(best_rate.active_remedies),
            best_source=best_name,
            sources_consulted=all_sources_consulted,
            confidence=confidence,
            disagreement_details=disagreement_details,
            fetched_at=best_rate.fetched_at,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _conf(self, band: str) -> float:
        """Look up a confidence band from config; fall back to 0.5 if missing."""
        return self._confidence.get(band, 0.5)

    def _get_precedence(self, name: str) -> int:
        return self._precedence.get(_config_key(name), 0)

    def _highest_precedence(self, data: dict[str, RawRate]) -> tuple[str, RawRate]:
        """Return the (name, rate) pair with the highest configured precedence score."""
        return max(data.items(), key=lambda kv: self._get_precedence(kv[0]))

    def _rates_agree(self, r1: RawRate, r2: RawRate) -> bool:
        """True when the two rates do not actively contradict each other on MFN.

        Silence (None) is not a position — a source that has no MFN value cannot
        be counted as *disagreeing* with one that does.  Only when both sources
        have a concrete, comparable value and those values differ is the result
        a real disagreement.

        Comparison rules:
        - Either side silent (mfn_rate=None, duty_expression=None): not a conflict → True
        - Both ad-valorem:    |r1 - r2| <= _RATE_TOLERANCE
        - Both specific duty: equal duty_expression strings
        - Mixed (one ad-valorem, one specific): cannot confirm — False
        """
        r1_silent = r1.mfn_rate is None and r1.duty_expression is None
        r2_silent = r2.mfn_rate is None and r2.duty_expression is None
        if r1_silent or r2_silent:
            return True  # silence is not a contradiction

        if r1.mfn_rate is not None and r2.mfn_rate is not None:
            return abs(r1.mfn_rate - r2.mfn_rate) <= _RATE_TOLERANCE
        if r1.mfn_rate is None and r2.mfn_rate is None:
            # Both specific-duty
            return r1.duty_expression == r2.duty_expression
        return False  # one ad-valorem, one specific — not confirmably agreeing

    def _format_disagreement(
        self,
        name_a: str,
        rate_a: RawRate,
        name_b: str,
        rate_b: RawRate,
    ) -> str:
        def _str(r: RawRate) -> str:
            if r.mfn_rate is not None:
                return f"{r.mfn_rate}%"
            return r.duty_expression or "N/A"

        return f"{name_a} reported {_str(rate_a)}, {name_b} reported {_str(rate_b)}"
