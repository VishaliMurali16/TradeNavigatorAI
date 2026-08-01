"""
Data contract for the Aggregator Agent.

Public types
------------
    RawRate       — what a single connector returns before reconciliation
    CanonicalRate — the reconciled "best call" produced by the reconciler

Internal base (not exported)
-----------------------------
    _BaseRate — shared fields and validators; inherited by both public types

Non-ad-valorem duties
---------------------
US HTS and WITS both contain specific ("6.8¢/kg") and compound ("5% + 2¢/kg")
duties that cannot be represented as a single float. For those cases:
    mfn_rate        is None
    duty_expression holds the verbatim source string
Both can coexist when the connector can compute an ad-valorem equivalent AND
preserve the original expression. A connector must never fabricate a float.

Canonical join key
------------------
_BaseRate.hs6 returns the first 6 significant digits of any HS code format,
so the reconciler treats "8471.30.50.10", "847130", and "8471305010" as the
same lane. Country codes are normalised to ISO 3166-1 alpha-2; alpha-3 codes
are converted via pycountry on ingestion.

CanonicalRate adapter note
--------------------------
The existing UI feed (data_simulator.get_tariff_feed) expects dicts with keys:
    {id, timestamp, time_short, headline, detail, status, source}

CanonicalRate is deliberately shaped so that a thin adapter can map it to that
schema without any information loss:

    headline   <- .summary          one-line human description
    detail     <- .detail_summary   RoO, remedies, disagreement note
    status     <- .feed_status      'cleared' | 'issued'
    source     <- .best_source      authoritative source name
    timestamp  <- .fetched_at       UTC datetime of last reconciliation
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import Optional

import pycountry
from pydantic import BaseModel, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# HS code validation
# ---------------------------------------------------------------------------

# Accepts:
#   plain digits 4–10 chars  e.g. "847130", "8471305010"
#   dotted segments           e.g. "8471.30", "8471.30.50", "8471.30.50.10"
_HS_PATTERN = re.compile(r"^\d{4,10}$|^\d{4}(\.\d{2,})+$")


# ---------------------------------------------------------------------------
# Base model — shared fields and validators
# ---------------------------------------------------------------------------

class _BaseRate(BaseModel):
    """
    Shared fields and cross-field validators for RawRate and CanonicalRate.

    Not part of the public API — import RawRate or CanonicalRate directly.
    """

    hs_code: str = Field(
        description="Harmonised System code, e.g. '8471.30', '847130', or '8471305010'"
    )
    origin: str = Field(
        min_length=2,
        max_length=3,
        description=(
            "Origin country — ISO 3166-1 alpha-2 (e.g. 'VN') or alpha-3 (e.g. 'VNM'). "
            "Alpha-3 codes are converted to alpha-2 on ingestion."
        ),
    )
    destination: str = Field(
        min_length=2,
        max_length=3,
        description=(
            "Destination country — ISO 3166-1 alpha-2 (e.g. 'US') or alpha-3 (e.g. 'USA'). "
            "Alpha-3 codes are converted to alpha-2 on ingestion."
        ),
    )
    effective_date: date = Field(
        description="Date on which this rate is effective"
    )
    mfn_rate: Optional[float] = Field(
        default=None,
        ge=0.0,
        description=(
            "Most Favoured Nation tariff rate as a percentage (4.5 means 4.5%). "
            "None when the duty is specific or compound and cannot be expressed as a "
            "simple ad-valorem rate; see duty_expression for the verbatim source string."
        ),
    )
    duty_expression: Optional[str] = Field(
        default=None,
        description=(
            "Verbatim duty string from the source when the rate is specific or compound, "
            "e.g. '6.8¢/kg' or '5% + 2¢/kg'. None for pure ad-valorem duties. "
            "Connectors must never fabricate a float — use this field instead."
        ),
    )
    preferential_rate: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Preferential duty rate under an FTA, as a percentage",
    )
    preferential_expression: Optional[str] = Field(
        default=None,
        description=(
            "Verbatim preferential duty string when the preferential rate is non-ad-valorem, "
            "e.g. '1.5¢/kg'. Analogous to duty_expression for the MFN rate."
        ),
    )
    applicable_fta: Optional[str] = Field(
        default=None,
        description="Name of the applicable Free Trade Agreement, e.g. 'USMCA'",
    )
    rules_of_origin: Optional[str] = Field(
        default=None,
        description="Rules of origin requirement, e.g. 'RVC 45%' or 'CTH'",
    )
    active_remedies: list[str] = Field(
        default_factory=list,
        description=(
            "Trade remedies currently in effect, "
            "e.g. ['Section 301 25%', 'Antidumping 12%']"
        ),
    )

    # ------------------------------------------------------------------
    # Field validators
    # ------------------------------------------------------------------

    @field_validator("hs_code")
    @classmethod
    def _check_hs_code(cls, v: str) -> str:
        """Normalise whitespace and enforce valid HS code format."""
        cleaned = v.strip()
        if not _HS_PATTERN.match(cleaned):
            raise ValueError(
                f"Invalid HS code '{v}'. "
                "Expected 4–10 digits (e.g. '847130') or dotted segments (e.g. '8471.30')."
            )
        return cleaned

    @field_validator("origin", "destination")
    @classmethod
    def _normalise_country(cls, v: str) -> str:
        """Strip, uppercase; convert ISO 3166-1 alpha-3 to alpha-2 via pycountry.

        Unknown 3-letter codes (custom regions, test values) are kept as-is.
        """
        cleaned = v.strip().upper()
        if len(cleaned) == 3:
            country = pycountry.countries.get(alpha_3=cleaned)
            if country is not None:
                return country.alpha_2
        return cleaned

    # ------------------------------------------------------------------
    # Cross-field validator
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def _check_preferential_pair(self) -> "_BaseRate":
        """
        Enforce the symmetric rule between preferential rate fields and applicable_fta.

        Rule: a preferential rate may be expressed as preferential_rate (numeric),
        preferential_expression (verbatim string), or both. Whenever EITHER is present,
        applicable_fta MUST also be present — and vice versa.

        Valid combinations:
            preferential_rate + applicable_fta
            preferential_expression + applicable_fta
            preferential_rate + preferential_expression + applicable_fta
            all three absent

        Invalid (both raise ValueError):
            applicable_fta alone          — FTA named with no rate (uninterpretable)
            preferential_rate alone        — numeric rate with no FTA to attribute it to
            preferential_expression alone  — expression with no FTA to attribute it to
        """
        has_any_rate = (self.preferential_rate is not None) or (self.preferential_expression is not None)
        has_fta = self.applicable_fta is not None

        if has_any_rate and not has_fta:
            raise ValueError(
                "preferential_rate or preferential_expression requires applicable_fta — "
                "a preferential rate with no FTA name is uninterpretable. "
                f"Got preferential_rate={self.preferential_rate!r}, "
                f"preferential_expression={self.preferential_expression!r}, "
                f"applicable_fta={self.applicable_fta!r}."
            )
        if has_fta and not has_any_rate:
            raise ValueError(
                "applicable_fta requires preferential_rate or preferential_expression — "
                "an FTA name with no rate of any kind is uninterpretable. "
                f"Got applicable_fta={self.applicable_fta!r}."
            )
        return self

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def hs6(self) -> str:
        """First 6 significant digits of the HS code, dots stripped.

        Canonical reconciler join key: '8471.30.50.10', '847130', and '8471305010'
        all return '847130'. A 4-digit heading (e.g. '8471') returns all 4 digits —
        the reconciler must account for headings shorter than 6 digits.
        """
        digits = re.sub(r"[^\d]", "", self.hs_code)
        return digits[:6]


# ---------------------------------------------------------------------------
# RawRate — one connector's output, before reconciliation
# ---------------------------------------------------------------------------

class RawRate(_BaseRate):
    """
    Output of a single connector before reconciliation.

    Connectors must set fields to None for data they cannot determine —
    fabricating values is explicitly forbidden. Use duty_expression (not
    mfn_rate) for specific and compound duty strings.

    Fields inherited from _BaseRate
    --------------------------------
    hs_code, origin, destination, effective_date,
    mfn_rate, duty_expression,
    preferential_rate, preferential_expression, applicable_fta,
    rules_of_origin, active_remedies

    Fields specific to RawRate
    --------------------------
    source     — connector identifier, e.g. 'US HTS' or 'WITS'
    fetched_at — UTC timestamp of when this record was retrieved
    """

    source: str = Field(
        description="Connector that produced this record, e.g. 'US HTS' or 'WITS'"
    )
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp of when this record was fetched from the source",
    )


# ---------------------------------------------------------------------------
# CanonicalRate — reconciled best call
# ---------------------------------------------------------------------------

class CanonicalRate(_BaseRate):
    """
    The reconciled 'best call' produced by the reconciler from one or more RawRates.

    Carries all the information a RawRate does, plus:
    - provenance  (which sources were consulted, whether they agreed)
    - confidence  (numeric 0.0–1.0 score from the reconciler)
    - staleness   (flag set by the scheduler)

    Fields inherited from _BaseRate
    --------------------------------
    hs_code, origin, destination, effective_date,
    mfn_rate, duty_expression,
    preferential_rate, preferential_expression, applicable_fta,
    rules_of_origin, active_remedies

    Fields specific to CanonicalRate
    ---------------------------------
    best_source          — authoritative source chosen by reconciler
    sources_consulted    — every source queried (for audit trail)
    confidence           — reconciler confidence score (0.0–1.0)
    disagreement_details — human-readable notes where sources disagreed
    fetched_at           — UTC timestamp of most recent reconciliation
    is_stale             — True if not refreshed within staleness threshold

    Adapter properties (map to UI feed dict)
    ----------------------------------------
    .summary        -> 'headline'  one-line human description
    .detail_summary -> 'detail'    secondary info (RoO, remedies, note)
    .feed_status    -> 'status'    'cleared' | 'issued'
    .best_source    -> 'source'    authoritative connector name
    .fetched_at     -> 'timestamp' UTC datetime
    .confidence_tier               'high' | 'medium' | 'low'
    """

    best_source: str = Field(
        description="Authoritative source chosen by the reconciler, e.g. 'US HTS'"
    )
    sources_consulted: list[str] = Field(
        default_factory=list,
        description="All connector names queried during reconciliation, for audit",
    )
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="Reconciler confidence score: 0.0 = low uncertainty, 1.0 = full agreement",
    )
    disagreement_details: list[str] = Field(
        default_factory=list,
        description=(
            "Human-readable notes on where sources disagreed, "
            "e.g. ['WITS reported 5.0%, US HTS reported 4.5%']"
        ),
    )
    fetched_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp of the most recent reconciliation run",
    )
    is_stale: bool = Field(
        default=False,
        description="True when the record has not been refreshed within the configured staleness threshold",
    )

    # ------------------------------------------------------------------
    # Confidence tier — drives feed_status and adapter logic
    # ------------------------------------------------------------------

    @property
    def confidence_tier(self) -> str:
        """Coarsen the numeric confidence score into a named tier.

        Returns 'high' (≥0.80), 'medium' (≥0.50), or 'low' (<0.50).
        Thresholds mirror the reconciler's scoring rules:
            all sources agree     → 0.90–1.00 → 'high'
            single source only    → 0.60–0.70 → 'medium'
            sources disagree      → 0.40–0.50 → 'low'
        """
        if self.confidence >= 0.8:
            return "high"
        if self.confidence >= 0.5:
            return "medium"
        return "low"

    # ------------------------------------------------------------------
    # Adapter properties — map directly to UI feed dict fields
    # ------------------------------------------------------------------

    @property
    def feed_status(self) -> str:
        """Map confidence tier to the UI feed's two-state status vocabulary.

        'high'   -> 'cleared'  consistent, authoritative answer
        'medium' -> 'cleared'  single-source answer — usable, less corroborated
        'low'    -> 'issued'   real disagreement — surface as an alert

        Note: 'pending' is styled in the UI CSS but get_tariff_feed() never
        emits it. Intentionally unused until the simulator or live feed produces
        that status value.
        """
        return "issued" if self.confidence_tier == "low" else "cleared"

    @property
    def summary(self) -> str:
        """One-line human-readable description for the feed 'headline' field.

        MFN rate section:
          mfn_rate set          → "MFN 4.5%"
          duty_expression set   → "MFN 6.8¢/kg"
          both absent           → "MFN rate unavailable"

        Preferential section (appended when present):
          preferential_rate set + applicable_fta → "preferential 0.0% under USMCA"
          preferential_expression + applicable_fta → "preferential 1.5¢/kg under USMCA"
        """
        if self.mfn_rate is not None:
            rate_str = f"MFN {self.mfn_rate}%"
        elif self.duty_expression:
            rate_str = f"MFN {self.duty_expression}"
        else:
            rate_str = "MFN rate unavailable"

        base = f"HS {self.hs_code}, {self.origin}→{self.destination}: {rate_str}"

        if self.preferential_rate is not None and self.applicable_fta:
            return f"{base}, preferential {self.preferential_rate}% under {self.applicable_fta}"
        if self.preferential_expression and self.applicable_fta:
            return f"{base}, preferential {self.preferential_expression} under {self.applicable_fta}"
        return base

    @property
    def detail_summary(self) -> str:
        """Secondary detail line for the feed 'detail' field.

        Combines rules of origin, active remedies, and the first disagreement note,
        pipe-separated. Returns 'No additional details' when all are absent.
        """
        parts: list[str] = []
        if self.rules_of_origin:
            parts.append(f"Rule: {self.rules_of_origin}")
        if self.active_remedies:
            parts.append("Remedies: " + "; ".join(self.active_remedies))
        if self.disagreement_details:
            parts.append(f"Note: {self.disagreement_details[0]}")
        return " | ".join(parts) if parts else "No additional details"

    # ------------------------------------------------------------------
    # Convenience constructor
    # ------------------------------------------------------------------

    @classmethod
    def from_raw_rate(
        cls,
        raw: RawRate,
        *,
        confidence: float,
        sources_consulted: list[str] | None = None,
        disagreement_details: list[str] | None = None,
    ) -> "CanonicalRate":
        """Promote a single RawRate to a CanonicalRate.

        Used by the reconciler when only one source is available. All fields
        are passed through unchanged; the caller supplies the confidence score
        and optional provenance overrides.

        Args:
            raw:                  the RawRate to promote
            confidence:           reconciler confidence score (0.0–1.0)
            sources_consulted:    list of all source names queried; defaults to [raw.source]
            disagreement_details: notes on any disagreements; defaults to []
        """
        return cls(
            hs_code=raw.hs_code,
            origin=raw.origin,
            destination=raw.destination,
            effective_date=raw.effective_date,
            mfn_rate=raw.mfn_rate,
            duty_expression=raw.duty_expression,
            preferential_rate=raw.preferential_rate,
            preferential_expression=raw.preferential_expression,
            applicable_fta=raw.applicable_fta,
            rules_of_origin=raw.rules_of_origin,
            active_remedies=list(raw.active_remedies),  # defensive copy
            best_source=raw.source,
            sources_consulted=(
                sources_consulted if sources_consulted is not None else [raw.source]
            ),
            confidence=confidence,
            disagreement_details=disagreement_details or [],
            fetched_at=raw.fetched_at,
        )
