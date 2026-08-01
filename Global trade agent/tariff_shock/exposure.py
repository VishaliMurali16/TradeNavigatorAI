"""
Exposure quantification — Tariff Shock & Resilience Agent, Step 2.

This module is pure computation: no I/O, no side effects, no network calls.
It can be unit-tested entirely offline.

HONESTY CONSTRAINT
------------------
stub_value_usd in every ExposureResult is an ILLUSTRATIVE annual shipment value
from the config stub, NOT a live ERP figure.  Callers must propagate that label
in every user-visible surface.  This module enforces nothing beyond naming; the
alert adapter enforces the label in message text.

Effective rate semantics (core trade logic)
-------------------------------------------
The duty cost a shipper actually incurs is the EFFECTIVE rate, not the MFN rate
alone.  Two adjustments apply:

    1. FTA preference
       When the lane qualifies under an FTA (applicable_fta is set AND
       preferential_rate is a float), the shipper declares at import under the
       preferential rate.  An MFN-only change does NOT move their duty cost.
       Effective base = preferential_rate.

       If the lane has an FTA but preferential_rate is None (non-ad-valorem
       preferential, e.g. a specific-duty pref), we cannot compute a percentage
       delta.  This is treated as a review case — same as a specific MFN duty.

    2. Remedy stacking
       Trade remedies (Section 301, antidumping, CVD, Section 232) are assessed
       at entry on top of the base rate and generally cannot be avoided by FTA
       origin claims.  Each entry in active_remedies is parsed for an explicit
       "X%" figure; all such figures are summed and added to the base.

       SAFETY RULE: if ANY remedy string carries no parseable percentage, the
       entire remedy total is None and the result is flagged for manual review.
       A missed remedy would silently under-state exposure — worse than a flag.

       effective = base + remedy_total

    3. Specific / compound duties
       When mfn_rate is None and the FTA path cannot be used, we have only a
       duty_expression string (e.g. "$1.227/kg").  No percentage delta can be
       computed.  Flag for manual review; NEVER fabricate a number.

SIGN CONVENTION (read before touching the delta arithmetic)
-----------------------------------------------------------
    delta_pct      = new_effective - old_effective   (signed)
    exposure_amount = (delta_pct / 100) * stub_value_usd   (signed)

    positive → rate increased → added COST (exposure)
    negative → rate decreased → SAVING (negative exposure)

Never take abs() before surfacing to a user — a saving reported as "exposure"
is a material misread for a CFO or trade finance desk.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from aggregator.models import CanonicalRate

# ---------------------------------------------------------------------------
# Remedy parsing
# ---------------------------------------------------------------------------

# Matches the FIRST explicit percentage in a remedy string.
# Examples that DO match:
#     "Section 301 25%"          -> 25.0
#     "Antidumping 12.4%"        -> 12.4
#     "CVD 7.68%"                -> 7.68
#     "Section 232 10%"          -> 10.0
# Examples that do NOT match (-> manual review):
#     "AD/CVD — variable, pending investigation"
#     "Safeguard — subject to TRQ allocation"
#     "Provisional measure (unquantified)"
_REMEDY_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")


def _parse_remedy_total(
    remedies: list[str],
) -> tuple[float, str] | tuple[None, str]:
    """
    Sum all parseable remedy percentages from an active_remedies list.

    Returns
    -------
    (total_pct: float, description: str)  — success; total may be 0.0 if list empty
    (None, reason: str)                   — failure; ANY unparseable entry triggers this

    SAFETY: returning None on the first unparseable entry means we never silently
    under-count remedies.  Callers must treat None as a review flag.
    """
    if not remedies:
        return 0.0, ""

    parts: list[str] = []
    total = 0.0
    for r in remedies:
        m = _REMEDY_PCT_RE.search(r)
        if m is None:
            return None, f"remedy not parseable as a percentage: {r!r}"
        pct = float(m.group(1))
        total += pct
        parts.append(f"{r} ({pct}%)")

    return total, " + ".join(parts)


# ---------------------------------------------------------------------------
# Effective rate
# ---------------------------------------------------------------------------

def _effective_rate(rate: CanonicalRate) -> tuple[float | None, str]:
    """
    Compute the ad-valorem effective duty rate for a lane.

    This is the CORE TRADE LOGIC function.  Read the module docstring before
    modifying; every branch has a specific trade-finance reason.

    Returns
    -------
    (effective_pct: float, description: str)
        float — effective duty rate in percentage points (e.g. 25.0 = 25%).
                Includes both base rate and all stacked remedies.

    (None, reason: str)
        Cannot compute an ad-valorem rate.  Triggers a review flag.
        Reasons:
          - specific/compound MFN duty (duty_expression present, mfn_rate absent)
          - FTA present but preferential_rate absent (non-ad-valorem pref)
          - any remedy string carries no parseable percentage

    Effective rate construction
    ---------------------------
    Step 1 — base:
        FTA lane (applicable_fta set AND preferential_rate is a float):
            base = preferential_rate
            Shipper claims pref at import; MFN changes do not affect their cost.

        Non-FTA, or FTA with non-ad-valorem pref (preferential_rate is None):
            base = mfn_rate  when mfn_rate is a float
            base = None      when only duty_expression is available -> review

    Step 2 — remedy stack:
        effective = base + _parse_remedy_total(active_remedies)
        Remedies add on top of base regardless of FTA status.
        Any parse failure -> return None (review).
    """
    # ---- Step 1: base rate ----
    if rate.applicable_fta is not None and rate.preferential_rate is not None:
        # FTA lane, ad-valorem preferential rate.
        base = rate.preferential_rate
        base_desc = f"pref {base}% under {rate.applicable_fta}"
    elif rate.mfn_rate is not None:
        # No FTA (or FTA with non-ad-valorem pref — treat as MFN).
        base = rate.mfn_rate
        base_desc = f"MFN {base}%"
    else:
        # Specific or compound duty; duty_expression holds the verbatim string.
        expr = rate.duty_expression or "unknown"
        return None, (
            f"specific/compound duty ({expr!r}); "
            "cannot compute ad-valorem delta — manual review required"
        )

    # ---- Step 2: remedy stack ----
    remedy_total, remedy_desc = _parse_remedy_total(rate.active_remedies)
    if remedy_total is None:
        return None, remedy_desc  # propagate the unparseable-remedy reason

    effective = base + remedy_total
    if remedy_desc:
        return effective, f"{base_desc} + remedies [{remedy_desc}] = {effective}%"
    return effective, f"{base_desc} (no remedies) = {effective}%"


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class ExposureResult:
    """
    Structured output of compute_exposure().

    SIGN CONVENTION — see module docstring.  Never take abs() of exposure_amount.
    STUB LABEL — stub_value_usd is illustrative; propagate the label in all output.
    """

    lane_key: str                        # "{hs6}_{origin}_{destination}"
    hs6: str
    origin: str
    destination: str

    # --- rate snapshot (both may be None when review_flag is True) ---
    old_effective_rate: Optional[float]
    new_effective_rate: Optional[float]
    old_effective_desc: str              # human-readable breakdown for audit
    new_effective_desc: str

    # --- signed delta ---
    delta_pct: Optional[float]           # new - old, in percentage points; None on review

    # --- exposure (signed USD) ---
    stub_value_usd: float                # ILLUSTRATIVE annual shipment value, NOT live ERP
    exposure_amount: Optional[float]     # signed; None on review
    direction: str                       # "increase" | "decrease" | "no_change" | "review"

    # --- review ---
    review_flag: bool
    review_reason: Optional[str]         # explanation when review_flag is True

    # --- provenance ---
    provenance: str                      # full audit trace for the rate inputs
    applicable_fta: Optional[str]
    old_remedies: list[str] = field(default_factory=list)
    new_remedies: list[str] = field(default_factory=list)
    # Set to "assumed_unverified" when active_remedies were parsed and applied to the
    # effective rate.  Country-scope of remedies (e.g. Section 301 is US–China only)
    # is NOT checked here — the number stands; the certainty does not exceed what
    # was actually verified.  None when no remedies are present or when review_flag
    # is True (review already covers the uncertainty).
    remedy_applicability: str | None = None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_exposure(
    old: CanonicalRate | None,
    new: CanonicalRate,
    stub_value_usd: float,
) -> ExposureResult | None:
    """
    Compute the signed duty-delta P&L exposure for a rate change event.

    Parameters
    ----------
    old            : previously stored CanonicalRate, or None on first-ever fetch.
                     When None there is no prior baseline — returns None (no alert).
    new            : freshly reconciled CanonicalRate from the aggregator.
    stub_value_usd : ILLUSTRATIVE annual shipment value from the config stub.
                     MUST be clearly labeled as such in all user-visible output.

    Returns
    -------
    ExposureResult — computed result; exposure_amount and delta_pct are None when
                     a specific duty or unparseable remedy prevents ad-valorem calc,
                     but review_flag is True in those cases instead.
    None           — old is None (first observation, no delta to compute).

    Sign convention: see module docstring.  Never call abs() on exposure_amount.
    """
    if old is None:
        return None   # no prior baseline; nothing to diff

    lane_key = f"{new.hs6}_{new.origin}_{new.destination}"

    old_rate, old_desc = _effective_rate(old)
    new_rate, new_desc = _effective_rate(new)

    provenance = (
        f"source={new.best_source}; confidence={new.confidence:.2f}; "
        f"old_effective=({old_desc}); new_effective=({new_desc})"
    )

    # Either side unresolvable -> review
    if old_rate is None or new_rate is None:
        review_reason = old_desc if old_rate is None else new_desc
        return ExposureResult(
            lane_key=lane_key,
            hs6=new.hs6,
            origin=new.origin,
            destination=new.destination,
            old_effective_rate=old_rate,
            new_effective_rate=new_rate,
            old_effective_desc=old_desc,
            new_effective_desc=new_desc,
            delta_pct=None,
            stub_value_usd=stub_value_usd,
            exposure_amount=None,
            direction="review",
            review_flag=True,
            review_reason=review_reason,
            provenance=provenance,
            applicable_fta=new.applicable_fta,
            old_remedies=list(old.active_remedies),
            new_remedies=list(new.active_remedies),
            remedy_applicability=None,  # review flag already covers the uncertainty
        )

    # Signed delta: positive = rate increase = added cost
    delta = new_rate - old_rate
    exposure = (delta / 100.0) * stub_value_usd  # signed USD

    if abs(delta) < 1e-9:
        direction = "no_change"
    elif delta > 0:
        direction = "increase"
    else:
        direction = "decrease"

    # Flag assumed applicability whenever any remedy contributed to either effective rate.
    # Country-scope of remedies is NOT checked by this agent; the number stands but
    # certainty is limited.
    old_rems = list(old.active_remedies)
    new_rems = list(new.active_remedies)
    remedy_app = "assumed_unverified" if (old_rems or new_rems) else None

    return ExposureResult(
        lane_key=lane_key,
        hs6=new.hs6,
        origin=new.origin,
        destination=new.destination,
        old_effective_rate=old_rate,
        new_effective_rate=new_rate,
        old_effective_desc=old_desc,
        new_effective_desc=new_desc,
        delta_pct=delta,
        stub_value_usd=stub_value_usd,
        exposure_amount=exposure,
        direction=direction,
        review_flag=False,
        review_reason=None,
        provenance=provenance,
        applicable_fta=new.applicable_fta,
        old_remedies=old_rems,
        new_remedies=new_rems,
        remedy_applicability=remedy_app,
    )
