"""
Maps an ExposureResult to the alert dict shape expected by the UI and
data_simulator.get_alerts().

Shape contract
--------------
    {
        "timestamp": "2026-07-31 14:30 UTC",   # strftime("%Y-%m-%d %H:%M UTC")
        "agent_id":  "tariff_shock",
        "severity":  "high" | "medium" | "low",
        "message":   str,
    }

Every message explicitly states that shipment volume is ILLUSTRATIVE when the
computation used stub data.  This label must never be omitted — this output
may reach a CFO dashboard, and a stub exposure reported as a real figure is
a material misrepresentation.

Severity thresholds (absolute annual exposure):
    high   : |exposure| > $500 000
    medium : |exposure| > $100 000
    low    : |exposure| <= $100 000
    (review cases always map to "medium" — attention needed, amount unknown)
"""

from __future__ import annotations

from datetime import datetime, timezone

from tariff_shock.exposure import ExposureResult


def to_alert(result: ExposureResult) -> dict:
    """
    Map an ExposureResult to the get_alerts() alert dict.

    The 'message' field always includes the ILLUSTRATIVE stub label when the
    exposure uses stub volume data.  Never suppress that label.
    """
    now = datetime.now(timezone.utc)
    stub_note = (
        f"[ILLUSTRATIVE — ${result.stub_value_usd / 1_000_000:.1f}M annual "
        "shipment value is a config stub, not live ERP data]"
    )

    if result.review_flag:
        severity = "medium"
        message = (
            f"Tariff change on HS {result.hs6} {result.origin}->{result.destination} "
            f"requires manual review: {result.review_reason}. "
            f"Exposure cannot be computed automatically. {stub_note}"
        )
    else:
        assert result.delta_pct is not None        # guaranteed when review_flag is False
        assert result.exposure_amount is not None

        # Severity reflects RISK for cost increases only.
        # Savings (direction="decrease") and no-change events are never threat-level:
        # a large saving reported as "high severity" reads as a threat when it is not.
        if result.direction in ("decrease", "no_change"):
            severity = "low"
        else:
            abs_k = abs(result.exposure_amount) / 1_000
            if abs_k > 500:
                severity = "high"
            elif abs_k > 100:
                severity = "medium"
            else:
                severity = "low"

        abs_k = abs(result.exposure_amount) / 1_000
        sign_pp = "+" if result.delta_pct > 0 else ""
        old_str = f"{result.old_effective_rate:.2f}%"
        new_str = f"{result.new_effective_rate:.2f}%"

        if result.direction == "increase":
            direction_label = f"annual COST INCREASE of ${abs_k:,.0f}K"
        elif result.direction == "decrease":
            direction_label = f"annual SAVING of ${abs_k:,.0f}K"
        else:
            direction_label = "no duty-cost impact"

        message = (
            f"Duty rate change on HS {result.hs6} {result.origin}->{result.destination}: "
            f"effective rate {old_str} -> {new_str} "
            f"({sign_pp}{result.delta_pct:.2f}pp). "
            f"Estimated {direction_label}. {stub_note}"
        )

        # Applicability caveat: when remedies drove the exposure, note that country-scope
        # of those remedies was not verified by this agent (e.g. Section 301 is US-China).
        if result.remedy_applicability == "assumed_unverified":
            message += (
                f" Note: remedy applicability to {result.origin} is ASSUMED — "
                "remedy country scope is not verified by this agent."
            )

    return {
        "timestamp": now.strftime("%Y-%m-%d %H:%M UTC"),
        "agent_id":  "tariff_shock",
        "severity":  severity,
        "message":   message,
        # Structured lane metadata — used by /api/tariff-shock for industry filtering.
        # Not rendered by the current UI (renderAlerts reads only timestamp/severity/message).
        "lane_key":    result.lane_key,
        "hs6":         result.hs6,
        "origin":      result.origin,
        "destination": result.destination,
    }
