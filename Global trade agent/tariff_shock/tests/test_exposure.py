"""
Offline unit tests for tariff_shock.exposure and tariff_shock.alert_adapter.

All tests run without network access — no connectors, no DB, no Flask.

Coverage
--------
    TestRemedyParsing
        - parseable single remedy
        - parseable decimal remedy
        - multiple parseable remedies summed
        - unparseable remedy -> None (no number fabricated)
        - empty list -> 0.0 (no remedies)

    TestEffectiveRate
        - ad-valorem MFN, no FTA, no remedies
        - FTA lane uses pref, ignores MFN
        - FTA lane + parseable remedy stacked on top of pref
        - specific-duty MFN -> None (review)
        - unparseable remedy -> None (review)

    TestComputeExposure (signed delta, direction, exposure_amount)
        - old=None -> returns None (no baseline)
        - ad-valorem increase -> positive delta, positive exposure, direction="increase"
        - ad-valorem decrease -> negative delta, negative exposure, direction="decrease"
        - FTA lane: MFN changes, pref unchanged -> delta=0, direction="no_change"
          (proves MFN noise does NOT create phantom exposure on FTA lanes)
        - Section 301 added (parseable) -> stacked exposure, direction="increase"
        - remedy present but unparseable -> review_flag=True, exposure_amount=None
        - specific-duty change -> review_flag=True, exposure_amount=None

    TestAlertAdapter
        - increase alert: severity, message contains signed delta and stub label
        - decrease alert: direction label is "SAVING" (not "exposure")
        - review alert: severity=medium, no dollar amount, stub label present
        - parseable-remedy alert: message reflects stacked rate

    TestEndToEnd
        - KORUS pref 0.0% -> Section 301 25% added:
          old_effective=0.0, new_effective=25.0, delta=+25.0,
          exposure=$200 000 (positive), direction="increase",
          alert message states stub label explicitly
"""

from __future__ import annotations

import pytest
from datetime import date

from aggregator.models import CanonicalRate
from tariff_shock.exposure import (
    ExposureResult,
    _parse_remedy_total,
    _effective_rate,
    compute_exposure,
)
from tariff_shock.alert_adapter import to_alert


# ---------------------------------------------------------------------------
# Test fixture helper
# ---------------------------------------------------------------------------

def _rate(
    hs_code: str = "847130",
    origin: str = "VN",
    destination: str = "US",
    mfn_rate: float | None = 0.0,
    duty_expression: str | None = None,
    preferential_rate: float | None = None,
    applicable_fta: str | None = None,
    active_remedies: list[str] | None = None,
) -> CanonicalRate:
    """Build a minimal CanonicalRate for testing without hitting any network."""
    return CanonicalRate(
        hs_code=hs_code,
        origin=origin,
        destination=destination,
        effective_date=date(2026, 1, 1),
        mfn_rate=mfn_rate,
        duty_expression=duty_expression,
        preferential_rate=preferential_rate,
        applicable_fta=applicable_fta,
        active_remedies=active_remedies or [],
        best_source="US HTS",
        confidence=0.9,
    )


# ---------------------------------------------------------------------------
# Remedy parsing
# ---------------------------------------------------------------------------

class TestRemedyParsing:
    def test_empty_remedies_returns_zero(self):
        total, desc = _parse_remedy_total([])
        assert total == 0.0
        assert desc == ""

    def test_single_integer_pct(self):
        total, desc = _parse_remedy_total(["Section 301 25%"])
        assert total == 25.0
        assert "25" in desc

    def test_single_decimal_pct(self):
        total, desc = _parse_remedy_total(["Antidumping 12.4%"])
        assert total == pytest.approx(12.4)

    def test_multiple_parseable_remedies_summed(self):
        total, desc = _parse_remedy_total(["Section 301 25%", "CVD 3.5%"])
        assert total == pytest.approx(28.5)

    def test_unparseable_returns_none(self):
        # Must return None, not a fabricated number.
        total, reason = _parse_remedy_total(["AD/CVD -- variable, pending investigation"])
        assert total is None
        assert "not parseable" in reason

    def test_mixed_one_unparseable_returns_none(self):
        # Even if one remedy IS parseable, a single miss -> None.
        total, reason = _parse_remedy_total(["Section 301 25%", "Safeguard TBD"])
        assert total is None


# ---------------------------------------------------------------------------
# Effective rate
# ---------------------------------------------------------------------------

class TestEffectiveRate:
    def test_mfn_only_no_fta_no_remedies(self):
        r = _rate(mfn_rate=7.2)
        rate, desc = _effective_rate(r)
        assert rate == pytest.approx(7.2)
        assert "MFN 7.2%" in desc

    def test_fta_lane_uses_pref_not_mfn(self):
        # MFN=7.2, pref=0.0 -> effective is 0.0 (pref wins)
        r = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS")
        rate, desc = _effective_rate(r)
        assert rate == pytest.approx(0.0)
        assert "pref 0.0% under KORUS" in desc
        assert "MFN" not in desc

    def test_fta_lane_remedy_stacks_on_pref(self):
        # pref=0.0, Section 301 25% -> effective = 0.0 + 25.0 = 25.0
        r = _rate(
            mfn_rate=7.2,
            preferential_rate=0.0,
            applicable_fta="KORUS",
            active_remedies=["Section 301 25%"],
        )
        rate, desc = _effective_rate(r)
        assert rate == pytest.approx(25.0)
        assert "25" in desc

    def test_specific_duty_returns_none(self):
        r = _rate(mfn_rate=None, duty_expression="$1.227/kg")
        rate, reason = _effective_rate(r)
        assert rate is None
        assert "specific" in reason.lower() or "compound" in reason.lower()

    def test_unparseable_remedy_returns_none(self):
        r = _rate(mfn_rate=5.0, active_remedies=["Safeguard — TRQ, variable"])
        rate, reason = _effective_rate(r)
        assert rate is None
        assert "not parseable" in reason


# ---------------------------------------------------------------------------
# compute_exposure — the main public function
# ---------------------------------------------------------------------------

class TestComputeExposure:
    _STUB = 1_000_000.0   # $1M annual stub value for easy math

    def test_old_none_returns_none(self):
        """First observation: no prior baseline, no delta possible."""
        new = _rate(mfn_rate=5.0)
        assert compute_exposure(None, new, self._STUB) is None

    def test_advalorem_increase_positive_exposure(self):
        """Rate goes up -> delta positive -> exposure positive (added cost)."""
        old = _rate(mfn_rate=5.0)
        new = _rate(mfn_rate=10.0)
        result = compute_exposure(old, new, self._STUB)
        assert result is not None
        assert result.review_flag is False
        assert result.delta_pct == pytest.approx(5.0)
        assert result.exposure_amount == pytest.approx(50_000.0)
        assert result.direction == "increase"
        assert result.old_effective_rate == pytest.approx(5.0)
        assert result.new_effective_rate == pytest.approx(10.0)

    def test_advalorem_decrease_negative_exposure(self):
        """Rate goes down -> delta negative -> exposure negative (saving). Sign must be preserved."""
        old = _rate(mfn_rate=10.0)
        new = _rate(mfn_rate=5.0)
        result = compute_exposure(old, new, self._STUB)
        assert result is not None
        assert result.review_flag is False
        # Signed delta: 5.0 - 10.0 = -5.0 (saving, NOT absolute value)
        assert result.delta_pct == pytest.approx(-5.0)
        # Signed exposure: -5.0/100 * 1_000_000 = -50_000 (saving, NOT positive)
        assert result.exposure_amount == pytest.approx(-50_000.0)
        assert result.direction == "decrease"

    def test_fta_lane_mfn_change_pref_unchanged_zero_exposure(self):
        """
        FTA lane: MFN rises from 7.2% to 10.0% but KORUS pref stays at 0.0%.
        Effective rate is pref in both cases -> delta = 0 -> no exposure.
        Proves that MFN noise does NOT create phantom exposure on FTA lanes.
        """
        old = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS")
        new = _rate(mfn_rate=10.0, preferential_rate=0.0, applicable_fta="KORUS")
        result = compute_exposure(old, new, self._STUB)
        assert result is not None
        assert result.review_flag is False
        assert result.old_effective_rate == pytest.approx(0.0)
        assert result.new_effective_rate == pytest.approx(0.0)
        assert result.delta_pct == pytest.approx(0.0, abs=1e-9)
        assert result.exposure_amount == pytest.approx(0.0, abs=1e-6)
        assert result.direction == "no_change"

    def test_parseable_remedy_added_stacked_exposure(self):
        """
        Section 301 added to an FTA lane: pref still 0%, but remedy stacks.
        old effective = 0%, new effective = 0% + 25% = 25%.
        """
        old = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS",
                    active_remedies=[])
        new = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS",
                    active_remedies=["Section 301 25%"])
        stub = 800_000.0
        result = compute_exposure(old, new, stub)
        assert result is not None
        assert result.review_flag is False
        assert result.old_effective_rate == pytest.approx(0.0)
        assert result.new_effective_rate == pytest.approx(25.0)
        assert result.delta_pct == pytest.approx(25.0)
        assert result.exposure_amount == pytest.approx(200_000.0)   # 25/100 * 800k
        assert result.direction == "increase"

    def test_unparseable_remedy_review_flag_no_number(self):
        """Unparseable remedy -> review flag; exposure_amount must be None (not fabricated)."""
        old = _rate(mfn_rate=5.0)
        new = _rate(mfn_rate=5.0, active_remedies=["AD/CVD -- variable, pending investigation"])
        result = compute_exposure(old, new, self._STUB)
        assert result is not None
        assert result.review_flag is True
        assert result.exposure_amount is None   # MUST be None, not a guessed number
        assert result.delta_pct is None
        assert result.direction == "review"
        assert result.review_reason is not None
        assert "not parseable" in result.review_reason

    def test_specific_duty_change_review_flag_no_number(self):
        """Specific-duty lane -> review flag; exposure_amount must be None (not fabricated)."""
        old = _rate(mfn_rate=None, duty_expression="$1.227/kg")
        new = _rate(mfn_rate=None, duty_expression="$1.803/kg")
        result = compute_exposure(old, new, self._STUB)
        assert result is not None
        assert result.review_flag is True
        assert result.exposure_amount is None   # MUST be None, not a guessed number
        assert result.delta_pct is None
        assert result.direction == "review"


# ---------------------------------------------------------------------------
# Alert adapter
# ---------------------------------------------------------------------------

class TestAlertAdapter:
    def _increase_result(self) -> ExposureResult:
        old = _rate(mfn_rate=5.0)
        new = _rate(mfn_rate=10.0)
        return compute_exposure(old, new, 1_000_000.0)

    def _decrease_result(self) -> ExposureResult:
        old = _rate(mfn_rate=10.0)
        new = _rate(mfn_rate=5.0)
        return compute_exposure(old, new, 1_000_000.0)

    def _review_result(self) -> ExposureResult:
        old = _rate(mfn_rate=None, duty_expression="$1.227/kg")
        new = _rate(mfn_rate=None, duty_expression="$1.803/kg")
        return compute_exposure(old, new, 1_000_000.0)

    def test_increase_alert_fields(self):
        alert = to_alert(self._increase_result())
        assert alert["agent_id"] == "tariff_shock"
        assert alert["severity"] in ("high", "medium", "low")
        assert "COST INCREASE" in alert["message"]
        assert "ILLUSTRATIVE" in alert["message"]
        assert "+5.00pp" in alert["message"]

    def test_decrease_alert_uses_saving_label(self):
        """A rate decrease must be labelled SAVING, not COST INCREASE or 'exposure'."""
        alert = to_alert(self._decrease_result())
        assert "SAVING" in alert["message"]
        assert "COST INCREASE" not in alert["message"]
        assert "ILLUSTRATIVE" in alert["message"]
        assert "-5.00pp" in alert["message"]

    def test_review_alert_no_dollar_amount(self):
        alert = to_alert(self._review_result())
        assert alert["severity"] == "medium"
        assert "manual review" in alert["message"].lower()
        assert "ILLUSTRATIVE" in alert["message"]
        # Must NOT contain computed exposure patterns (K/yr, COST INCREASE, SAVING).
        # The duty expression '$1.227/kg' may appear in the review reason — that's fine.
        assert "K/yr" not in alert["message"]
        assert "COST INCREASE" not in alert["message"]
        assert "SAVING" not in alert["message"]

    def test_parseable_remedy_alert_shows_stacked_rate(self):
        old = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS",
                    active_remedies=[], hs_code="040690", origin="KR")
        new = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS",
                    active_remedies=["Section 301 25%"], hs_code="040690", origin="KR")
        result = compute_exposure(old, new, 800_000.0)
        alert = to_alert(result)
        assert "0.00%" in alert["message"]   # old effective
        assert "25.00%" in alert["message"]  # new effective
        assert "+25.00pp" in alert["message"]
        assert "COST INCREASE" in alert["message"]
        assert "ILLUSTRATIVE" in alert["message"]


# ---------------------------------------------------------------------------
# End-to-end: KORUS pref 0.0% -> Section 301 25% added
# ---------------------------------------------------------------------------

class TestEndToEnd:
    """
    Simulate the real-time loop offline:
        aggregator detects Section 301 added to KR->US 0406.90
        -> agent receives (old=KORUS pref 0%, new=KORUS pref 0% + S301 25%)
        -> exposure = $200 000/yr increase
        -> alert message states stub label
    """
    _STUB_KR = 800_000.0  # mirrors config stub for 040690_KR_US

    def _make_old(self) -> CanonicalRate:
        return _rate(
            hs_code="040690",
            origin="KR",
            destination="US",
            mfn_rate=7.2,
            preferential_rate=0.0,
            applicable_fta="KORUS",
            active_remedies=[],
        )

    def _make_new_with_s301(self) -> CanonicalRate:
        return _rate(
            hs_code="040690",
            origin="KR",
            destination="US",
            mfn_rate=7.2,
            preferential_rate=0.0,
            applicable_fta="KORUS",
            active_remedies=["Section 301 25%"],
        )

    def test_exposure_result_correct(self):
        result = compute_exposure(self._make_old(), self._make_new_with_s301(), self._STUB_KR)
        assert result is not None
        assert result.review_flag is False
        assert result.old_effective_rate == pytest.approx(0.0)
        assert result.new_effective_rate == pytest.approx(25.0)
        assert result.delta_pct == pytest.approx(25.0)
        assert result.exposure_amount == pytest.approx(200_000.0)
        assert result.direction == "increase"
        assert result.applicable_fta == "KORUS"
        assert result.new_remedies == ["Section 301 25%"]
        assert result.old_remedies == []

    def test_alert_matches_get_alerts_shape(self):
        result = compute_exposure(self._make_old(), self._make_new_with_s301(), self._STUB_KR)
        alert = to_alert(result)
        # Shape contract
        assert set(alert.keys()) == {"timestamp", "agent_id", "severity", "message"}
        assert alert["agent_id"] == "tariff_shock"
        assert alert["severity"] == "medium"   # $200k: > $100k but <= $500k
        # Content
        assert "040690" in alert["message"]
        assert "KR->US" in alert["message"]
        assert "0.00%" in alert["message"]     # old effective
        assert "25.00%" in alert["message"]    # new effective
        assert "+25.00pp" in alert["message"]
        assert "COST INCREASE" in alert["message"]
        assert "$200" in alert["message"]      # exposure amount
        assert "ILLUSTRATIVE" in alert["message"]  # stub label MUST be present
        assert "$0.8M" in alert["message"]     # stub value disclosed

    def test_agent_cache_written_and_readable(self):
        """Thread-safety smoke-test: write via on_rate_change, read via latest_*."""
        from tariff_shock.agent import TariffShockAgent
        agent = TariffShockAgent(stub_volumes={"040690_KR_US": self._STUB_KR})
        # Simulate first observation (old=None) — should not produce an alert
        agent.on_rate_change(self._make_old(), None)
        assert agent.latest_alerts() == []
        assert agent.latest_report() == {}
        # Simulate rate change (old -> new with Section 301)
        agent.on_rate_change(self._make_new_with_s301(), self._make_old())
        alerts = agent.latest_alerts()
        reports = agent.latest_report()
        assert len(alerts) == 1
        assert alerts[0]["agent_id"] == "tariff_shock"
        assert "040690_KR_US" in reports
        r = reports["040690_KR_US"]
        assert r["direction"] == "increase"
        assert r["exposure_amount"] == pytest.approx(200_000.0)
        assert r["review_flag"] is False
        assert "ILLUSTRATIVE" in r["stub_label"]

    def test_unknown_lane_uses_zero_stub(self):
        """Lane not in stub_volumes -> stub=0 -> exposure=0, direction=no_change."""
        from tariff_shock.agent import TariffShockAgent
        agent = TariffShockAgent(stub_volumes={})  # no volumes configured
        agent.on_rate_change(self._make_new_with_s301(), self._make_old())
        alerts = agent.latest_alerts()
        assert len(alerts) == 1
        r = agent.latest_report()["040690_KR_US"]
        # delta_pct is still 25.0 but exposure is 0 because stub is 0
        assert r["delta_pct"] == pytest.approx(25.0)
        assert r["exposure_amount"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Remedy applicability caveat + severity-vs-sign (adjustment tests)
# ---------------------------------------------------------------------------

class TestRemedyApplicabilityAndSeverity:
    """
    Adjustment (a): remedy-driven exposure carries assumed_unverified flag.
    Adjustment (b): a saving (negative exposure) is never high/medium severity.
    """

    def test_remedy_driven_exposure_has_assumed_unverified_flag(self):
        """
        When a remedy drives the effective rate, remedy_applicability must be
        "assumed_unverified": the country-scope of the remedy was not checked.
        The number stands; the certainty must not exceed what was verified.
        """
        from tariff_shock.agent import _result_to_dict
        old = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS",
                    hs_code="040690", origin="KR", active_remedies=[])
        new = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS",
                    hs_code="040690", origin="KR", active_remedies=["Section 301 25%"])
        result = compute_exposure(old, new, 800_000.0)
        assert result is not None
        assert result.review_flag is False
        # Field is set on the ExposureResult
        assert result.remedy_applicability == "assumed_unverified"
        # Alert message carries the caveat
        alert = to_alert(result)
        assert "ASSUMED" in alert["message"]
        assert "country scope" in alert["message"].lower() or "country" in alert["message"].lower()
        # Report dict carries the field
        report = _result_to_dict(result)
        assert report["remedy_applicability"] == "assumed_unverified"

    def test_no_remedy_no_applicability_flag(self):
        """Pure ad-valorem change with no remedies -> remedy_applicability is None."""
        old = _rate(mfn_rate=5.0)
        new = _rate(mfn_rate=10.0)
        result = compute_exposure(old, new, 1_000_000.0)
        assert result is not None
        assert result.remedy_applicability is None
        # Alert must NOT contain an assumed applicability caveat
        alert = to_alert(result)
        assert "ASSUMED" not in alert["message"]

    def test_saving_large_magnitude_has_low_severity(self):
        """
        A rate decrease producing a large saving must NOT be labeled high severity.
        Severity is a risk signal; a saving is favorable, not a threat.
        $600K saving on a $3M stub: magnitude alone would give "high", but direction
        must cap it at "low".
        """
        old = _rate(mfn_rate=25.0)   # high duty rate
        new = _rate(mfn_rate=5.0)    # rate drops — saving
        stub = 3_000_000.0           # $3M -> $600K saving if pure ad-valorem
        result = compute_exposure(old, new, stub)
        assert result is not None
        assert result.direction == "decrease"
        assert result.exposure_amount == pytest.approx(-600_000.0)  # signed negative
        alert = to_alert(result)
        # Despite $600K magnitude, severity must be "low" (saving, not a threat)
        assert alert["severity"] == "low"
        assert "SAVING" in alert["message"]
        assert "COST INCREASE" not in alert["message"]

    def test_increase_severity_still_scales_with_magnitude(self):
        """An increase is still graded high/medium/low by size (no regression)."""
        # High: > $500K -> $600K increase
        old_lo = _rate(mfn_rate=5.0)
        new_hi = _rate(mfn_rate=25.0)
        r_high = compute_exposure(old_lo, new_hi, 3_000_000.0)
        assert to_alert(r_high)["severity"] == "high"
        # Medium: $200K
        old_7 = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS")
        new_s301 = _rate(mfn_rate=7.2, preferential_rate=0.0, applicable_fta="KORUS",
                         active_remedies=["Section 301 25%"])
        r_med = compute_exposure(old_7, new_s301, 800_000.0)
        assert to_alert(r_med)["severity"] == "medium"
        # Low: < $100K
        old_sm = _rate(mfn_rate=5.0)
        new_sm = _rate(mfn_rate=6.0)
        r_low = compute_exposure(old_sm, new_sm, 500_000.0)
        assert to_alert(r_low)["severity"] == "low"
