"""
Tests for aggregator.models — RawRate and CanonicalRate.

Run from the 'Global trade agent/' directory:
    pytest aggregator/tests/test_models.py -v

All tests are offline — no HTTP calls, no external dependencies.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from aggregator.models import CanonicalRate, RawRate

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

TODAY = date(2026, 7, 31)
FIXED_TS = datetime(2026, 7, 31, 12, 0, 0)


def _raw_payload(**overrides: object) -> dict:
    """Return a minimally valid RawRate payload, with optional field overrides."""
    base: dict = {
        "hs_code": "8471.30",
        "origin": "VN",
        "destination": "US",
        "effective_date": TODAY,
        "mfn_rate": 4.5,
        "source": "US HTS",
    }
    base.update(overrides)
    return base


def _canonical_payload(**overrides: object) -> dict:
    """Return a minimally valid CanonicalRate payload, with optional field overrides."""
    base: dict = {
        "hs_code": "8471.30",
        "origin": "VN",
        "destination": "US",
        "effective_date": TODAY,
        "mfn_rate": 4.5,
        "best_source": "US HTS",
        "confidence": 0.9,
    }
    base.update(overrides)
    return base


# ===========================================================================
# RawRate — construction
# ===========================================================================

class TestRawRateConstruction:
    def test_minimal_required_fields(self) -> None:
        r = RawRate(**_raw_payload())
        assert r.hs_code == "8471.30"
        assert r.origin == "VN"
        assert r.destination == "US"
        assert r.mfn_rate == 4.5
        assert r.source == "US HTS"

    def test_optional_fields_default_to_none_or_empty(self) -> None:
        r = RawRate(**_raw_payload())
        assert r.preferential_rate is None
        assert r.applicable_fta is None
        assert r.rules_of_origin is None
        assert r.active_remedies == []
        assert r.duty_expression is None
        assert r.preferential_expression is None

    def test_mfn_rate_defaults_to_none_when_unset(self) -> None:
        """mfn_rate is Optional — omitting it should produce None, not an error."""
        payload = {k: v for k, v in _raw_payload().items() if k != "mfn_rate"}
        r = RawRate(**payload)
        assert r.mfn_rate is None

    def test_fetched_at_defaults_to_utcnow(self) -> None:
        before = datetime.now(UTC)
        r = RawRate(**_raw_payload())
        after = datetime.now(UTC)
        assert before <= r.fetched_at <= after

    def test_explicit_fetched_at_is_preserved(self) -> None:
        r = RawRate(**_raw_payload(fetched_at=FIXED_TS))
        assert r.fetched_at == FIXED_TS

    def test_all_optional_fields_accepted(self) -> None:
        r = RawRate(**_raw_payload(
            preferential_rate=0.0,
            applicable_fta="USMCA",
            rules_of_origin="RVC 75%",
            active_remedies=["Section 301 25%", "ADD 8%"],
        ))
        assert r.preferential_rate == 0.0
        assert r.applicable_fta == "USMCA"
        assert r.rules_of_origin == "RVC 75%"
        assert r.active_remedies == ["Section 301 25%", "ADD 8%"]

    def test_zero_mfn_rate_accepted(self) -> None:
        r = RawRate(**_raw_payload(mfn_rate=0.0))
        assert r.mfn_rate == 0.0

    def test_zero_preferential_rate_accepted(self) -> None:
        r = RawRate(**_raw_payload(preferential_rate=0.0, applicable_fta="EU-Vietnam FTA"))
        assert r.preferential_rate == 0.0


# ===========================================================================
# RawRate — HS code validation
# ===========================================================================

class TestRawRateHsCode:
    @pytest.mark.parametrize("code", [
        "8471",          # 4-digit chapter/heading
        "847130",        # 6-digit (standard HS6)
        "8471305010",    # 10-digit (US HTS)
        "8471.30",       # dotted HS6
        "8471.30.50",    # dotted 3-segment
        "8471.30.50.10", # dotted 4-segment
    ])
    def test_valid_hs_code_formats(self, code: str) -> None:
        r = RawRate(**_raw_payload(hs_code=code))
        assert r.hs_code == code

    def test_hs_code_whitespace_stripped(self) -> None:
        r = RawRate(**_raw_payload(hs_code="  8471.30  "))
        assert r.hs_code == "8471.30"

    @pytest.mark.parametrize("bad_code", [
        "ABC",      # letters
        "123",      # too short (3 digits)
        "847",      # too short
        "ABCD1234", # letters mixed in
        "8471.",    # trailing dot
        "8471.3",   # second segment only 1 digit
        "",         # empty
    ])
    def test_invalid_hs_code_raises(self, bad_code: str) -> None:
        with pytest.raises(ValidationError, match="HS code"):
            RawRate(**_raw_payload(hs_code=bad_code))


# ===========================================================================
# RawRate — country code normalisation  (MODIFIED: alpha-3 now converts to alpha-2)
# ===========================================================================

class TestRawRateCountryCodes:
    def test_lowercase_origin_normalised(self) -> None:
        r = RawRate(**_raw_payload(origin="vn"))
        assert r.origin == "VN"

    def test_lowercase_destination_normalised(self) -> None:
        r = RawRate(**_raw_payload(destination="us"))
        assert r.destination == "US"

    def test_mixed_case_normalised(self) -> None:
        r = RawRate(**_raw_payload(origin="Vn", destination="Us"))
        assert r.origin == "VN"
        assert r.destination == "US"

    def test_alpha3_vnm_converted_to_alpha2_vn(self) -> None:
        """ISO alpha-3 'VNM' must be stored as alpha-2 'VN'."""
        r = RawRate(**_raw_payload(origin="VNM"))
        assert r.origin == "VN"

    def test_alpha3_usa_converted_to_alpha2_us(self) -> None:
        r = RawRate(**_raw_payload(destination="USA"))
        assert r.destination == "US"

    def test_alpha3_deu_converted_to_alpha2_de(self) -> None:
        r = RawRate(**_raw_payload(origin="DEU"))
        assert r.origin == "DE"

    def test_alpha2_code_unchanged(self) -> None:
        """alpha-2 codes must not be modified."""
        r = RawRate(**_raw_payload(origin="VN", destination="US"))
        assert r.origin == "VN"
        assert r.destination == "US"

    def test_unknown_three_letter_code_kept_as_is(self) -> None:
        """Custom or unknown 3-letter codes pass through without crashing."""
        r = RawRate(**_raw_payload(origin="XYZ"))
        assert r.origin == "XYZ"


# ===========================================================================
# RawRate — validation errors
# ===========================================================================

class TestRawRateValidationErrors:
    def test_negative_mfn_rate_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RawRate(**_raw_payload(mfn_rate=-0.1))

    def test_negative_preferential_rate_rejected(self) -> None:
        with pytest.raises(ValidationError):
            RawRate(**_raw_payload(preferential_rate=-1.0, applicable_fta="USMCA"))

    def test_preferential_rate_without_fta_rejected(self) -> None:
        with pytest.raises(ValidationError, match="applicable_fta"):
            RawRate(**_raw_payload(preferential_rate=2.5))

    def test_fta_without_preferential_rate_rejected(self) -> None:
        with pytest.raises(ValidationError, match="applicable_fta"):
            RawRate(**_raw_payload(applicable_fta="USMCA"))

    def test_missing_required_source_rejected(self) -> None:
        payload = _raw_payload()
        del payload["source"]
        with pytest.raises(ValidationError):
            RawRate(**payload)

    def test_missing_required_hs_code_rejected(self) -> None:
        payload = _raw_payload()
        del payload["hs_code"]
        with pytest.raises(ValidationError):
            RawRate(**payload)


# ===========================================================================
# hs6 property  (NEW)
# ===========================================================================

class TestHs6Property:
    """hs6 returns the first 6 significant digits, dots stripped — canonical join key."""

    def test_plain_6digit(self) -> None:
        r = RawRate(**_raw_payload(hs_code="847130"))
        assert r.hs6 == "847130"

    def test_dotted_hs6(self) -> None:
        r = RawRate(**_raw_payload(hs_code="8471.30"))
        assert r.hs6 == "847130"

    def test_plain_10digit_truncates_to_6(self) -> None:
        """10-digit US HTS code must truncate to first 6."""
        r = RawRate(**_raw_payload(hs_code="8471305010"))
        assert r.hs6 == "847130"

    def test_dotted_10digit_truncates_to_6(self) -> None:
        """Dotted 4-segment code must yield the same hs6 as the plain variant."""
        r = RawRate(**_raw_payload(hs_code="8471.30.50.10"))
        assert r.hs6 == "847130"

    def test_dotted_3segment_truncates_to_6(self) -> None:
        r = RawRate(**_raw_payload(hs_code="8471.30.50"))
        assert r.hs6 == "847130"

    def test_4digit_heading_returns_all_4(self) -> None:
        """When only a 4-digit heading is provided, return those 4 digits."""
        r = RawRate(**_raw_payload(hs_code="8471"))
        assert r.hs6 == "8471"

    def test_hs6_consistent_across_formats(self) -> None:
        """All representations of the same lane produce the same hs6."""
        codes = ["847130", "8471.30", "8471305010", "8471.30.50.10"]
        hs6_values = {RawRate(**_raw_payload(hs_code=c)).hs6 for c in codes}
        assert hs6_values == {"847130"}

    def test_canonical_rate_inherits_hs6(self) -> None:
        """CanonicalRate inherits hs6 from _BaseRate."""
        c = CanonicalRate(**_canonical_payload(hs_code="8471305010"))
        assert c.hs6 == "847130"


# ===========================================================================
# Specific and compound duty fields  (NEW)
# ===========================================================================

class TestSpecificDutyFields:
    """
    mfn_rate is Optional — connectors use duty_expression for specific and
    compound duties rather than crashing or fabricating a float.
    """

    def test_specific_duty_mfn_rate_none_duty_expression_set(self) -> None:
        """6.8¢/kg is a specific duty — mfn_rate must be None."""
        r = RawRate(**_raw_payload(mfn_rate=None, duty_expression="6.8¢/kg"))
        assert r.mfn_rate is None
        assert r.duty_expression == "6.8¢/kg"

    def test_compound_duty_expression(self) -> None:
        """5% + 2¢/kg is a compound duty."""
        r = RawRate(**_raw_payload(mfn_rate=None, duty_expression="5% + 2¢/kg"))
        assert r.duty_expression == "5% + 2¢/kg"

    def test_both_mfn_rate_and_expression_can_coexist(self) -> None:
        """A connector may supply both when it can compute an ad-valorem equivalent."""
        r = RawRate(**_raw_payload(mfn_rate=3.5, duty_expression="6.8¢/kg"))
        assert r.mfn_rate == 3.5
        assert r.duty_expression == "6.8¢/kg"

    def test_both_none_accepted(self) -> None:
        """Connector that cannot determine any duty form is allowed to leave both None."""
        payload = {k: v for k, v in _raw_payload().items() if k != "mfn_rate"}
        r = RawRate(**payload, mfn_rate=None, duty_expression=None)
        assert r.mfn_rate is None
        assert r.duty_expression is None

    def test_preferential_expression_accepted_with_fta(self) -> None:
        """Non-ad-valorem preferential rate stored verbatim; applicable_fta required."""
        r = RawRate(**_raw_payload(preferential_expression="1.5c/kg + 3%", applicable_fta="USMCA"))
        assert r.preferential_expression == "1.5c/kg + 3%"
        assert r.applicable_fta == "USMCA"

    def test_preferential_expression_without_fta_rejected(self) -> None:
        """preferential_expression alone (no applicable_fta) is uninterpretable — must raise."""
        with pytest.raises(ValidationError, match="applicable_fta"):
            RawRate(**_raw_payload(preferential_expression="2c/kg"))

    def test_duty_expression_passed_through_from_raw_rate(self) -> None:
        """from_raw_rate must carry duty_expression into CanonicalRate."""
        raw = RawRate(**_raw_payload(mfn_rate=None, duty_expression="6.8¢/kg"))
        c = CanonicalRate.from_raw_rate(raw, confidence=0.65)
        assert c.duty_expression == "6.8¢/kg"
        assert c.mfn_rate is None

    def test_preferential_expression_passed_through_from_raw_rate(self) -> None:
        """from_raw_rate must carry preferential_expression into CanonicalRate."""
        raw = RawRate(**_raw_payload(preferential_expression="1.5c/kg", applicable_fta="USMCA"))
        c = CanonicalRate.from_raw_rate(raw, confidence=0.9)
        assert c.preferential_expression == "1.5c/kg"
        assert c.applicable_fta == "USMCA"


# ===========================================================================
# CanonicalRate — construction
# ===========================================================================

class TestCanonicalRateConstruction:
    def test_minimal_required_fields(self) -> None:
        c = CanonicalRate(**_canonical_payload())
        assert c.confidence == 0.9
        assert c.best_source == "US HTS"

    def test_optional_fields_default_correctly(self) -> None:
        c = CanonicalRate(**_canonical_payload())
        assert c.sources_consulted == []
        assert c.disagreement_details == []
        assert c.is_stale is False
        assert c.preferential_rate is None
        assert c.applicable_fta is None
        assert c.duty_expression is None
        assert c.preferential_expression is None

    def test_fetched_at_defaults_to_utcnow(self) -> None:
        before = datetime.now(UTC)
        c = CanonicalRate(**_canonical_payload())
        after = datetime.now(UTC)
        assert before <= c.fetched_at <= after

    def test_full_fields_accepted(self) -> None:
        c = CanonicalRate(**_canonical_payload(
            preferential_rate=0.0,
            applicable_fta="EU-Vietnam FTA",
            rules_of_origin="RVC 45%",
            active_remedies=["Section 301 25%"],
            sources_consulted=["US HTS", "WITS"],
            disagreement_details=["WITS reported 5.0%, US HTS reported 4.5%"],
            confidence=0.45,
            is_stale=True,
        ))
        assert c.applicable_fta == "EU-Vietnam FTA"
        assert c.rules_of_origin == "RVC 45%"
        assert len(c.sources_consulted) == 2
        assert c.is_stale is True

    def test_confidence_boundary_zero_accepted(self) -> None:
        c = CanonicalRate(**_canonical_payload(confidence=0.0))
        assert c.confidence == 0.0

    def test_confidence_boundary_one_accepted(self) -> None:
        c = CanonicalRate(**_canonical_payload(confidence=1.0))
        assert c.confidence == 1.0


# ===========================================================================
# CanonicalRate — validation errors
# ===========================================================================

class TestCanonicalRateValidationErrors:
    def test_confidence_above_one_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CanonicalRate(**_canonical_payload(confidence=1.01))

    def test_confidence_below_zero_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CanonicalRate(**_canonical_payload(confidence=-0.01))

    def test_preferential_without_fta_rejected(self) -> None:
        with pytest.raises(ValidationError, match="applicable_fta"):
            CanonicalRate(**_canonical_payload(preferential_rate=2.0))

    def test_fta_without_preferential_rate_rejected(self) -> None:
        with pytest.raises(ValidationError, match="applicable_fta"):
            CanonicalRate(**_canonical_payload(applicable_fta="USMCA"))

    def test_invalid_hs_code_rejected(self) -> None:
        with pytest.raises(ValidationError, match="HS code"):
            CanonicalRate(**_canonical_payload(hs_code="INVALID"))

    def test_negative_mfn_rate_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CanonicalRate(**_canonical_payload(mfn_rate=-1.0))


# ===========================================================================
# CanonicalRate — confidence_tier property
# ===========================================================================

class TestConfidenceTier:
    @pytest.mark.parametrize("score, expected_tier", [
        (1.00, "high"),
        (0.80, "high"),    # boundary — high starts at 0.80
        (0.79, "medium"),  # just below high
        (0.60, "medium"),
        (0.50, "medium"),  # boundary — medium starts at 0.50
        (0.49, "low"),     # just below medium
        (0.30, "low"),
        (0.00, "low"),
    ])
    def test_tier_thresholds(self, score: float, expected_tier: str) -> None:
        c = CanonicalRate(**_canonical_payload(confidence=score))
        assert c.confidence_tier == expected_tier

    def test_tier_values_are_exhaustive(self) -> None:
        valid = {"high", "medium", "low"}
        for score in (0.0, 0.3, 0.5, 0.65, 0.8, 1.0):
            c = CanonicalRate(**_canonical_payload(confidence=score))
            assert c.confidence_tier in valid


# ===========================================================================
# CanonicalRate — feed_status property  (MODIFIED: two-state map, no 'pending')
# ===========================================================================

class TestFeedStatus:
    @pytest.mark.parametrize("score, expected_status", [
        (0.95, "cleared"),  # high confidence
        (0.80, "cleared"),  # high boundary
        (0.70, "cleared"),  # medium — usable single-source answer, maps to 'cleared'
        (0.50, "cleared"),  # medium boundary
        (0.45, "issued"),   # low confidence — surface as alert
        (0.00, "issued"),
    ])
    def test_status_mapping(self, score: float, expected_status: str) -> None:
        c = CanonicalRate(**_canonical_payload(confidence=score))
        assert c.feed_status == expected_status

    def test_medium_confidence_maps_to_cleared_not_pending(self) -> None:
        """Medium confidence (single-source) is usable — must not produce 'pending'."""
        c = CanonicalRate(**_canonical_payload(confidence=0.65))
        assert c.feed_status == "cleared"
        assert c.feed_status != "pending"

    def test_status_values_match_ui_vocabulary(self) -> None:
        """feed_status must only produce the two values get_tariff_feed() actually emits."""
        # 'pending' is styled in the CSS but never emitted by the simulator.
        allowed = {"cleared", "issued"}
        for score in (0.0, 0.5, 1.0):
            c = CanonicalRate(**_canonical_payload(confidence=score))
            assert c.feed_status in allowed


# ===========================================================================
# CanonicalRate — summary property  (EXTENDED: specific/compound duty cases)
# ===========================================================================

class TestSummaryProperty:
    def test_mfn_only_format(self) -> None:
        c = CanonicalRate(**_canonical_payload(mfn_rate=4.5))
        assert c.summary == "HS 8471.30, VN→US: MFN 4.5%"

    def test_with_preferential_and_fta(self) -> None:
        c = CanonicalRate(**_canonical_payload(
            mfn_rate=4.5,
            preferential_rate=0.0,
            applicable_fta="USMCA",
        ))
        assert "MFN 4.5%" in c.summary
        assert "preferential 0.0% under USMCA" in c.summary

    def test_lane_reflected_in_summary(self) -> None:
        c = CanonicalRate(**_canonical_payload(
            hs_code="6204.62",
            origin="VN",
            destination="EU",
            mfn_rate=12.0,
        ))
        assert "VN→EU" in c.summary
        assert "6204.62" in c.summary
        assert "MFN 12.0%" in c.summary

    def test_summary_contains_hs_code(self) -> None:
        c = CanonicalRate(**_canonical_payload(hs_code="8471.30.50"))
        assert "8471.30.50" in c.summary

    def test_no_preferential_means_no_fta_in_summary(self) -> None:
        c = CanonicalRate(**_canonical_payload())
        assert "preferential" not in c.summary
        assert "under" not in c.summary

    def test_summary_specific_duty_uses_expression(self) -> None:
        """When mfn_rate is None, duty_expression appears in the headline."""
        c = CanonicalRate(**_canonical_payload(mfn_rate=None, duty_expression="6.8¢/kg"))
        assert "6.8¢/kg" in c.summary
        assert "MFN 6.8¢/kg" in c.summary

    def test_summary_compound_duty_uses_expression(self) -> None:
        c = CanonicalRate(**_canonical_payload(mfn_rate=None, duty_expression="5% + 2¢/kg"))
        assert "5% + 2¢/kg" in c.summary

    def test_summary_no_rate_no_expression(self) -> None:
        """Both absent → 'MFN rate unavailable' placeholder."""
        c = CanonicalRate(**_canonical_payload(mfn_rate=None, duty_expression=None))
        assert "MFN rate unavailable" in c.summary

    def test_summary_preferential_expression_with_fta(self) -> None:
        """preferential_expression + applicable_fta appears in summary."""
        c = CanonicalRate(**_canonical_payload(
            mfn_rate=4.5,
            preferential_expression="1.5c/kg",
            applicable_fta="USMCA",
        ))
        assert "preferential 1.5c/kg under USMCA" in c.summary


# ===========================================================================
# CanonicalRate — detail_summary property
# ===========================================================================

class TestDetailSummary:
    def test_empty_when_no_extras(self) -> None:
        c = CanonicalRate(**_canonical_payload())
        assert c.detail_summary == "No additional details"

    def test_rules_of_origin_included(self) -> None:
        c = CanonicalRate(**_canonical_payload(rules_of_origin="RVC 45%"))
        assert "Rule: RVC 45%" in c.detail_summary

    def test_single_remedy_included(self) -> None:
        c = CanonicalRate(**_canonical_payload(active_remedies=["Section 301 25%"]))
        assert "Section 301 25%" in c.detail_summary

    def test_multiple_remedies_semicolon_separated(self) -> None:
        c = CanonicalRate(**_canonical_payload(
            active_remedies=["Section 301 25%", "ADD 8%"]
        ))
        assert "Section 301 25%" in c.detail_summary
        assert "ADD 8%" in c.detail_summary
        assert ";" in c.detail_summary

    def test_disagreement_note_included(self) -> None:
        c = CanonicalRate(**_canonical_payload(
            confidence=0.45,
            disagreement_details=["WITS reported 5.0%, US HTS reported 4.5%"],
        ))
        assert "Note:" in c.detail_summary
        assert "WITS" in c.detail_summary

    def test_only_first_disagreement_note_shown(self) -> None:
        c = CanonicalRate(**_canonical_payload(
            confidence=0.45,
            disagreement_details=["First note", "Second note"],
        ))
        assert "First note" in c.detail_summary
        assert "Second note" not in c.detail_summary

    def test_three_parts_pipe_separated(self) -> None:
        c = CanonicalRate(**_canonical_payload(
            rules_of_origin="RVC 45%",
            active_remedies=["Section 301 25%"],
            confidence=0.45,
            disagreement_details=["WITS disagrees"],
        ))
        segments = c.detail_summary.split(" | ")
        assert len(segments) == 3

    def test_two_parts_pipe_separated(self) -> None:
        c = CanonicalRate(**_canonical_payload(rules_of_origin="CTH"))
        assert " | " not in c.detail_summary
        assert "Rule: CTH" in c.detail_summary


# ===========================================================================
# CanonicalRate — from_raw_rate classmethod
# ===========================================================================

class TestFromRawRate:
    def test_basic_promotion_copies_core_fields(self) -> None:
        raw = RawRate(**_raw_payload())
        c = CanonicalRate.from_raw_rate(raw, confidence=0.65)
        assert c.hs_code == raw.hs_code
        assert c.origin == raw.origin
        assert c.destination == raw.destination
        assert c.effective_date == raw.effective_date
        assert c.mfn_rate == raw.mfn_rate
        assert c.fetched_at == raw.fetched_at

    def test_source_becomes_best_source(self) -> None:
        raw = RawRate(**_raw_payload(source="WITS"))
        c = CanonicalRate.from_raw_rate(raw, confidence=0.65)
        assert c.best_source == "WITS"

    def test_sources_consulted_defaults_to_single_source(self) -> None:
        raw = RawRate(**_raw_payload(source="US HTS"))
        c = CanonicalRate.from_raw_rate(raw, confidence=0.65)
        assert c.sources_consulted == ["US HTS"]

    def test_sources_consulted_override_accepted(self) -> None:
        raw = RawRate(**_raw_payload())
        c = CanonicalRate.from_raw_rate(
            raw,
            confidence=0.45,
            sources_consulted=["US HTS", "WITS"],
        )
        assert c.sources_consulted == ["US HTS", "WITS"]

    def test_disagreement_details_passed_through(self) -> None:
        raw = RawRate(**_raw_payload())
        c = CanonicalRate.from_raw_rate(
            raw,
            confidence=0.45,
            disagreement_details=["Rate differs by 0.5pp"],
        )
        assert c.disagreement_details == ["Rate differs by 0.5pp"]

    def test_optional_fields_preserved(self) -> None:
        raw = RawRate(**_raw_payload(
            preferential_rate=0.0,
            applicable_fta="USMCA",
            rules_of_origin="RVC 75%",
            active_remedies=["Section 301 25%"],
        ))
        c = CanonicalRate.from_raw_rate(raw, confidence=0.9)
        assert c.preferential_rate == 0.0
        assert c.applicable_fta == "USMCA"
        assert c.rules_of_origin == "RVC 75%"
        assert c.active_remedies == ["Section 301 25%"]

    def test_duty_expression_passed_through(self) -> None:
        raw = RawRate(**_raw_payload(mfn_rate=None, duty_expression="6.8¢/kg"))
        c = CanonicalRate.from_raw_rate(raw, confidence=0.65)
        assert c.duty_expression == "6.8¢/kg"
        assert c.mfn_rate is None

    def test_preferential_expression_passed_through(self) -> None:
        raw = RawRate(**_raw_payload(preferential_expression="1.5c/kg", applicable_fta="USMCA"))
        c = CanonicalRate.from_raw_rate(raw, confidence=0.9)
        assert c.preferential_expression == "1.5c/kg"
        assert c.applicable_fta == "USMCA"

    def test_active_remedies_defensively_copied(self) -> None:
        """Mutating raw.active_remedies after promotion must not affect CanonicalRate."""
        raw = RawRate(**_raw_payload(active_remedies=["Section 301 25%"]))
        c = CanonicalRate.from_raw_rate(raw, confidence=0.9)
        raw.active_remedies.append("NEW REMEDY")
        assert "NEW REMEDY" not in c.active_remedies

    def test_confidence_score_carried(self) -> None:
        raw = RawRate(**_raw_payload())
        c = CanonicalRate.from_raw_rate(raw, confidence=0.62)
        assert c.confidence == 0.62

    def test_is_stale_defaults_false(self) -> None:
        raw = RawRate(**_raw_payload())
        c = CanonicalRate.from_raw_rate(raw, confidence=0.9)
        assert c.is_stale is False


# ===========================================================================
# Adapter contract — roundtrip to feed dict shape
# ===========================================================================

class TestAdapterContract:
    """
    Verify that every field needed to build a UI feed dict can be read from
    a CanonicalRate without any information loss.

    This does NOT build the adapter (that comes later); it confirms the
    source fields are present and cover the exact vocabulary the UI uses.
    """

    def _realistic_canonical(self) -> CanonicalRate:
        return CanonicalRate(**_canonical_payload(
            hs_code="8471.30",
            origin="VN",
            destination="EU",
            mfn_rate=4.5,
            preferential_rate=0.0,
            applicable_fta="EU-Vietnam FTA",
            rules_of_origin="RVC 45%",
            active_remedies=["Section 301 25%"],
            best_source="US HTS",
            sources_consulted=["US HTS", "WITS"],
            confidence=0.9,
            fetched_at=FIXED_TS,
        ))

    def test_headline_source_is_non_empty(self) -> None:
        c = self._realistic_canonical()
        assert c.summary  # maps to 'headline'

    def test_detail_source_is_non_empty(self) -> None:
        c = self._realistic_canonical()
        assert c.detail_summary  # maps to 'detail'

    def test_status_source_is_valid_vocabulary(self) -> None:
        """feed_status must only produce the two values get_tariff_feed() emits."""
        c = self._realistic_canonical()
        assert c.feed_status in {"cleared", "issued"}  # 'pending' is never emitted

    def test_source_field_is_non_empty(self) -> None:
        c = self._realistic_canonical()
        assert c.best_source  # maps to 'source'

    def test_timestamp_source_is_datetime(self) -> None:
        c = self._realistic_canonical()
        assert isinstance(c.fetched_at, datetime)  # maps to 'timestamp'

    def test_example_from_task_brief(self) -> None:
        """HS 8471.30, VN→EU: MFN 4.5%, preferential 0% under EU-Vietnam FTA, rule=RVC 45%"""
        c = CanonicalRate(**_canonical_payload(
            hs_code="8471.30",
            origin="VN",
            destination="EU",
            mfn_rate=4.5,
            preferential_rate=0.0,
            applicable_fta="EU-Vietnam FTA",
            rules_of_origin="RVC 45%",
            confidence=0.92,
        ))
        assert "8471.30" in c.summary
        assert "VN→EU" in c.summary
        assert "4.5%" in c.summary
        assert "EU-Vietnam FTA" in c.summary
        assert "RVC 45%" in c.detail_summary
        assert c.feed_status == "cleared"

    def test_specific_duty_lane_adapter_ready(self) -> None:
        """A specific-duty lane is still adapter-mappable — headline uses expression."""
        c = CanonicalRate(**_canonical_payload(
            hs_code="0402.10",
            origin="NZ",
            destination="US",
            mfn_rate=None,
            duty_expression="6.8¢/kg",
            confidence=0.65,
        ))
        assert c.summary          # non-empty headline
        assert "6.8¢/kg" in c.summary
        assert c.feed_status == "cleared"
        assert c.best_source
