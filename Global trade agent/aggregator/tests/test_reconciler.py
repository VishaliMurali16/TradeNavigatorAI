"""
Tests for aggregator/reconciler.py.

All tests are fully offline — no HTTP calls, no file I/O.
Connectors are thin stubs; config is an in-process dict.

Test inventory
--------------
TestReconcilerNoData         — returns None when all results empty
TestReconcilerAuth           — authoritative source scenarios (alone, agreeing, disagreeing)
TestReconcilerNonAuth        — non-authoritative-only scenarios (fallback, multi-agree, disagree)
TestReconcilerDisagreement   — disagreement_details content
TestReconcilerCrossRefCaveat — US HTS only + pref=None → caveat note
TestReconcilerConfig         — confidence and precedence read from config, not hardcoded
TestReconcilerAgreement      — _rates_agree edge cases (tolerance, specific duties, mixed)
TestReconcilerProvenanceFields — sources_consulted, best_source, fetched_at
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from aggregator.connectors.base import BaseConnector, ConnectorError
from aggregator.models import CanonicalRate, RawRate
from aggregator.reconciler import Reconciler, _config_key

# ---------------------------------------------------------------------------
# Test configuration — mirrors config.yaml structure but in-process
# ---------------------------------------------------------------------------
_CFG = {
    "precedence": {
        "us_hts": 90,
        "eu_taric": 90,
        "macmap": 50,
        "wits": 10,
    },
    "confidence": {
        "authoritative_present": 0.90,
        "non_authoritative_agree": 0.75,
        "fallback_only": 0.50,
        "sources_disagree": 0.45,
    },
}


# ---------------------------------------------------------------------------
# Minimal stub connectors
# ---------------------------------------------------------------------------

class _USHts(BaseConnector):
    """Authoritative for US."""
    name = "US HTS"
    def is_authoritative_for(self, d: str) -> bool: return d.upper() == "US"
    def fetch(self, *a, **kw): raise NotImplementedError

class _WITS(BaseConnector):
    """Non-authoritative (global aggregator)."""
    name = "WITS"
    def is_authoritative_for(self, d: str) -> bool: return False
    def fetch(self, *a, **kw): raise NotImplementedError

class _MacMap(BaseConnector):
    """Non-authoritative."""
    name = "MacMap"
    def is_authoritative_for(self, d: str) -> bool: return False
    def fetch(self, *a, **kw): raise NotImplementedError

class _EUTaric(BaseConnector):
    """Authoritative for EU."""
    name = "EU TARIC"
    def is_authoritative_for(self, d: str) -> bool: return d.upper() == "EU"
    def fetch(self, *a, **kw): raise NotImplementedError


_US_HTS = _USHts()
_WITS = _WITS()
_MACMAP = _MacMap()
_EU_TARIC = _EUTaric()

_ALL_CONNECTORS = [_US_HTS, _WITS, _MACMAP, _EU_TARIC]

TODAY = date(2025, 1, 15)
_TS = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)

LANE = dict(hs_code="847130", origin="VN", destination="US", effective_date=TODAY)

_WITS_ERR = ConnectorError("WITS", Exception("403 Forbidden"))
_US_HTS_ERR = ConnectorError("US HTS", Exception("timeout"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rec() -> Reconciler:
    return Reconciler(config=_CFG)


def _raw(mfn_rate=4.5, source="US HTS", **kwargs) -> RawRate:
    base = dict(
        hs_code="847130",
        origin="VN",
        destination="US",
        effective_date=TODAY,
        mfn_rate=mfn_rate,
        source=source,
        fetched_at=_TS,
    )
    base.update(kwargs)
    return RawRate(**base)


def _raw_specific(duty_expression: str, source: str = "US HTS") -> RawRate:
    return RawRate(
        hs_code="847130",
        origin="VN",
        destination="US",
        effective_date=TODAY,
        mfn_rate=None,
        duty_expression=duty_expression,
        source=source,
        fetched_at=_TS,
    )


def _reconcile(results, errors=None, connectors=None):
    rec = _make_rec()
    return rec.reconcile(
        connectors=connectors or _ALL_CONNECTORS,
        errors=errors or {},
        **LANE,
        results=results,
    )


# ===========================================================================
# No data at all → None
# ===========================================================================

class TestReconcilerNoData:
    def test_all_empty_results_returns_none(self) -> None:
        assert _reconcile({"US HTS": [], "WITS": []}) is None

    def test_all_errors_returns_none(self) -> None:
        result = _reconcile(
            results={},
            errors={"US HTS": _US_HTS_ERR, "WITS": _WITS_ERR},
        )
        assert result is None

    def test_mixed_empty_and_error_returns_none(self) -> None:
        assert _reconcile(results={"WITS": []}, errors={"US HTS": _US_HTS_ERR}) is None


# ===========================================================================
# Authoritative source scenarios
# ===========================================================================

class TestReconcilerAuth:
    def test_single_auth_source_high_confidence(self) -> None:
        c = _reconcile({"US HTS": [_raw()]})
        assert c is not None
        assert c.confidence == 0.90
        assert c.best_source == "US HTS"

    def test_auth_and_nonauth_agree_high_confidence(self) -> None:
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(4.5, source="WITS")]})
        assert c is not None
        assert c.confidence == 0.90
        assert c.best_source == "US HTS"

    def test_auth_and_nonauth_within_tolerance_agree(self) -> None:
        # 4.5 vs 4.55 — within 0.1pp
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(4.55, source="WITS")]})
        assert c is not None
        assert c.confidence == 0.90

    def test_auth_and_nonauth_disagree_low_confidence(self) -> None:
        # 4.5 vs 5.0 — beyond 0.1pp tolerance
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(5.0, source="WITS")]})
        assert c is not None
        assert c.confidence == 0.45

    def test_auth_disagree_picks_authoritative_value(self) -> None:
        """When auth and non-auth disagree, the canonical rate comes from auth."""
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(5.0, source="WITS")]})
        assert c is not None
        assert c.mfn_rate == 4.5
        assert c.best_source == "US HTS"

    def test_auth_and_nonauth_disagree_records_both(self) -> None:
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(5.0, source="WITS")]})
        assert c is not None
        assert len(c.disagreement_details) >= 1
        note = c.disagreement_details[0]
        assert "4.5" in note and "5.0" in note

    def test_two_auth_sources_both_agree_high(self) -> None:
        """US HTS + EU TARIC both authoritative (for their destinations) — if same dest, both auth."""
        # For dest=US, only US HTS is authoritative; EU TARIC is non-auth.
        c = _reconcile(
            {"US HTS": [_raw(4.5)], "EU TARIC": [_raw(4.5, source="EU TARIC")]},
        )
        assert c is not None
        assert c.confidence == 0.90  # EU TARIC is non-auth for US → treated as agreeing non-auth

    def test_auth_source_is_canonical_not_nonauth(self) -> None:
        """Even when non-auth has lower rate, auth value is used."""
        c = _reconcile({"US HTS": [_raw(8.0)], "WITS": [_raw(3.0, source="WITS")]})
        assert c is not None
        assert c.mfn_rate == 8.0


# ===========================================================================
# Non-authoritative-only scenarios
# ===========================================================================

class TestReconcilerNonAuth:
    def test_only_wits_auth_errored_fallback_confidence(self) -> None:
        """WITS only, US HTS errored → fallback_only confidence."""
        c = _reconcile(
            results={"WITS": [_raw(4.5, source="WITS")]},
            errors={"US HTS": _US_HTS_ERR},
        )
        assert c is not None
        assert c.confidence == 0.50

    def test_only_wits_auth_errored_adds_note(self) -> None:
        """Authoritative source unavailable → note in disagreement_details."""
        c = _reconcile(
            results={"WITS": [_raw(4.5, source="WITS")]},
            errors={"US HTS": _US_HTS_ERR},
        )
        assert c is not None
        notes = " ".join(c.disagreement_details)
        assert "US HTS" in notes
        assert "unavailable" in notes

    def test_only_wits_no_auth_configured_fallback(self) -> None:
        """No authoritative connector for destination — WITS alone → fallback confidence."""
        c = _reconcile(
            results={"WITS": [_raw(4.5, source="WITS")]},
            errors={},
            connectors=[_WITS],   # only WITS, no US HTS
        )
        assert c is not None
        assert c.confidence == 0.50

    def test_multiple_nonauth_agree_med_high(self) -> None:
        c = _reconcile(
            results={
                "WITS": [_raw(4.5, source="WITS")],
                "MacMap": [_raw(4.5, source="MacMap")],
            },
            errors={"US HTS": _US_HTS_ERR},
            connectors=[_WITS, _MACMAP, _US_HTS],
        )
        assert c is not None
        assert c.confidence == 0.75

    def test_multiple_nonauth_disagree_low(self) -> None:
        c = _reconcile(
            results={
                "WITS": [_raw(4.5, source="WITS")],
                "MacMap": [_raw(7.0, source="MacMap")],
            },
            errors={"US HTS": _US_HTS_ERR},
            connectors=[_WITS, _MACMAP, _US_HTS],
        )
        assert c is not None
        assert c.confidence == 0.45

    def test_multiple_nonauth_disagree_picks_highest_precedence(self) -> None:
        """MacMap (precedence 50) beats WITS (precedence 10)."""
        c = _reconcile(
            results={
                "WITS": [_raw(4.5, source="WITS")],
                "MacMap": [_raw(7.0, source="MacMap")],
            },
            errors={"US HTS": _US_HTS_ERR},
            connectors=[_WITS, _MACMAP, _US_HTS],
        )
        assert c is not None
        assert c.mfn_rate == 7.0
        assert c.best_source == "MacMap"

    def test_wits_is_actual_real_failure_scenario(self) -> None:
        """Use the real WITS 403 as the test scenario: only WITS, auth unreachable."""
        wits_rate = _raw(source="WITS", mfn_rate=0.0)
        c = _reconcile(
            results={"WITS": [wits_rate]},
            errors={"US HTS": ConnectorError("US HTS", Exception("403 Forbidden"))},
        )
        assert c is not None
        assert c.confidence == 0.50
        assert any("US HTS" in d for d in c.disagreement_details)


# ===========================================================================
# Disagreement detail content
# ===========================================================================

class TestReconcilerDisagreement:
    def test_disagreement_note_contains_source_names(self) -> None:
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(6.0, source="WITS")]})
        assert c is not None
        note = c.disagreement_details[0]
        assert "US HTS" in note
        assert "WITS" in note

    def test_disagreement_note_contains_rate_values(self) -> None:
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(6.0, source="WITS")]})
        assert c is not None
        note = c.disagreement_details[0]
        assert "4.5" in note
        assert "6.0" in note

    def test_no_disagreement_details_when_agree(self) -> None:
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(4.5, source="WITS")]})
        assert c is not None
        assert c.disagreement_details == []

    def test_specific_duty_disagreement_in_note(self) -> None:
        """Specific duty string appears in the note."""
        c = _reconcile({
            "US HTS": [_raw_specific("$1.80/kg")],
            "WITS": [_raw(4.5, source="WITS")],
        })
        assert c is not None
        assert "$1.80/kg" in c.disagreement_details[0]


# ===========================================================================
# Cross-reference caveat
# ===========================================================================

class TestReconcilerCrossRefCaveat:
    def test_caveat_added_when_us_hts_only_and_pref_none(self) -> None:
        """US HTS alone, preferential_rate=None → caveat note added."""
        raw = _raw(mfn_rate=0.0, source="US HTS")
        # preferential_rate is None by default in _raw
        c = _reconcile({"US HTS": [raw]})
        assert c is not None
        assert any("cross-reference" in d for d in c.disagreement_details)

    def test_caveat_not_added_when_preferential_rate_present(self) -> None:
        """If the connector DID resolve an FTA rate, no caveat."""
        raw = RawRate(
            hs_code="847130", origin="KR", destination="US",
            effective_date=TODAY, source="US HTS",
            mfn_rate=0.0, preferential_rate=0.0, applicable_fta="KORUS",
            fetched_at=_TS,
        )
        c = _reconcile({"US HTS": [raw]}, connectors=[_US_HTS])
        assert c is not None
        assert not any("cross-reference" in d for d in c.disagreement_details)

    def test_caveat_not_added_with_multiple_sources(self) -> None:
        """Cross-ref caveat only fires when US HTS is the ONLY source with data."""
        c = _reconcile({"US HTS": [_raw()], "WITS": [_raw(4.5, source="WITS")]})
        assert c is not None
        assert not any("cross-reference" in d for d in c.disagreement_details)

    def test_caveat_not_added_for_non_us_hts_source(self) -> None:
        """Caveat is US-HTS-specific."""
        c = _reconcile(
            {"WITS": [_raw(4.5, source="WITS")]},
            errors={"US HTS": _US_HTS_ERR},
        )
        assert c is not None
        assert not any("cross-reference" in d for d in c.disagreement_details)


# ===========================================================================
# Config-driven confidence + precedence
# ===========================================================================

class TestReconcilerConfig:
    def test_confidence_values_come_from_config_not_hardcoded(self) -> None:
        custom_cfg = {
            **_CFG,
            "confidence": {
                "authoritative_present": 0.99,
                "non_authoritative_agree": 0.80,
                "fallback_only": 0.30,
                "sources_disagree": 0.10,
            },
        }
        rec = Reconciler(config=custom_cfg)
        c = rec.reconcile(connectors=_ALL_CONNECTORS, errors={}, **LANE,
                          results={"US HTS": [_raw()]})
        assert c is not None
        assert c.confidence == 0.99

    def test_fallback_confidence_from_config(self) -> None:
        custom_cfg = {**_CFG, "confidence": {**_CFG["confidence"], "fallback_only": 0.33}}
        rec = Reconciler(config=custom_cfg)
        c = rec.reconcile(connectors=_ALL_CONNECTORS,
                          errors={"US HTS": _US_HTS_ERR}, **LANE,
                          results={"WITS": [_raw(4.5, source="WITS")]})
        assert c is not None
        assert c.confidence == 0.33

    def test_precedence_determines_best_source_among_nonauth(self) -> None:
        """MacMap (50) > WITS (10) → MacMap is chosen when both non-auth."""
        custom_cfg = {**_CFG, "precedence": {"macmap": 50, "wits": 10}}
        rec = Reconciler(config=custom_cfg)
        c = rec.reconcile(
            connectors=[_WITS, _MACMAP],
            errors={"US HTS": _US_HTS_ERR},
            **LANE,
            results={
                "WITS": [_raw(3.0, source="WITS")],
                "MacMap": [_raw(3.0, source="MacMap")],
            },
        )
        assert c is not None
        assert c.best_source == "MacMap"

    def test_missing_confidence_band_falls_back_to_0_5(self) -> None:
        """A config missing a band falls back to 0.5 rather than crashing."""
        rec = Reconciler(config={"precedence": {"us_hts": 90}, "confidence": {}})
        c = rec.reconcile(connectors=[_US_HTS], errors={}, **LANE,
                          results={"US HTS": [_raw()]})
        assert c is not None
        assert c.confidence == 0.5


# ===========================================================================
# _rates_agree edge cases
# ===========================================================================

def _raw_w(mfn: float) -> RawRate:
    return _raw(mfn_rate=mfn, source="WITS")


class TestReconcilerAgreement:
    def _rec(self): return _make_rec()

    def test_within_tolerance_agree(self) -> None:
        r = self._rec()
        assert r._rates_agree(_raw(4.5), _raw_w(4.55))

    def test_at_exact_tolerance_agree(self) -> None:
        r = self._rec()
        assert r._rates_agree(_raw(4.5), _raw_w(4.6))  # exactly 0.1

    def test_beyond_tolerance_disagree(self) -> None:
        r = self._rec()
        assert not r._rates_agree(_raw(4.5), _raw_w(4.61))

    def test_exact_equality_agree(self) -> None:
        r = self._rec()
        assert r._rates_agree(_raw(4.5), _raw_w(4.5))

    def test_same_specific_duty_agree(self) -> None:
        r = self._rec()
        assert r._rates_agree(_raw_specific("$1.80/kg"), _raw_specific("$1.80/kg", "WITS"))

    def test_different_specific_duty_disagree(self) -> None:
        r = self._rec()
        assert not r._rates_agree(_raw_specific("$1.80/kg"), _raw_specific("$2.10/kg", "WITS"))

    def test_mixed_float_and_specific_not_confirmable(self) -> None:
        r = self._rec()
        assert not r._rates_agree(_raw(4.5), _raw_specific("$1.80/kg", "WITS"))

    def test_mixed_specific_and_float_not_confirmable(self) -> None:
        r = self._rec()
        assert not r._rates_agree(_raw_specific("$1.80/kg"), _raw_w(4.5))


# ===========================================================================
# Provenance fields
# ===========================================================================

class TestReconcilerProvenanceFields:
    def test_sources_consulted_includes_errored_sources(self) -> None:
        c = _reconcile(
            results={"WITS": [_raw(4.5, source="WITS")]},
            errors={"US HTS": _US_HTS_ERR},
        )
        assert c is not None
        assert "US HTS" in c.sources_consulted
        assert "WITS" in c.sources_consulted

    def test_sources_consulted_includes_empty_result_sources(self) -> None:
        """A connector that returned [] was still consulted."""
        c = _reconcile({"US HTS": [_raw()], "WITS": []})
        assert c is not None
        assert "WITS" in c.sources_consulted

    def test_best_source_is_connector_name(self) -> None:
        c = _reconcile({"US HTS": [_raw()]})
        assert c is not None
        assert c.best_source == "US HTS"

    def test_fetched_at_comes_from_best_rate(self) -> None:
        c = _reconcile({"US HTS": [_raw()]})
        assert c is not None
        assert c.fetched_at == _TS

    def test_canonical_rate_fields_match_best_raw_rate(self) -> None:
        raw = _raw(mfn_rate=3.7)
        c = _reconcile({"US HTS": [raw]})
        assert c is not None
        assert c.mfn_rate == 3.7
        assert c.hs_code == raw.hs_code
        assert c.origin == raw.origin
        assert c.destination == raw.destination
        assert c.effective_date == raw.effective_date


# ===========================================================================
# None-vs-value: silence is not a disagreement
# ===========================================================================

def _raw_silent(source: str = "WITS") -> RawRate:
    """A RawRate where mfn_rate=None and duty_expression=None (source had no MFN data)."""
    return RawRate(
        hs_code="847130",
        origin="VN",
        destination="US",
        effective_date=TODAY,
        mfn_rate=None,
        duty_expression=None,
        source=source,
        fetched_at=_TS,
    )


class TestReconcilerNoneVsValue:
    """
    When one source has mfn_rate=None and another has a concrete value,
    the None-source is silent — it must NOT count as a disagreement and must
    NOT lower confidence to sources_disagree (0.45).
    """

    def test_nonauth_silent_mfn_does_not_lower_confidence(self) -> None:
        """US HTS reports 4.5%; WITS has no MFN data — should NOT disagree."""
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw_silent("WITS")]})
        assert c is not None
        assert c.confidence == 0.90, (
            f"Expected authoritative_present (0.90), got {c.confidence}"
        )

    def test_nonauth_silent_produces_no_disagreement_note(self) -> None:
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw_silent("WITS")]})
        assert c is not None
        assert c.disagreement_details == [] or all(
            "WITS" not in d for d in c.disagreement_details
        ), "WITS silence should not generate a disagreement note"

    def test_auth_silent_mfn_nonauth_has_value_not_a_disagreement(self) -> None:
        """When the auth source is the silent one, WITS value still not a conflict."""
        c = _reconcile(
            {"US HTS": [_raw_silent("US HTS")], "WITS": [_raw(4.5, source="WITS")]},
        )
        assert c is not None
        # Auth source has data (rate object present) but silent on MFN — still auth_data
        # path; silence vs real value is not a disagreement.
        assert c.confidence != 0.45, "Silence vs value must not produce sources_disagree"

    def test_genuine_mfn_disagreement_still_lowers_confidence(self) -> None:
        """The fix must not suppress real disagreements (both present, both differ)."""
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(6.0, source="WITS")]})
        assert c is not None
        assert c.confidence == 0.45

    def test_genuine_disagreement_still_produces_note(self) -> None:
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(6.0, source="WITS")]})
        assert c is not None
        assert len(c.disagreement_details) >= 1
        note = c.disagreement_details[0]
        assert "4.5" in note and "6.0" in note


# ===========================================================================
# MFN backfill: auth selected but silent on MFN → backfill from non-auth
# ===========================================================================
#
# The US HTS connector filters out items where general="" (both MFN fields
# become None) and returns [] for that entry, so a silent-on-MFN RawRate
# never enters `available` via US HTS in practice.  The reconciler must still
# handle this correctly for future auth connectors that lack this filter.
#
# When the auth source is silent and MFN is backfilled from a non-auth source:
#   - canonical mfn_rate = non-auth value (not None)
#   - confidence = fallback_only (0.50) — auth contributed no rate data
#   - best_source = the selected auth connector (structural selection, not rate provenance)
#   - WITS (fill source) in sources_consulted
#
# Connector-filter guarantee: the US HTS test
#   test_general_empty_string_item_skipped asserts that _parse() returns []
#   (not a silent RawRate) when general="".  That test is in test_connectors.py
#   and constitutes the filter-holds assertion for the current auth connector.

class TestReconcilerMFNBackfill:

    def test_auth_silent_mfn_backfilled_from_wits(self) -> None:
        """Core case: auth present but silent on MFN, WITS has 4.5% — backfill."""
        c = _reconcile({"US HTS": [_raw_silent("US HTS")], "WITS": [_raw(4.5, source="WITS")]})
        assert c is not None
        assert c.mfn_rate == 4.5, "MFN must be backfilled from WITS when auth is silent"

    def test_auth_silent_mfn_confidence_is_fallback_not_authoritative(self) -> None:
        """Auth contributed no rate — confidence must be fallback_only (0.50), not 0.90."""
        c = _reconcile({"US HTS": [_raw_silent("US HTS")], "WITS": [_raw(4.5, source="WITS")]})
        assert c is not None
        assert c.confidence == 0.50, (
            f"Auth gave no MFN data; expected fallback_only (0.50), got {c.confidence}"
        )

    def test_auth_silent_mfn_both_sources_in_sources_consulted(self) -> None:
        c = _reconcile({"US HTS": [_raw_silent("US HTS")], "WITS": [_raw(4.5, source="WITS")]})
        assert c is not None
        assert "US HTS" in c.sources_consulted
        assert "WITS" in c.sources_consulted

    def test_auth_silent_mfn_best_source_remains_selected_connector(self) -> None:
        """best_source reflects structural selection, not rate provenance."""
        c = _reconcile({"US HTS": [_raw_silent("US HTS")], "WITS": [_raw(4.5, source="WITS")]})
        assert c is not None
        assert c.best_source == "US HTS"

    def test_no_mfn_backfill_when_all_sources_silent(self) -> None:
        """No other source has MFN data — canonical mfn_rate stays None."""
        c = _reconcile({"US HTS": [_raw_silent("US HTS")], "WITS": [_raw_silent("WITS")]})
        assert c is not None
        assert c.mfn_rate is None
        assert c.duty_expression is None

    def test_no_mfn_backfill_when_auth_already_has_mfn(self) -> None:
        """Auth has a concrete MFN — WITS value is irrelevant to backfill."""
        c = _reconcile({"US HTS": [_raw(4.5)], "WITS": [_raw(3.0, source="WITS")]})
        assert c is not None
        assert c.mfn_rate == 4.5  # auth value kept (disagreement case, not backfill)

    def test_mfn_backfill_picks_highest_precedence_nonauth(self) -> None:
        """MacMap (prec=50) beats WITS (prec=10) as the MFN fill source."""
        macmap_rate = _raw(7.0, source="MacMap")
        wits_rate = _raw(3.0, source="WITS")
        c = _reconcile({
            "US HTS": [_raw_silent("US HTS")],
            "MacMap": [macmap_rate],
            "WITS": [wits_rate],
        })
        assert c is not None
        assert c.mfn_rate == 7.0, "MacMap (higher precedence) must be the fill source"

    def test_mfn_backfill_specific_duty(self) -> None:
        """Specific duty string is also backfilled when auth is silent."""
        wits_specific = RawRate(
            hs_code="040690", origin="VN", destination="US",
            effective_date=TODAY, source="WITS",
            mfn_rate=None, duty_expression="$1.803/kg",
            fetched_at=_TS,
        )
        c = _reconcile(
            {"US HTS": [_raw_silent("US HTS")], "WITS": [wits_specific]},
        )
        assert c is not None
        assert c.duty_expression == "$1.803/kg"
        assert c.mfn_rate is None


# ===========================================================================
# Preferential backfill: auth MFN wins, non-auth pref is kept not dropped
# ===========================================================================

def _raw_with_pref(pref_rate: float, fta: str, source: str = "WITS") -> RawRate:
    return RawRate(
        hs_code="847130",
        origin="KR",
        destination="US",
        effective_date=TODAY,
        mfn_rate=4.5,
        preferential_rate=pref_rate,
        applicable_fta=fta,
        source=source,
        fetched_at=_TS,
    )


def _raw_kr(mfn_rate: float = 4.5, source: str = "US HTS") -> RawRate:
    """US HTS rate for KR→US with no preferential resolved (xref case)."""
    return RawRate(
        hs_code="847130",
        origin="KR",
        destination="US",
        effective_date=TODAY,
        mfn_rate=mfn_rate,
        source=source,
        fetched_at=_TS,
    )


_KR_LANE = dict(hs_code="847130", origin="KR", destination="US", effective_date=TODAY)


def _reconcile_kr(results, errors=None, connectors=None):
    rec = _make_rec()
    return rec.reconcile(
        connectors=connectors or _ALL_CONNECTORS,
        errors=errors or {},
        **_KR_LANE,
        results=results,
    )


class TestReconcilerPrefBackfill:
    """
    When the authoritative source wins on MFN but has preferential_rate=None,
    and a non-authoritative source has preferential_rate=<value> with an FTA,
    the canonical output must backfill the preferential fields from the non-auth
    source.  Confidence must not drop; WITS must appear in sources_consulted.
    """

    def test_backfill_pref_from_wits_when_hts_has_none(self) -> None:
        """Core case: US HTS MFN wins, WITS pref=0.0/KORUS backfilled."""
        hts_rate = _raw_kr(mfn_rate=4.5, source="US HTS")
        wits_rate = _raw_with_pref(0.0, "KORUS", source="WITS")
        c = _reconcile_kr({"US HTS": [hts_rate], "WITS": [wits_rate]})
        assert c is not None
        assert c.mfn_rate == 4.5, "MFN must come from authoritative US HTS"
        assert c.preferential_rate == 0.0, "Preferential must be backfilled from WITS"
        assert c.applicable_fta == "KORUS"

    def test_backfill_does_not_lower_confidence(self) -> None:
        """Backfilling pref from a non-auth source is not a disagreement on MFN."""
        hts_rate = _raw_kr(mfn_rate=4.5)
        wits_rate = _raw_with_pref(0.0, "KORUS", source="WITS")
        c = _reconcile_kr({"US HTS": [hts_rate], "WITS": [wits_rate]})
        assert c is not None
        assert c.confidence == 0.90, (
            f"Backfill must not lower confidence; got {c.confidence}"
        )

    def test_backfill_produces_no_disagreement_note_on_pref(self) -> None:
        """A missing→filled pref is not a conflict — no note should be added."""
        hts_rate = _raw_kr(mfn_rate=4.5)
        wits_rate = _raw_with_pref(0.0, "KORUS", source="WITS")
        c = _reconcile_kr({"US HTS": [hts_rate], "WITS": [wits_rate]})
        assert c is not None
        # The only allowed note is the xref caveat (if still present) — no "WITS
        # reported" disagreement note about the preferential field.
        pref_conflict_notes = [d for d in c.disagreement_details if "KORUS" in d and "%" in d]
        assert pref_conflict_notes == []

    def test_wits_in_sources_consulted_after_backfill(self) -> None:
        hts_rate = _raw_kr(mfn_rate=4.5)
        wits_rate = _raw_with_pref(0.0, "KORUS", source="WITS")
        c = _reconcile_kr({"US HTS": [hts_rate], "WITS": [wits_rate]})
        assert c is not None
        assert "WITS" in c.sources_consulted

    def test_no_backfill_when_auth_already_has_pref(self) -> None:
        """If US HTS resolved the pref itself, WITS pref must not override it."""
        hts_rate = RawRate(
            hs_code="847130", origin="KR", destination="US",
            effective_date=TODAY, source="US HTS",
            mfn_rate=4.5, preferential_rate=0.0, applicable_fta="KORUS",
            fetched_at=_TS,
        )
        wits_rate = _raw_with_pref(1.5, "KORUS-OTHER", source="WITS")
        c = _reconcile_kr({"US HTS": [hts_rate], "WITS": [wits_rate]})
        assert c is not None
        assert c.preferential_rate == 0.0
        assert c.applicable_fta == "KORUS"

    def test_backfill_removes_xref_caveat(self) -> None:
        """Once a pref is backfilled, the 'may be a cross-reference' caveat is moot."""
        hts_rate = _raw_kr(mfn_rate=0.0)   # Free (pref=None, xref caveat would fire)
        wits_rate = _raw_with_pref(0.0, "KORUS", source="WITS")
        c = _reconcile_kr({"US HTS": [hts_rate], "WITS": [wits_rate]})
        assert c is not None
        assert not any("cross-reference" in d for d in c.disagreement_details)

    def test_backfill_from_highest_precedence_nonauth(self) -> None:
        """When MacMap (prec=50) and WITS (prec=10) both have pref, MacMap wins."""
        hts_rate = _raw_kr(mfn_rate=4.5)
        wits_rate = _raw_with_pref(0.0, "KORUS", source="WITS")
        macmap_rate = _raw_with_pref(1.0, "KORUS-MACMAP", source="MacMap")
        c = _reconcile_kr(
            {"US HTS": [hts_rate], "WITS": [wits_rate], "MacMap": [macmap_rate]},
            connectors=_ALL_CONNECTORS,
        )
        assert c is not None
        assert c.applicable_fta == "KORUS-MACMAP"  # MacMap wins on precedence

    def test_no_backfill_when_all_others_also_have_none_pref(self) -> None:
        """No other source has pref data — canonical pref stays None."""
        hts_rate = _raw_kr(mfn_rate=4.5)
        wits_rate = _raw(4.5, source="WITS")   # no preferential fields
        c = _reconcile_kr({"US HTS": [hts_rate], "WITS": [wits_rate]})
        assert c is not None
        assert c.preferential_rate is None
        assert c.applicable_fta is None
