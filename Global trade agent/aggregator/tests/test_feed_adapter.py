"""
Tests for aggregator/feed_adapter.py.

Pure unit tests — no mocking, no HTTP calls.

Test inventory
--------------
TestToFeedEntry — field mapping, key presence, format of time strings
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from aggregator.feed_adapter import to_feed_entry
from aggregator.models import CanonicalRate

_TS = datetime(2025, 1, 15, 14, 23, 0, tzinfo=UTC)

_REQUIRED_KEYS = {"id", "timestamp", "time_short", "headline", "detail", "status", "source"}

_BASE = dict(
    hs_code="847130",
    origin="VN",
    destination="US",
    effective_date=date(2025, 1, 15),
    mfn_rate=4.5,
    best_source="US HTS",
    sources_consulted=["US HTS"],
    confidence=0.90,
    fetched_at=_TS,
)


def _rate(**overrides) -> CanonicalRate:
    return CanonicalRate(**{**_BASE, **overrides})


class TestToFeedEntry:
    def test_returns_dict(self) -> None:
        assert isinstance(to_feed_entry(_rate()), dict)

    def test_all_required_keys_present(self) -> None:
        entry = to_feed_entry(_rate())
        assert _REQUIRED_KEYS == set(entry.keys())

    def test_id_contains_hs6(self) -> None:
        entry = to_feed_entry(_rate())
        assert "847130" in entry["id"]

    def test_id_contains_origin(self) -> None:
        entry = to_feed_entry(_rate())
        assert "VN" in entry["id"]

    def test_id_contains_destination(self) -> None:
        entry = to_feed_entry(_rate())
        assert "US" in entry["id"]

    def test_id_format(self) -> None:
        entry = to_feed_entry(_rate())
        assert entry["id"] == "847130_VN_US_2025-01-15"

    def test_headline_uses_summary(self) -> None:
        r = _rate()
        entry = to_feed_entry(r)
        assert entry["headline"] == r.summary

    def test_detail_uses_detail_summary(self) -> None:
        r = _rate()
        entry = to_feed_entry(r)
        assert entry["detail"] == r.detail_summary

    def test_status_uses_feed_status(self) -> None:
        r = _rate()
        entry = to_feed_entry(r)
        assert entry["status"] == r.feed_status

    def test_source_uses_best_source(self) -> None:
        entry = to_feed_entry(_rate())
        assert entry["source"] == "US HTS"

    def test_timestamp_format_includes_utc(self) -> None:
        entry = to_feed_entry(_rate())
        # e.g. "14:23 UTC"
        assert "UTC" in entry["timestamp"]
        assert "14:23" in entry["timestamp"]

    def test_time_short_format(self) -> None:
        entry = to_feed_entry(_rate())
        assert entry["time_short"] == "14:23"

    def test_status_cleared_for_high_confidence(self) -> None:
        entry = to_feed_entry(_rate(confidence=0.90))
        assert entry["status"] == "cleared"

    def test_status_cleared_for_medium_confidence(self) -> None:
        entry = to_feed_entry(_rate(confidence=0.50))
        assert entry["status"] == "cleared"

    def test_status_issued_for_low_confidence(self) -> None:
        entry = to_feed_entry(_rate(confidence=0.45))
        assert entry["status"] == "issued"

    def test_dotted_hs_code_id_uses_hs6(self) -> None:
        r = CanonicalRate(**{**_BASE, "hs_code": "8471.30"})
        entry = to_feed_entry(r)
        assert entry["id"] == "847130_VN_US_2025-01-15"

    def test_headline_includes_hs_code(self) -> None:
        entry = to_feed_entry(_rate())
        assert "847130" in entry["headline"] or "8471" in entry["headline"]

    def test_headline_includes_rate(self) -> None:
        entry = to_feed_entry(_rate(mfn_rate=4.5))
        assert "4.5" in entry["headline"]

    def test_headline_includes_fta_when_present(self) -> None:
        r = CanonicalRate(**{
            **_BASE,
            "preferential_rate": 0.0,
            "applicable_fta": "KORUS",
        })
        entry = to_feed_entry(r)
        assert "KORUS" in entry["headline"]

    def test_detail_no_additional_details_when_empty(self) -> None:
        entry = to_feed_entry(_rate())
        assert entry["detail"] == "No additional details"

    def test_detail_includes_remedy_when_present(self) -> None:
        r = CanonicalRate(**{**_BASE, "active_remedies": ["Section 301 25%"]})
        entry = to_feed_entry(r)
        assert "Section 301 25%" in entry["detail"]
