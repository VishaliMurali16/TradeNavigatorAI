"""
Tests for aggregator/store.py.

All tests use ':memory:' SQLite — fully offline, no file system side-effects.
TestRateStoreConcurrency uses a real temp-file DB and proves both directions:
  - Without WAL + timeout=0 (_BrokenStore): concurrent writers MUST raise
    OperationalError("database is locked") — proves the test detects the bug.
  - With WAL + busy_timeout (real RateStore): 3 writers + 1 reader run clean
    for 500 iterations each — proves the fix works.

Test inventory
--------------
TestRateStoreInit           — table created on construction (no crash)
TestRateStoreGetEmpty       — missing keys return None
TestRateStoreUpsert         — basic insert and retrieve round-trip
TestRateStoreHs6Key         — various hs_code formats all map to the same hs6 row
TestRateStoreOverwrite      — second upsert replaces, not duplicates
TestRateStoreFields         — all CanonicalRate fields survive serialisation round-trip
TestRateStoreCaseFold       — origin/destination stored and queried case-insensitively
TestRateStoreMultiRow       — distinct lanes stored independently
TestRateStoreConcurrency    — negative + positive concurrency proofs on file-backed store
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import Generator

import pytest

from aggregator.models import CanonicalRate
from aggregator.store import RateStore, _to_hs6

TODAY = date(2025, 1, 15)
_TS = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)

_MINIMAL_FIELDS = dict(
    hs_code="847130",
    origin="VN",
    destination="US",
    effective_date=TODAY,
    mfn_rate=0.0,
    best_source="US HTS",
    sources_consulted=["US HTS"],
    confidence=0.90,
    fetched_at=_TS,
)


def _rate(**overrides) -> CanonicalRate:
    fields = {**_MINIMAL_FIELDS, **overrides}
    return CanonicalRate(**fields)


def _store() -> RateStore:
    return RateStore(":memory:")


class _BrokenStore(RateStore):
    """
    RateStore variant with no WAL and zero busy timeout.

    Used only as a negative control in TestRateStoreConcurrency to prove that
    the concurrency test can actually detect write-lock contention.  In SQLite's
    default journal_mode=DELETE, only one connection may hold a RESERVED lock at
    a time; with timeout=0, any second writer immediately raises
    OperationalError("database is locked") instead of waiting.
    """

    @contextmanager
    def _connect(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self._db_path, timeout=0)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# ===========================================================================
# _to_hs6 helper
# ===========================================================================

class TestToHs6:
    def test_strips_dots(self) -> None:
        assert _to_hs6("8471.30.01.00") == "847130"

    def test_truncates_to_6(self) -> None:
        assert _to_hs6("847130010000") == "847130"

    def test_plain_6_digits(self) -> None:
        assert _to_hs6("847130") == "847130"

    def test_strips_spaces(self) -> None:
        assert _to_hs6("8471 30") == "847130"

    def test_shorter_code_unchanged(self) -> None:
        assert _to_hs6("8471") == "8471"


# ===========================================================================
# Construction
# ===========================================================================

class TestRateStoreInit:
    def test_constructs_without_error(self) -> None:
        s = _store()
        assert s is not None

    def test_get_on_fresh_store_returns_none(self) -> None:
        s = _store()
        result = s.get("847130", "VN", "US", TODAY)
        assert result is None


# ===========================================================================
# get — missing keys
# ===========================================================================

class TestRateStoreGetEmpty:
    def test_wrong_hs_code(self) -> None:
        s = _store()
        s.upsert(_rate())
        assert s.get("999999", "VN", "US", TODAY) is None

    def test_wrong_origin(self) -> None:
        s = _store()
        s.upsert(_rate())
        assert s.get("847130", "CN", "US", TODAY) is None

    def test_wrong_destination(self) -> None:
        s = _store()
        s.upsert(_rate())
        assert s.get("847130", "VN", "DE", TODAY) is None

    def test_wrong_effective_date(self) -> None:
        s = _store()
        s.upsert(_rate())
        assert s.get("847130", "VN", "US", date(2024, 1, 1)) is None


# ===========================================================================
# Basic upsert + round-trip
# ===========================================================================

class TestRateStoreUpsert:
    def test_basic_round_trip(self) -> None:
        s = _store()
        r = _rate()
        s.upsert(r)
        result = s.get("847130", "VN", "US", TODAY)
        assert result is not None
        assert result.mfn_rate == 0.0
        assert result.best_source == "US HTS"

    def test_confidence_preserved(self) -> None:
        s = _store()
        s.upsert(_rate(confidence=0.75))
        result = s.get("847130", "VN", "US", TODAY)
        assert result is not None
        assert result.confidence == 0.75

    def test_sources_consulted_preserved(self) -> None:
        s = _store()
        s.upsert(_rate(sources_consulted=["US HTS", "WITS"]))
        result = s.get("847130", "VN", "US", TODAY)
        assert result is not None
        assert result.sources_consulted == ["US HTS", "WITS"]

    def test_disagreement_details_preserved(self) -> None:
        s = _store()
        s.upsert(_rate(disagreement_details=["US HTS 4.5%, WITS 5.0%"]))
        result = s.get("847130", "VN", "US", TODAY)
        assert result is not None
        assert result.disagreement_details == ["US HTS 4.5%, WITS 5.0%"]


# ===========================================================================
# hs6 key normalisation
# ===========================================================================

class TestRateStoreHs6Key:
    def test_dotted_hs_code_retrieves_same_row(self) -> None:
        s = _store()
        s.upsert(_rate(hs_code="8471.30.01.00"))
        result = s.get("8471.30", "VN", "US", TODAY)
        assert result is not None

    def test_long_hs_code_retrieves_same_row(self) -> None:
        s = _store()
        s.upsert(_rate(hs_code="8471300100"))  # 10-digit (max valid length)
        result = s.get("847130", "VN", "US", TODAY)
        assert result is not None

    def test_upsert_and_get_same_hs6_different_format(self) -> None:
        s = _store()
        s.upsert(_rate(hs_code="8471.30"))
        result = s.get("847130010000", "VN", "US", TODAY)
        assert result is not None

    def test_different_hs6_prefix_separate_rows(self) -> None:
        s = _store()
        s.upsert(_rate(hs_code="8471.30"))
        assert s.get("9999.99", "VN", "US", TODAY) is None


# ===========================================================================
# Second upsert overwrites
# ===========================================================================

class TestRateStoreOverwrite:
    def test_upsert_replaces_existing_row(self) -> None:
        s = _store()
        s.upsert(_rate(mfn_rate=4.5, confidence=0.90))
        s.upsert(_rate(mfn_rate=3.7, confidence=0.75))
        result = s.get("847130", "VN", "US", TODAY)
        assert result is not None
        assert result.mfn_rate == 3.7
        assert result.confidence == 0.75

    def test_no_duplicate_row_after_two_upserts(self) -> None:
        s = _store()
        s.upsert(_rate())
        s.upsert(_rate(mfn_rate=99.0))
        result = s.get("847130", "VN", "US", TODAY)
        # Only one row — reading should return the latest, not multiple.
        assert result is not None
        assert result.mfn_rate == 99.0


# ===========================================================================
# Full field round-trip
# ===========================================================================

class TestRateStoreFields:
    def test_fta_fields_round_trip(self) -> None:
        s = _store()
        r = CanonicalRate(
            hs_code="040690",
            origin="KR",
            destination="US",
            effective_date=TODAY,
            mfn_rate=9.6,
            preferential_rate=0.0,
            applicable_fta="KORUS",
            best_source="US HTS",
            sources_consulted=["US HTS"],
            confidence=0.90,
            fetched_at=_TS,
        )
        s.upsert(r)
        result = s.get("040690", "KR", "US", TODAY)
        assert result is not None
        assert result.mfn_rate == 9.6
        assert result.preferential_rate == 0.0
        assert result.applicable_fta == "KORUS"

    def test_specific_duty_round_trip(self) -> None:
        s = _store()
        r = CanonicalRate(
            hs_code="040690",
            origin="VN",
            destination="US",
            effective_date=TODAY,
            duty_expression="$1.803/kg",
            mfn_rate=None,
            best_source="US HTS",
            sources_consulted=["US HTS"],
            confidence=0.90,
            fetched_at=_TS,
        )
        s.upsert(r)
        result = s.get("040690", "VN", "US", TODAY)
        assert result is not None
        assert result.duty_expression == "$1.803/kg"
        assert result.mfn_rate is None

    def test_fetched_at_preserved(self) -> None:
        s = _store()
        s.upsert(_rate())
        result = s.get("847130", "VN", "US", TODAY)
        assert result is not None
        assert result.fetched_at == _TS


# ===========================================================================
# Case folding
# ===========================================================================

class TestRateStoreCaseFold:
    def test_lowercase_origin_query_finds_row(self) -> None:
        s = _store()
        s.upsert(_rate(origin="VN"))
        result = s.get("847130", "vn", "US", TODAY)
        assert result is not None

    def test_uppercase_origin_stored_retrieved_lowercase_query(self) -> None:
        s = _store()
        s.upsert(_rate(origin="VN"))
        result = s.get("847130", "VN", "us", TODAY)
        assert result is not None

    def test_mixed_case_destination(self) -> None:
        s = _store()
        s.upsert(_rate(destination="US"))
        result = s.get("847130", "VN", "Us", TODAY)
        assert result is not None


# ===========================================================================
# Multiple distinct rows
# ===========================================================================

class TestRateStoreMultiRow:
    def test_two_different_origins_independent(self) -> None:
        s = _store()
        s.upsert(_rate(origin="VN", mfn_rate=4.5))
        s.upsert(_rate(origin="CN", mfn_rate=7.5))
        assert s.get("847130", "VN", "US", TODAY).mfn_rate == 4.5
        assert s.get("847130", "CN", "US", TODAY).mfn_rate == 7.5

    def test_two_different_hs_codes_independent(self) -> None:
        s = _store()
        s.upsert(_rate(hs_code="847130", mfn_rate=0.0))
        s.upsert(_rate(hs_code="040690", mfn_rate=9.6))
        assert s.get("847130", "VN", "US", TODAY).mfn_rate == 0.0
        assert s.get("040690", "VN", "US", TODAY).mfn_rate == 9.6

    def test_two_different_dates_independent(self) -> None:
        s = _store()
        d1, d2 = date(2024, 1, 1), date(2025, 1, 15)
        s.upsert(_rate(effective_date=d1, mfn_rate=5.0, fetched_at=_TS))
        s.upsert(_rate(effective_date=d2, mfn_rate=4.5, fetched_at=_TS))
        assert s.get("847130", "VN", "US", d1).mfn_rate == 5.0
        assert s.get("847130", "VN", "US", d2).mfn_rate == 4.5

    def test_two_different_destinations_independent(self) -> None:
        s = _store()
        s.upsert(_rate(destination="US", mfn_rate=0.0))
        s.upsert(_rate(destination="DE", mfn_rate=3.5))
        assert s.get("847130", "VN", "US", TODAY).mfn_rate == 0.0
        assert s.get("847130", "VN", "DE", TODAY).mfn_rate == 3.5


# ===========================================================================
# list_recent
# ===========================================================================

_TS_EARLY  = datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC)
_TS_MIDDLE = datetime(2025, 1, 15, 11, 0, 0, tzinfo=UTC)
_TS_LATE   = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestRateStoreListRecent:
    def test_empty_store_returns_empty_list(self) -> None:
        assert _store().list_recent(10) == []

    def test_returns_canonical_rate_instances(self) -> None:
        s = _store()
        s.upsert(_rate())
        result = s.list_recent(10)
        assert len(result) == 1
        assert isinstance(result[0], CanonicalRate)

    def test_order_is_newest_first(self) -> None:
        s = _store()
        # Three distinct lanes (different origin) so each is a separate row.
        s.upsert(_rate(origin="VN", fetched_at=_TS_EARLY))
        s.upsert(_rate(origin="CN", fetched_at=_TS_MIDDLE))
        s.upsert(_rate(origin="KR", fetched_at=_TS_LATE))
        result = s.list_recent(10)
        assert [r.fetched_at for r in result] == [_TS_LATE, _TS_MIDDLE, _TS_EARLY]

    def test_all_rows_returned_within_limit(self) -> None:
        s = _store()
        s.upsert(_rate(origin="VN", fetched_at=_TS_EARLY))
        s.upsert(_rate(origin="CN", fetched_at=_TS_MIDDLE))
        s.upsert(_rate(origin="KR", fetched_at=_TS_LATE))
        assert len(s.list_recent(10)) == 3

    def test_limit_is_respected(self) -> None:
        s = _store()
        s.upsert(_rate(origin="VN", fetched_at=_TS_EARLY))
        s.upsert(_rate(origin="CN", fetched_at=_TS_MIDDLE))
        s.upsert(_rate(origin="KR", fetched_at=_TS_LATE))
        result = s.list_recent(2)
        assert len(result) == 2
        # Must be the two newest.
        assert result[0].fetched_at == _TS_LATE
        assert result[1].fetched_at == _TS_MIDDLE

    def test_limit_zero_returns_empty(self) -> None:
        s = _store()
        s.upsert(_rate())
        assert s.list_recent(0) == []


# ===========================================================================
# File-backed concurrency (WAL mode) — negative + positive proofs
# ===========================================================================

class TestRateStoreConcurrency:
    """
    Two complementary tests that together prove the WAL fix is both necessary
    and sufficient.

    test_no_wal_no_retry_fails_under_concurrent_writers  (negative control)
        Uses _BrokenStore (journal_mode=DELETE, timeout=0).  In DELETE mode
        only one connection may hold a RESERVED lock; with zero retry any
        second concurrent writer raises immediately.  This test asserts that
        errors DO occur — proving the test can detect the bug it guards against.

    test_file_backed_concurrent_read_write  (positive / regression)
        Uses the real RateStore (journal_mode=WAL, busy_timeout=10 000 ms).
        WAL allows readers and one writer to proceed concurrently without
        blocking each other.  3 writers + 1 reader run 500 iterations each;
        the test asserts zero errors.
    """

    _WRITERS = 3
    _ITERATIONS = 500

    def _run(self, store: RateStore) -> list[str]:
        errors: list[str] = []
        rate_a = _rate(mfn_rate=4.5, confidence=0.90)
        rate_b = _rate(mfn_rate=5.0, confidence=0.75)

        def writer(tid: int) -> None:
            for i in range(self._ITERATIONS):
                try:
                    store.upsert(rate_a if i % 2 == 0 else rate_b)
                except Exception as exc:
                    errors.append(f"writer{tid}[{i}]: {exc}")

        def reader() -> None:
            for i in range(self._ITERATIONS):
                try:
                    result = store.get("847130", "VN", "US", TODAY)
                    if result is not None and result.mfn_rate not in (4.5, 5.0):
                        errors.append(
                            f"reader[{i}]: unexpected mfn_rate {result.mfn_rate}"
                        )
                except Exception as exc:
                    errors.append(f"reader[{i}]: {exc}")

        threads = [
            threading.Thread(target=writer, args=(tid,))
            for tid in range(self._WRITERS)
        ]
        threads.append(threading.Thread(target=reader))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return errors

    def test_no_wal_no_retry_fails_under_concurrent_writers(self, tmp_path) -> None:
        """
        Negative control: _BrokenStore (no WAL, timeout=0) must produce
        'database is locked' errors under 3 concurrent writers.
        """
        store = _BrokenStore(str(tmp_path / "broken.db"))
        errors = self._run(store)
        assert len(errors) > 0, (
            "_BrokenStore (journal_mode=DELETE, timeout=0) produced no errors under "
            f"{self._WRITERS} concurrent writers × {self._ITERATIONS} iterations — "
            "write contention was not exercised."
        )
        assert any("locked" in e.lower() or "busy" in e.lower() for e in errors), (
            f"Expected 'locked'/'busy' in errors; got: {errors[:5]}"
        )

    def test_file_backed_concurrent_read_write(self, tmp_path) -> None:
        """
        Positive test: real RateStore (WAL + busy_timeout=10 000 ms) runs
        3 writers + 1 reader for 500 iterations each with no errors.
        """
        store = RateStore(str(tmp_path / "rates.db"))
        errors = self._run(store)
        assert errors == [], "Concurrent read/write errors:\n" + "\n".join(errors)
