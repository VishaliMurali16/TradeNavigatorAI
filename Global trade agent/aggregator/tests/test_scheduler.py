"""
Tests for aggregator/scheduler.py.

All tests call _refresh_lane() directly — no background thread is started,
so APScheduler is not exercised (that would require real-time waits).

Test inventory
--------------
TestRateSchedulerCallback  — subscriber notifications (change / no change / first fetch)
TestRateSchedulerStaleness — stale marking when query fails
TestRateSchedulerRefreshAll — _refresh_all iterates lanes, errors isolated
TestRateDateChanged        — _rate_data_changed helper unit tests
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from aggregator.models import CanonicalRate
from aggregator.scheduler import RateScheduler, _rate_data_changed

_TS_OLD = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
_TS_NEW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
TODAY = date(2025, 1, 15)

_LANE = {"hs_code": "847130", "origin": "VN", "destination": "US"}

_SCHED_CFG = {
    "refresh": {
        "interval_hours": 24,
        "staleness_threshold_days": 7,
        "lanes": [_LANE],
    }
}

_BASE = dict(
    hs_code="847130",
    origin="VN",
    destination="US",
    effective_date=TODAY,
    best_source="US HTS",
    sources_consulted=["US HTS"],
    confidence=0.90,
)


def _rate(mfn_rate: float = 4.5, fetched_at: datetime = _TS_NEW, **kw) -> CanonicalRate:
    return CanonicalRate(**{**_BASE, "mfn_rate": mfn_rate, "fetched_at": fetched_at, **kw})


def _mock_agent(query_return=None, stored=None) -> MagicMock:
    agent = MagicMock()
    agent.store.get.return_value = stored
    agent.query.return_value = query_return
    return agent


def _scheduler(agent=None, cfg=None) -> RateScheduler:
    return RateScheduler(agent or _mock_agent(), config=cfg or _SCHED_CFG)


# ===========================================================================
# _rate_data_changed helper
# ===========================================================================

class TestRateDateChanged:
    def test_old_none_is_changed(self) -> None:
        assert _rate_data_changed(_rate(4.5), None)

    def test_same_mfn_not_changed(self) -> None:
        assert not _rate_data_changed(_rate(4.5), _rate(4.5))

    def test_different_mfn_is_changed(self) -> None:
        assert _rate_data_changed(_rate(4.5), _rate(5.0))

    def test_fetched_at_change_not_counted(self) -> None:
        r1 = _rate(4.5, fetched_at=_TS_NEW)
        r2 = _rate(4.5, fetched_at=_TS_OLD)
        assert not _rate_data_changed(r1, r2)

    def test_new_fta_is_changed(self) -> None:
        old = _rate(4.5)
        new = CanonicalRate(**{**_BASE, "mfn_rate": 4.5, "fetched_at": _TS_NEW,
                                "preferential_rate": 0.0, "applicable_fta": "KORUS"})
        assert _rate_data_changed(new, old)

    def test_new_remedy_is_changed(self) -> None:
        old = _rate(4.5)
        new = CanonicalRate(**{**_BASE, "mfn_rate": 4.5, "fetched_at": _TS_NEW,
                                "active_remedies": ["Section 301 25%"]})
        assert _rate_data_changed(new, old)

    def test_confidence_change_not_counted(self) -> None:
        r1 = CanonicalRate(**{**_BASE, "mfn_rate": 4.5, "fetched_at": _TS_NEW,
                               "confidence": 0.90})
        r2 = CanonicalRate(**{**_BASE, "mfn_rate": 4.5, "fetched_at": _TS_NEW,
                               "confidence": 0.50})
        assert not _rate_data_changed(r1, r2)


# ===========================================================================
# Subscriber callbacks
# ===========================================================================

class TestRateSchedulerCallback:
    def test_callback_called_when_rate_changes(self) -> None:
        old = _rate(4.5, fetched_at=_TS_OLD)
        new = _rate(5.0, fetched_at=_TS_NEW)
        agent = _mock_agent(query_return=new, stored=old)

        sched = _scheduler(agent)
        received: list = []
        sched.subscribe(lambda n, o: received.append((n, o)))
        sched._refresh_lane(_LANE)

        assert len(received) == 1
        assert received[0][0] is new
        assert received[0][1] is old

    def test_no_callback_when_rate_unchanged(self) -> None:
        old = _rate(4.5, fetched_at=_TS_OLD)
        new = _rate(4.5, fetched_at=_TS_NEW)
        agent = _mock_agent(query_return=new, stored=old)

        sched = _scheduler(agent)
        received: list = []
        sched.subscribe(lambda n, o: received.append((n, o)))
        sched._refresh_lane(_LANE)

        assert received == []

    def test_callback_called_for_first_ever_fetch(self) -> None:
        """No stored rate → any new rate is a 'change'."""
        new = _rate(4.5)
        agent = _mock_agent(query_return=new, stored=None)

        sched = _scheduler(agent)
        received: list = []
        sched.subscribe(lambda n, o: received.append((n, o)))
        sched._refresh_lane(_LANE)

        assert len(received) == 1
        assert received[0][1] is None  # old is None

    def test_multiple_callbacks_all_called(self) -> None:
        old = _rate(4.5, fetched_at=_TS_OLD)
        new = _rate(5.0, fetched_at=_TS_NEW)
        agent = _mock_agent(query_return=new, stored=old)

        sched = _scheduler(agent)
        hits: list[int] = []
        sched.subscribe(lambda n, o: hits.append(1))
        sched.subscribe(lambda n, o: hits.append(2))
        sched._refresh_lane(_LANE)

        assert hits == [1, 2]

    def test_callback_exception_does_not_abort_remaining_callbacks(self) -> None:
        new = _rate(5.0)
        agent = _mock_agent(query_return=new, stored=_rate(4.5))

        sched = _scheduler(agent)
        hits: list = []
        sched.subscribe(lambda n, o: (_ for _ in ()).throw(RuntimeError("boom")))
        sched.subscribe(lambda n, o: hits.append(1))
        sched._refresh_lane(_LANE)

        assert hits == [1]


# ===========================================================================
# Staleness marking
# ===========================================================================

class TestRateSchedulerStaleness:
    def test_stale_marked_when_query_fails_and_record_old(self) -> None:
        stale_ts = datetime.now(UTC) - timedelta(days=10)
        old = _rate(4.5, fetched_at=stale_ts, is_stale=False)
        agent = _mock_agent(query_return=None, stored=old)

        sched = _scheduler(agent)
        sched._refresh_lane(_LANE)

        upserted: CanonicalRate = agent.store.upsert.call_args[0][0]
        assert upserted.is_stale is True

    def test_stale_not_marked_when_record_is_fresh(self) -> None:
        fresh_ts = datetime.now(UTC) - timedelta(days=1)
        old = _rate(4.5, fetched_at=fresh_ts, is_stale=False)
        agent = _mock_agent(query_return=None, stored=old)

        sched = _scheduler(agent)
        sched._refresh_lane(_LANE)

        agent.store.upsert.assert_not_called()

    def test_stale_not_reupserted_when_already_stale(self) -> None:
        stale_ts = datetime.now(UTC) - timedelta(days=10)
        old = _rate(4.5, fetched_at=stale_ts, is_stale=True)
        agent = _mock_agent(query_return=None, stored=old)

        sched = _scheduler(agent)
        sched._refresh_lane(_LANE)

        agent.store.upsert.assert_not_called()

    def test_no_stale_action_when_no_stored_rate(self) -> None:
        agent = _mock_agent(query_return=None, stored=None)
        sched = _scheduler(agent)
        sched._refresh_lane(_LANE)
        agent.store.upsert.assert_not_called()

    def test_successful_query_does_not_mark_stale(self) -> None:
        old = _rate(4.5, fetched_at=datetime.now(UTC) - timedelta(days=10))
        new = _rate(4.5)
        agent = _mock_agent(query_return=new, stored=old)

        sched = _scheduler(agent)
        sched._refresh_lane(_LANE)

        # upsert called by query() (mocked), not by scheduler stale logic.
        # The important assertion: upsert was called with is_stale=False (the new rate).
        # Since query is mocked, it doesn't actually call agent.store.upsert.
        # Just verify no stale=True record was pushed by the scheduler.
        for call in agent.store.upsert.call_args_list:
            arg: CanonicalRate = call[0][0]
            assert not arg.is_stale


# ===========================================================================
# _refresh_all
# ===========================================================================

class TestRateSchedulerRefreshAll:
    def test_refresh_all_calls_each_lane(self) -> None:
        cfg = {
            "refresh": {
                "interval_hours": 24,
                "staleness_threshold_days": 7,
                "lanes": [
                    {"hs_code": "847130", "origin": "VN", "destination": "US"},
                    {"hs_code": "040690", "origin": "KR", "destination": "US"},
                ],
            }
        }
        agent = _mock_agent(query_return=None)
        sched = RateScheduler(agent, config=cfg)
        sched._refresh_all()
        assert agent.query.call_count == 2

    def test_refresh_all_continues_after_lane_error(self) -> None:
        """An exception in one lane must not abort the others."""
        cfg = {
            "refresh": {
                "interval_hours": 24,
                "staleness_threshold_days": 7,
                "lanes": [
                    {"hs_code": "847130", "origin": "VN", "destination": "US"},
                    {"hs_code": "040690", "origin": "KR", "destination": "US"},
                ],
            }
        }
        agent = MagicMock()
        agent.store.get.return_value = None
        agent.query.side_effect = [RuntimeError("first lane explodes"), _rate(4.5)]

        sched = RateScheduler(agent, config=cfg)
        received: list = []
        sched.subscribe(lambda n, o: received.append(n))
        sched._refresh_all()

        # Second lane's callback must still have fired.
        assert len(received) == 1
