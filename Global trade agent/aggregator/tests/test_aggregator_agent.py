"""
Tests for aggregator/aggregator_agent.py.

All tests are fully offline.  US HTS HTTP calls are mocked via
unittest.mock.patch.  WITS is disabled in the test config so its 403
behaviour does not appear in these tests (it is already covered in
test_connectors.py::TestWITSConnector).

Test inventory
--------------
TestAggregatorAgentBuild  — connector instantiation from config
TestAggregatorAgentQuery  — query() happy-path and error handling
TestAggregatorAgentStore  — result is persisted and retrievable
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import requests

import pytest

from aggregator.aggregator_agent import AggregatorAgent
from aggregator.connectors.base import ConnectorError
from aggregator.models import CanonicalRate

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Config with only US HTS enabled — avoids WITS 403 in offline tests.
_CFG = {
    "sources": {
        "us_hts": {
            "enabled": True,
            "base_url": "https://hts.usitc.gov/reststop",
            "timeout_seconds": 20,
            "authoritative_for": ["US"],
        },
        "wits": {
            "enabled": False,
            "base_url": "https://wits.worldbank.org/API/V1",
            "timeout_seconds": 30,
            "authoritative_for": [],
        },
    },
    "precedence": {"us_hts": 90, "wits": 10},
    "confidence": {
        "authoritative_present": 0.90,
        "non_authoritative_agree": 0.75,
        "fallback_only": 0.50,
        "sources_disagree": 0.45,
    },
}

_LANE = dict(hs_code="8471.30", origin="VN", destination="US",
             effective_date=date(2025, 1, 15))

# A minimal valid HTS API response for HS 8471.30 (verified live shape).
_HTS_FREE = [{"htsno": "8471.30.01.00", "general": "Free", "special": "", "other": "35%"}]
_HTS_PERCENT = [{"htsno": "8471.30.01.00", "general": "4.5%", "special": "", "other": "35%"}]


def _mock_hts(response_data):
    """Return a requests.get mock that returns response_data as JSON."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = response_data
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def _agent() -> AggregatorAgent:
    return AggregatorAgent(db_path=":memory:", config=_CFG)


# ===========================================================================
# Connector build
# ===========================================================================

class TestAggregatorAgentBuild:
    def test_only_enabled_connectors_instantiated(self) -> None:
        agent = _agent()
        names = [c.name for c in agent.connectors]
        assert "US HTS" in names
        assert "WITS" not in names  # disabled in _CFG

    def test_unknown_connector_key_skipped_without_crash(self) -> None:
        cfg = {**_CFG, "sources": {**_CFG["sources"], "nonexistent": {"enabled": True}}}
        agent = AggregatorAgent(db_path=":memory:", config=cfg)
        # Should not raise; unknown key is logged and skipped.
        names = [c.name for c in agent.connectors]
        assert "US HTS" in names


# ===========================================================================
# query() — happy path
# ===========================================================================

class TestAggregatorAgentQuery:
    @patch("aggregator.connectors.us_hts.requests.get")
    def test_returns_canonical_rate_on_success(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        agent = _agent()
        rate = agent.query(**_LANE)
        assert isinstance(rate, CanonicalRate)
        assert rate.mfn_rate == 0.0
        assert rate.best_source == "US HTS"

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_confidence_is_authoritative_present(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        rate = _agent().query(**_LANE)
        assert rate is not None
        assert rate.confidence == 0.90

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_sources_consulted_contains_connector(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        rate = _agent().query(**_LANE)
        assert rate is not None
        assert "US HTS" in rate.sources_consulted

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_percent_rate_parsed(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_PERCENT)
        rate = _agent().query(**_LANE)
        assert rate is not None
        assert rate.mfn_rate == 4.5

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_connector_timeout_does_not_crash_query(self, mock_get) -> None:
        mock_get.side_effect = requests.exceptions.Timeout("timed out")
        rate = _agent().query(**_LANE)
        # No data, but no exception raised to caller.
        assert rate is None

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_connector_http_error_does_not_crash_query(self, mock_get) -> None:
        mock_get.side_effect = requests.exceptions.HTTPError("503")
        rate = _agent().query(**_LANE)
        assert rate is None

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_unexpected_exception_does_not_crash_query(self, mock_get) -> None:
        mock_get.side_effect = RuntimeError("unexpected")
        rate = _agent().query(**_LANE)
        assert rate is None

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_empty_response_returns_none(self, mock_get) -> None:
        mock_get.return_value = _mock_hts([])
        rate = _agent().query(**_LANE)
        assert rate is None

    def test_no_connectors_configured_returns_none(self) -> None:
        cfg = {**_CFG, "sources": {}}
        agent = AggregatorAgent(db_path=":memory:", config=cfg)
        rate = agent.query(**_LANE)
        assert rate is None


# ===========================================================================
# Persistence
# ===========================================================================

class TestAggregatorAgentStore:
    @patch("aggregator.connectors.us_hts.requests.get")
    def test_result_upserted_to_store(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        agent = _agent()
        agent.query(**_LANE)
        stored = agent.store.get(**_LANE)
        assert stored is not None
        assert stored.mfn_rate == 0.0

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_none_result_not_upserted(self, mock_get) -> None:
        mock_get.return_value = _mock_hts([])
        agent = _agent()
        agent.query(**_LANE)
        stored = agent.store.get(**_LANE)
        assert stored is None

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_second_query_overwrites_stored(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        agent = _agent()
        agent.query(**_LANE)

        mock_get.return_value = _mock_hts(_HTS_PERCENT)
        agent.query(**_LANE)

        stored = agent.store.get(**_LANE)
        assert stored is not None
        assert stored.mfn_rate == 4.5  # second query's value


# ===========================================================================
# refresh_lanes()
# ===========================================================================

class TestAggregatorAgentRefreshLanes:
    @patch("aggregator.connectors.us_hts.requests.get")
    def test_refresh_lanes_stores_each_lane(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        agent = _agent()
        # Both lanes use the same hs_code so _HTS_FREE's htsno prefix matches both.
        # Different origins create distinct store rows.
        lanes = [
            {"hs_code": "8471.30", "origin": "VN", "destination": "US"},
            {"hs_code": "8471.30", "origin": "CN", "destination": "US"},
        ]
        agent.refresh_lanes(lanes)
        assert mock_get.call_count == 2
        assert agent.store.get("8471.30", "VN", "US", date.today()) is not None
        assert agent.store.get("8471.30", "CN", "US", date.today()) is not None

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_refresh_lanes_continues_after_one_lane_fails(self, mock_get) -> None:
        mock_get.side_effect = [
            requests.exceptions.Timeout("timeout"),  # first lane: connector fails
            _mock_hts(_HTS_FREE),                     # second lane: succeeds
        ]
        agent = _agent()
        lanes = [
            {"hs_code": "8471.30", "origin": "VN", "destination": "US"},
            {"hs_code": "8471.30", "origin": "CN", "destination": "US"},
        ]
        agent.refresh_lanes(lanes)  # must not raise
        # First lane timed out → not stored; second lane must be stored.
        assert agent.store.get("8471.30", "VN", "US", date.today()) is None
        assert agent.store.get("8471.30", "CN", "US", date.today()) is not None

    def test_refresh_lanes_empty_list_is_a_noop(self) -> None:
        _agent().refresh_lanes([])  # no crash, no exception

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_refresh_lanes_uses_today_when_effective_date_absent(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        agent = _agent()
        agent.refresh_lanes([{"hs_code": "8471.30", "origin": "VN", "destination": "US"}])
        assert agent.store.get("8471.30", "VN", "US", date.today()) is not None


# ===========================================================================
# recent_feed()
# ===========================================================================

class TestAggregatorAgentRecentFeed:
    def test_empty_store_returns_empty_list(self) -> None:
        assert _agent().recent_feed(10) == []

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_returns_feed_dicts_with_required_keys(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        agent = _agent()
        agent.query(**_LANE)
        feed = agent.recent_feed(10)
        assert len(feed) == 1
        required = {"id", "timestamp", "time_short", "headline", "detail", "status", "source"}
        assert required == set(feed[0].keys())

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_recent_feed_respects_limit(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        agent = _agent()
        # Three distinct lanes — each becomes a separate store row.
        for origin in ("VN", "KR", "CN"):
            agent.query("8471.30", origin, "US", date(2025, 1, 15))
        feed = agent.recent_feed(2)
        assert len(feed) == 2

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_recent_feed_status_is_cleared_or_issued(self, mock_get) -> None:
        mock_get.return_value = _mock_hts(_HTS_FREE)
        agent = _agent()
        agent.query(**_LANE)
        feed = agent.recent_feed(10)
        assert feed[0]["status"] in ("cleared", "issued")
