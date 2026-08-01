"""
Tests for aggregator/connectors/*.

All tests are fully offline — HTTP calls are mocked with unittest.mock.patch.
No live API requests are made in this suite.

Mock fixture notes
------------------
US HTS fixtures: verified against live USITC API on 2025-07-31.
    Real field names: 'general' (MFN), 'special' (FTA programs), 'other' (Col 2).
    Previous fields col1Rate / generalRate do NOT exist in the real API.

WITS fixtures: based on World Bank WITS REST API documentation (SDMX-JSON format).
    NOT verified against live API — all tariff endpoints returned HTTP 403 on
    2025-07-31.  Field names OBS_VALUE and TARIFFTYPE are from WITS documentation.
    Update fixtures when live access is restored.

Test inventory
--------------
TestConnectorError          — typed exception contract
TestBaseConnector           — abstract interface cannot be partially instantiated
TestUSHTSConnector          — name, authority, fetch outcomes, error propagation
TestUSHTSConnectorParsing   — _parse: real field names, FTA special-column, filters
TestUSHTSHelpers            — _parse_rate_str and _parse_special_for_origin directly
TestWITSConnector           — name, authority, fetch outcomes (incl. 403 → ConnectorError)
TestWITSConnectorParsing    — _parse: OBS_VALUE field, TARIFFTYPE filter, edge cases
TestStubConnectors          — eu_taric, macmap, sap_gts raise NotImplementedError
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests

from aggregator.connectors.base import BaseConnector, ConnectorError
from aggregator.connectors.eu_taric import EUTARICConnector
from aggregator.connectors.macmap import MacMapConnector
from aggregator.connectors.sap_gts import SAPGTSConnector
from aggregator.connectors.us_hts import (
    USHTSConnector,
    _parse_rate_str,
    _parse_special_for_origin,
)
from aggregator.connectors.wits import WITSConnector
from aggregator.models import RawRate

TODAY = date(2025, 1, 15)

# Lane used across most tests — VN origin, US destination, laptops HS code
LANE: dict = dict(hs_code="847130", origin="VN", destination="US", effective_date=TODAY)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_us_hts(**kwargs) -> USHTSConnector:
    defaults = dict(
        base_url="https://hts.usitc.gov/reststop",
        timeout_seconds=20,
        authoritative_for=["US"],
    )
    defaults.update(kwargs)
    return USHTSConnector(**defaults)


def _make_wits(**kwargs) -> WITSConnector:
    defaults = dict(
        base_url="https://wits.worldbank.org/API/V1",
        timeout_seconds=30,
        authoritative_for=[],
    )
    defaults.update(kwargs)
    return WITSConnector(**defaults)


def _mock_ok(json_data=None):
    """Mock response for a successful HTTP 200."""
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    resp.json.return_value = json_data if json_data is not None else []
    return resp


def _mock_http_error(status_code: int):
    """Mock response that raises HTTPError on raise_for_status()."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
        f"{status_code} Error", response=resp
    )
    return resp


# ---------------------------------------------------------------------------
# Canonical US HTS response shapes (verified against live API 2025-07-31)
# ---------------------------------------------------------------------------
# Laptop (HS 8471.30.01.00) — MFN Free, no FTA (special is empty)
_US_HTS_LAPTOP = {
    "htsno": "8471.30.01.00",
    "description": "Portable automatic data processing machines...",
    "general": "Free",
    "special": "",
    "other": "35%",
}

# Cheese (HS 0406.90.18.00) — specific MFN duty, rich FTA special column
_US_HTS_CHEESE = {
    "htsno": "0406.90.18.00",
    "description": "Other cheese",
    "general": "$1.803/kg",
    "special": "Free (BH,CL,JO,KR,MA,OM,P,PE,SG) See 9822.04.40 (AU)",
    "other": "$2.121/kg",
}

# Chapter 99 special-provision item that keyword search returns as cross-ref
_US_HTS_CH99 = {
    "htsno": "9903.41.15",
    "description": "Section 301 tariff provision",
    "general": "100%",
    "special": "",
    "other": None,
}

# ---------------------------------------------------------------------------
# Canonical WITS response shapes (based on documentation; NOT live-verified)
# ---------------------------------------------------------------------------
_WITS_MFN_ITEM = {"TARIFFTYPE": "MFN", "REPORTER": "USA", "PARTNER": "VNM",
                  "PRODUCTCODE": "847130", "OBS_VALUE": 4.5, "TOTALNOOFLINES": 8}
_WITS_PREF_ITEM = {"TARIFFTYPE": "PREF", "REPORTER": "USA", "PARTNER": "KOR",
                   "PRODUCTCODE": "847130", "OBS_VALUE": 0.0, "TOTALNOOFLINES": 1}


# ===========================================================================
# ConnectorError
# ===========================================================================

class TestConnectorError:
    def test_stores_source_name(self) -> None:
        err = ConnectorError("US HTS", Exception("refused"))
        assert err.source == "US HTS"

    def test_stores_cause(self) -> None:
        cause = ValueError("bad json")
        err = ConnectorError("WITS", cause)
        assert err.cause is cause

    def test_str_contains_source_name(self) -> None:
        err = ConnectorError("US HTS", Exception("timeout"))
        assert "US HTS" in str(err)

    def test_is_exception_subclass(self) -> None:
        err = ConnectorError("US HTS", Exception("x"))
        assert isinstance(err, Exception)


# ===========================================================================
# BaseConnector — abstract interface
# ===========================================================================

class TestBaseConnector:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            BaseConnector()  # type: ignore[abstract]

    def test_concrete_without_fetch_raises(self) -> None:
        class Incomplete(BaseConnector):
            name = "test"
            def is_authoritative_for(self, destination: str) -> bool:
                return False
        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]

    def test_concrete_without_is_authoritative_raises(self) -> None:
        class Incomplete(BaseConnector):
            name = "test"
            def fetch(self, hs_code, origin, destination, effective_date):
                return []
        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]


# ===========================================================================
# USHTSConnector — name, authority, fetch outcomes
# ===========================================================================

class TestUSHTSConnector:
    def test_name(self) -> None:
        assert _make_us_hts().name == "US HTS"

    def test_authoritative_for_us(self) -> None:
        assert _make_us_hts().is_authoritative_for("US") is True

    def test_authoritative_case_insensitive(self) -> None:
        assert _make_us_hts().is_authoritative_for("us") is True

    def test_not_authoritative_for_eu(self) -> None:
        assert _make_us_hts().is_authoritative_for("EU") is False

    def test_not_authoritative_for_vn(self) -> None:
        assert _make_us_hts().is_authoritative_for("VN") is False

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_fetch_valid_response_returns_raw_rates(self, mock_get) -> None:
        mock_get.return_value = _mock_ok([_US_HTS_LAPTOP])
        results = _make_us_hts().fetch(**LANE)
        assert len(results) == 1
        assert isinstance(results[0], RawRate)
        assert results[0].mfn_rate == 0.0   # "Free" → 0.0
        assert results[0].source == "US HTS"

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_fetch_empty_list_returns_empty(self, mock_get) -> None:
        mock_get.return_value = _mock_ok([])
        assert _make_us_hts().fetch(**LANE) == []

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_fetch_http_error_raises_connector_error(self, mock_get) -> None:
        mock_get.return_value = _mock_http_error(404)
        with pytest.raises(ConnectorError) as exc_info:
            _make_us_hts().fetch(**LANE)
        assert exc_info.value.source == "US HTS"

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_fetch_timeout_raises_connector_error(self, mock_get) -> None:
        mock_get.side_effect = requests.exceptions.Timeout("timed out")
        with pytest.raises(ConnectorError) as exc_info:
            _make_us_hts().fetch(**LANE)
        assert exc_info.value.source == "US HTS"

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_fetch_connection_error_raises_connector_error(self, mock_get) -> None:
        mock_get.side_effect = requests.exceptions.ConnectionError("no route")
        with pytest.raises(ConnectorError) as exc_info:
            _make_us_hts().fetch(**LANE)
        assert exc_info.value.source == "US HTS"

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_connector_error_wraps_original_cause(self, mock_get) -> None:
        original = requests.exceptions.Timeout("deadline exceeded")
        mock_get.side_effect = original
        with pytest.raises(ConnectorError) as exc_info:
            _make_us_hts().fetch(**LANE)
        assert exc_info.value.cause is original

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_invalid_json_raises_connector_error(self, mock_get) -> None:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.side_effect = ValueError("Expecting value")
        mock_get.return_value = resp
        with pytest.raises(ConnectorError) as exc_info:
            _make_us_hts().fetch(**LANE)
        assert exc_info.value.source == "US HTS"

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_raw_rate_lane_fields_match(self, mock_get) -> None:
        mock_get.return_value = _mock_ok([_US_HTS_LAPTOP])
        r = _make_us_hts().fetch(**LANE)[0]
        assert r.hs_code == LANE["hs_code"]
        assert r.origin == LANE["origin"]
        assert r.destination == LANE["destination"]
        assert r.effective_date == LANE["effective_date"]

    @patch("aggregator.connectors.us_hts.requests.get")
    def test_keyword_param_sent_not_origin_destination(self, mock_get) -> None:
        """API only accepts keyword; origin/destination must NOT be sent."""
        mock_get.return_value = _mock_ok([])
        _make_us_hts().fetch(**LANE)
        call_kwargs = mock_get.call_args[1]
        sent_params = call_kwargs.get("params", {})
        assert "keyword" in sent_params
        assert "origin" not in sent_params
        assert "destination" not in sent_params


# ===========================================================================
# USHTSConnector — _parse behaviour with real field shapes
# ===========================================================================

class TestUSHTSConnectorParsing:
    """
    Unit-tests for _parse() using canonical fixture shapes verified against
    the live USITC API.  All items use real field names (general / special / other).
    """

    def _c(self) -> USHTSConnector:
        return _make_us_hts()

    def test_general_free_maps_to_mfn_zero(self) -> None:
        results = self._c()._parse([_US_HTS_LAPTOP], "847130", "VN", "US", TODAY)
        assert results[0].mfn_rate == 0.0

    def test_general_specific_duty_stored_as_expression(self) -> None:
        results = self._c()._parse([_US_HTS_CHEESE], "040690", "VN", "US", TODAY)
        assert results[0].mfn_rate is None
        assert results[0].duty_expression == "$1.803/kg"

    def test_general_percent_parsed_as_float(self) -> None:
        item = {"htsno": "8471.30.01.00", "general": "4.5%", "special": "", "other": "35%"}
        results = self._c()._parse([item], "847130", "VN", "US", TODAY)
        assert results[0].mfn_rate == 4.5

    def test_general_empty_string_item_skipped(self) -> None:
        item = {"htsno": "6110.20.20.10", "general": "", "special": "", "other": ""}
        results = self._c()._parse([item], "611020", "VN", "US", TODAY)
        assert results == []

    def test_chapter99_item_filtered_out(self) -> None:
        """htsno starting with 99xx must be excluded even with a parseable rate."""
        results = self._c()._parse([_US_HTS_CH99], "847130", "VN", "US", TODAY)
        assert results == []

    def test_chapter99_filtered_mixed_with_valid(self) -> None:
        """Valid item survives; Chapter 99 cross-ref is removed."""
        results = self._c()._parse(
            [_US_HTS_LAPTOP, _US_HTS_CH99], "847130", "VN", "US", TODAY
        )
        assert len(results) == 1
        assert results[0].mfn_rate == 0.0

    def test_special_fta_rate_for_kr_origin(self) -> None:
        """KR maps to program code 'KR' → KORUS; 'Free' in special → pref_rate=0.0."""
        results = self._c()._parse([_US_HTS_CHEESE], "040690", "KR", "US", TODAY)
        assert results[0].preferential_rate == 0.0
        assert results[0].applicable_fta == "KORUS"

    def test_special_fta_rate_for_sg_origin(self) -> None:
        results = self._c()._parse([_US_HTS_CHEESE], "040690", "SG", "US", TODAY)
        assert results[0].preferential_rate == 0.0
        assert results[0].applicable_fta == "US-Singapore FTA"

    def test_special_no_fta_for_vn_origin(self) -> None:
        """VN has no bilateral US FTA → no preferential rate extracted."""
        results = self._c()._parse([_US_HTS_CHEESE], "040690", "VN", "US", TODAY)
        assert results[0].preferential_rate is None
        assert results[0].applicable_fta is None

    def test_special_empty_produces_no_preferential_rate(self) -> None:
        results = self._c()._parse([_US_HTS_LAPTOP], "847130", "KR", "US", TODAY)
        assert results[0].preferential_rate is None
        assert results[0].applicable_fta is None

    def test_non_list_response_returns_empty(self) -> None:
        c = self._c()
        assert c._parse({"error": "not found"}, "847130", "VN", "US", TODAY) == []
        assert c._parse(None, "847130", "VN", "US", TODAY) == []

    def test_non_dict_items_skipped(self) -> None:
        results = self._c()._parse(["not_a_dict", None, 42], "847130", "VN", "US", TODAY)
        assert results == []

    def test_multiple_valid_items_all_parsed(self) -> None:
        item1 = {"htsno": "8471.30.01.00", "general": "4.5%", "special": "", "other": "35%"}
        item2 = {"htsno": "8471.30.02.00", "general": "Free", "special": "", "other": "35%"}
        results = self._c()._parse([item1, item2], "847130", "VN", "US", TODAY)
        assert len(results) == 2
        assert results[0].mfn_rate == 4.5
        assert results[1].mfn_rate == 0.0


# ===========================================================================
# USHTSConnector — _parse_rate_str and _parse_special_for_origin helpers
# ===========================================================================

class TestUSHTSHelpers:
    """Targeted tests for the two module-level helper functions."""

    # --- _parse_rate_str ---

    def test_free_returns_zero(self) -> None:
        assert _parse_rate_str("Free") == (0.0, None)

    def test_free_case_insensitive(self) -> None:
        assert _parse_rate_str("FREE") == (0.0, None)
        assert _parse_rate_str("free") == (0.0, None)

    def test_percent_string(self) -> None:
        assert _parse_rate_str("4.5%") == (4.5, None)
        assert _parse_rate_str("100%") == (100.0, None)

    def test_specific_duty(self) -> None:
        assert _parse_rate_str("$1.803/kg") == (None, "$1.803/kg")
        assert _parse_rate_str("6.8c/kg") == (None, "6.8c/kg")

    def test_empty_string_returns_none_none(self) -> None:
        assert _parse_rate_str("") == (None, None)
        assert _parse_rate_str(None) == (None, None)
        assert _parse_rate_str("   ") == (None, None)

    # --- _parse_special_for_origin ---

    def test_kr_in_codes_returns_korus(self) -> None:
        special = "Free (BH,CL,JO,KR,MA,OM,P,PE,SG)"
        rate, expr, fta = _parse_special_for_origin(special, "KR")
        assert rate == 0.0
        assert expr is None
        assert fta == "KORUS"

    def test_sg_in_codes_returns_singapore_fta(self) -> None:
        special = "Free (BH,CL,JO,KR,MA,OM,P,PE,SG)"
        rate, expr, fta = _parse_special_for_origin(special, "SG")
        assert rate == 0.0
        assert fta == "US-Singapore FTA"

    def test_ca_mx_returns_usmca(self) -> None:
        special = "Free (CA,MX,BH,CL)"
        rate_ca, _, fta_ca = _parse_special_for_origin(special, "CA")
        rate_mx, _, fta_mx = _parse_special_for_origin(special, "MX")
        assert rate_ca == 0.0 and fta_ca == "USMCA"
        assert rate_mx == 0.0 and fta_mx == "USMCA"

    def test_vn_not_in_mapping_returns_none(self) -> None:
        special = "Free (BH,CL,JO,KR,MA,OM,P,PE,SG)"
        assert _parse_special_for_origin(special, "VN") == (None, None, None)

    def test_cn_not_in_mapping_returns_none(self) -> None:
        special = "Free (BH,CL,JO,KR)"
        assert _parse_special_for_origin(special, "CN") == (None, None, None)

    def test_origin_in_mapping_but_not_in_this_segment(self) -> None:
        """KR is in the mapping but not in this particular HS code's special column."""
        special = "Free (BH,CL,JO)"   # KR absent
        assert _parse_special_for_origin(special, "KR") == (None, None, None)

    def test_empty_special_returns_none(self) -> None:
        assert _parse_special_for_origin("", "KR") == (None, None, None)
        assert _parse_special_for_origin("   ", "KR") == (None, None, None)

    def test_see_only_special_returns_none(self) -> None:
        """'See HSCODE (AU)' references are skipped in Phase 1."""
        special = "See 9822.04.40 (AU)"
        assert _parse_special_for_origin(special, "AU") == (None, None, None)

    def test_numeric_preferential_rate(self) -> None:
        special = "2.5% (CA,MX,KR)"
        rate, expr, fta = _parse_special_for_origin(special, "KR")
        assert rate == 2.5
        assert expr is None
        assert fta == "KORUS"

    def test_specific_duty_in_special_column(self) -> None:
        special = "$0.50/kg (CA,MX)"
        rate, expr, fta = _parse_special_for_origin(special, "CA")
        assert rate is None
        assert expr == "$0.50/kg"
        assert fta == "USMCA"

    def test_cafta_dr_country_matches_p_code(self) -> None:
        """GT (Guatemala) maps to program code 'P' for CAFTA-DR."""
        special = "Free (BH,CL,JO,KR,MA,OM,P,PE,SG)"
        rate, _, fta = _parse_special_for_origin(special, "GT")
        assert rate == 0.0
        assert fta == "CAFTA-DR"


# ===========================================================================
# WITSConnector — name, authority, fetch outcomes
# ===========================================================================

class TestWITSConnector:
    def test_name(self) -> None:
        assert _make_wits().name == "WITS"

    def test_not_authoritative_for_any_destination(self) -> None:
        c = _make_wits()
        for dest in ("US", "EU", "VN", "DE"):
            assert c.is_authoritative_for(dest) is False

    @patch("aggregator.connectors.wits.requests.get")
    def test_fetch_valid_response_returns_raw_rates(self, mock_get) -> None:
        mock_get.return_value = _mock_ok({"data": [_WITS_MFN_ITEM]})
        results = _make_wits().fetch(**LANE)
        assert len(results) == 1
        assert isinstance(results[0], RawRate)
        assert results[0].mfn_rate == 4.5
        assert results[0].source == "WITS"

    @patch("aggregator.connectors.wits.requests.get")
    def test_fetch_empty_data_returns_empty(self, mock_get) -> None:
        mock_get.return_value = _mock_ok({"data": []})
        assert _make_wits().fetch(**LANE) == []

    @patch("aggregator.connectors.wits.requests.get")
    def test_fetch_403_raises_connector_error(self, mock_get) -> None:
        """The live WITS API returns 403; connector must raise ConnectorError (not []). """
        mock_get.return_value = _mock_http_error(403)
        with pytest.raises(ConnectorError) as exc_info:
            _make_wits().fetch(**LANE)
        assert exc_info.value.source == "WITS"

    @patch("aggregator.connectors.wits.requests.get")
    def test_fetch_http_error_raises_connector_error(self, mock_get) -> None:
        mock_get.return_value = _mock_http_error(500)
        with pytest.raises(ConnectorError) as exc_info:
            _make_wits().fetch(**LANE)
        assert exc_info.value.source == "WITS"

    @patch("aggregator.connectors.wits.requests.get")
    def test_fetch_timeout_raises_connector_error(self, mock_get) -> None:
        mock_get.side_effect = requests.exceptions.Timeout("timed out")
        with pytest.raises(ConnectorError) as exc_info:
            _make_wits().fetch(**LANE)
        assert exc_info.value.source == "WITS"

    @patch("aggregator.connectors.wits.requests.get")
    def test_connector_error_wraps_original_cause(self, mock_get) -> None:
        original = requests.exceptions.ConnectionError("no route")
        mock_get.side_effect = original
        with pytest.raises(ConnectorError) as exc_info:
            _make_wits().fetch(**LANE)
        assert exc_info.value.cause is original

    @patch("aggregator.connectors.wits.requests.get")
    def test_invalid_json_raises_connector_error(self, mock_get) -> None:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.side_effect = ValueError("Expecting value")
        mock_get.return_value = resp
        with pytest.raises(ConnectorError) as exc_info:
            _make_wits().fetch(**LANE)
        assert exc_info.value.source == "WITS"

    @patch("aggregator.connectors.wits.requests.get")
    def test_url_uses_destination_as_reporter(self, mock_get) -> None:
        """WITS convention: destination='US' appears as reporter in the URL."""
        mock_get.return_value = _mock_ok({"data": []})
        _make_wits().fetch(**LANE)
        called_url: str = mock_get.call_args[0][0]
        assert "/US/" in called_url

    @patch("aggregator.connectors.wits.requests.get")
    def test_raw_rate_lane_fields_match(self, mock_get) -> None:
        mock_get.return_value = _mock_ok({"data": [_WITS_MFN_ITEM]})
        r = _make_wits().fetch(**LANE)[0]
        assert r.origin == LANE["origin"]
        assert r.destination == LANE["destination"]
        assert r.effective_date == LANE["effective_date"]


# ===========================================================================
# WITSConnector — _parse behaviour with documented field shapes
# ===========================================================================

class TestWITSConnectorParsing:
    """
    Fixtures use documented WITS SDMX-JSON field names (OBS_VALUE, TARIFFTYPE).
    NOT verified against live API (returns 403). Update when access is restored.
    """

    def _c(self) -> WITSConnector:
        return _make_wits()

    def test_obs_value_read_as_mfn_rate(self) -> None:
        results = self._c()._parse({"data": [_WITS_MFN_ITEM]}, "847130", "VN", "US", TODAY)
        assert results[0].mfn_rate == 4.5

    def test_pref_tarifftype_filtered_out(self) -> None:
        """PREF rows require separate FTA attribution; only MFN is captured."""
        results = self._c()._parse({"data": [_WITS_PREF_ITEM]}, "847130", "KR", "US", TODAY)
        assert results == []

    def test_mixed_mfn_and_pref_only_mfn_returned(self) -> None:
        results = self._c()._parse(
            {"data": [_WITS_MFN_ITEM, _WITS_PREF_ITEM]}, "847130", "VN", "US", TODAY
        )
        assert len(results) == 1
        assert results[0].mfn_rate == 4.5

    def test_null_obs_value_item_skipped(self) -> None:
        item = {"TARIFFTYPE": "MFN", "OBS_VALUE": None}
        results = self._c()._parse({"data": [item]}, "847130", "VN", "US", TODAY)
        assert results == []

    def test_missing_obs_value_item_skipped(self) -> None:
        item = {"TARIFFTYPE": "MFN", "TOTALNOOFLINES": 8}  # no OBS_VALUE
        results = self._c()._parse({"data": [item]}, "847130", "VN", "US", TODAY)
        assert results == []

    def test_non_dict_response_returns_empty(self) -> None:
        c = self._c()
        assert c._parse([], "847130", "VN", "US", TODAY) == []
        assert c._parse(None, "847130", "VN", "US", TODAY) == []

    def test_missing_data_key_returns_empty(self) -> None:
        assert self._c()._parse({}, "847130", "VN", "US", TODAY) == []

    def test_data_not_list_returns_empty(self) -> None:
        assert self._c()._parse({"data": "wrong"}, "847130", "VN", "US", TODAY) == []


# ===========================================================================
# Stub connectors — EU TARIC, MacMap, SAP GTS
# ===========================================================================

class TestStubConnectors:
    def _make(self, cls, authoritative_for=None):
        return cls(base_url="", timeout_seconds=30, authoritative_for=authoritative_for or [])

    def test_eu_taric_name(self) -> None:
        assert self._make(EUTARICConnector, ["EU"]).name == "EU TARIC"

    def test_eu_taric_authoritative_for_eu(self) -> None:
        assert self._make(EUTARICConnector, ["EU"]).is_authoritative_for("EU") is True

    def test_eu_taric_fetch_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self._make(EUTARICConnector, ["EU"]).fetch(**LANE)

    def test_macmap_name(self) -> None:
        assert self._make(MacMapConnector).name == "MacMap"

    def test_macmap_not_authoritative(self) -> None:
        assert self._make(MacMapConnector).is_authoritative_for("US") is False

    def test_macmap_fetch_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self._make(MacMapConnector).fetch(**LANE)

    def test_sap_gts_name(self) -> None:
        assert self._make(SAPGTSConnector).name == "SAP GTS"

    def test_sap_gts_not_authoritative_by_default(self) -> None:
        assert self._make(SAPGTSConnector).is_authoritative_for("US") is False

    def test_sap_gts_fetch_raises_not_implemented(self) -> None:
        with pytest.raises(NotImplementedError):
            self._make(SAPGTSConnector).fetch(**LANE)

    def test_all_stubs_implement_base_connector(self) -> None:
        for cls in (EUTARICConnector, MacMapConnector, SAPGTSConnector):
            assert isinstance(self._make(cls), BaseConnector)
