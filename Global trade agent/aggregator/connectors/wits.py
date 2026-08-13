"""
WITS connector — World Bank WITS tariff REST API.

API status (verified 2025-07-31):
    All tariff endpoints at wits.worldbank.org/API/V1/tariff/... return HTTP 403.
    The WITS REST API requires either:
      (a) API credentials / registered account, or
      (b) corporate proxy whitelist for the wits.worldbank.org domain.
    Metadata endpoints (datasource list, reporter list) return HTTP 405 (GET not
    allowed — the API likely expects POST for these).

    CONSEQUENCE: field names and response shape below are based on World Bank WITS
    REST API documentation (SDMX-JSON format), NOT verified against a live response.
    When access is restored, run verify_apis.py and update _parse() accordingly.

Documented response shape (SDMX-JSON, /tariff/.../SUMMARY endpoint):
    {
      "data": [
        {
          "TARIFFTYPE":  "MFN",        # "MFN" | "PREF"
          "REPORTER":    "USA",
          "PARTNER":     "VNM",
          "PRODUCTCODE": "847130",
          "OBS_VALUE":   4.5,          # tariff rate as percentage; null for N/A
          "TOTALNOOFLINES": 8          # number of tariff lines aggregated
        }
      ]
    }

Notes:
    WITS uses the destination as 'reporter' and origin as 'partner'.
    The connector is a global aggregator — not authoritative for any destination.
    Specific duties (duty_expression) are rare in WITS data; it primarily
    reports ad-valorem equivalents.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import requests

from aggregator.connectors.base import BaseConnector, ConnectorError
from aggregator.models import RawRate


class WITSConnector(BaseConnector):
    """
    Fetches MFN tariff data from the World Bank WITS REST API.

    WARNING: API returns HTTP 403 as of 2025-07-31 from corporate networks.
    The connector raises ConnectorError on 403, which the reconciler records
    as a failed source (does not silently return empty).
    """

    name = "WITS"

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int,
        authoritative_for: list[str],  # always empty for WITS; param kept for uniform init
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._authoritative_for = {d.upper() for d in authoritative_for}

    def is_authoritative_for(self, destination: str) -> bool:
        return destination.upper() in self._authoritative_for

    def fetch(
        self,
        hs_code: str,
        origin: str,
        destination: str,
        effective_date: date,
    ) -> list[RawRate]:
        # WITS convention: destination = reporter, origin = tradePartner.
        reporter = destination.upper()
        partner = origin.upper()
        year = effective_date.year
        product = hs_code.replace(".", "")  # WITS expects plain digits

        url = (
            f"{self._base_url}/tariff/country/{reporter}"
            f"/indicator/MFN/year/{year}"
            f"/tradePartner/{partner}"
            f"/product/{product}/SUMMARY"
        )

        try:
            response = requests.get(url, timeout=self._timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            raise ConnectorError(self.name, exc) from exc

        try:
            data = response.json()
        except ValueError as exc:
            raise ConnectorError(self.name, exc) from exc

        return self._parse(data, hs_code, origin, destination, effective_date)

    def _parse(
        self,
        data: Any,
        hs_code: str,
        origin: str,
        destination: str,
        effective_date: date,
    ) -> list[RawRate]:
        """
        Parse WITS tariff summary response into RawRate records.

        Expected shape (documented, unverified against live API — see module docstring):
            {"data": [{"OBS_VALUE": <float|null>, "TARIFFTYPE": "MFN", ...}, ...]}

        Only MFN records are mapped (TARIFFTYPE == "MFN").
        Items with null OBS_VALUE are skipped.
        """
        rates: list[RawRate] = []

        if not isinstance(data, dict):
            return rates

        items = data.get("data")
        if not isinstance(items, list):
            return rates

        for item in items:
            if not isinstance(item, dict):
                continue

            # Filter to MFN rows only; PREF rows require separate FTA attribution.
            tariff_type = str(item.get("TARIFFTYPE") or "").upper()
            if tariff_type and tariff_type != "MFN":
                continue

            mfn_rate: float | None = None
            obs_value = item.get("OBS_VALUE")
            if obs_value is not None:
                try:
                    mfn_rate = float(obs_value)
                except (ValueError, TypeError):
                    pass

            if mfn_rate is None:
                continue

            rates.append(
                RawRate(
                    hs_code=hs_code,
                    origin=origin,
                    destination=destination,
                    effective_date=effective_date,
                    mfn_rate=mfn_rate,
                    source=self.name,
                )
            )

        return rates
