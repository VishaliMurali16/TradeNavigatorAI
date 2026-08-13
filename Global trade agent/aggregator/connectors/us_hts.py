"""
US HTS connector — USITC HTS public REST endpoint.

Verified against live API on 2025-07-31:
    endpoint:  GET {base_url}/search?keyword={hs_code}
    response:  JSON list of items.  Confirmed field names:
        general  — Column 1 General (MFN) rate: "Free", "4.5%", "$1.803/kg", ""
        special  — FTA / Special Program rates:
                   "Free (BH,CL,JO,KR,MA,OM,P,PE,SG) See 9822.04.40 (AU) ..."
                   Each parenthesised group is program-indicator → country codes.
                   "See HSCODE (CODES)" references quota-schedule provisions
                   and are skipped in Phase 1.
        other    — Column 2 rate (NTR-denied countries); not captured.

    Note: keyword searches also return Chapter 99 special-provision items
    (htsno starting with "99xx") that share a cross-reference to the queried
    HS code but carry separate tariff treatment.  These are filtered out by
    matching the first four digits of htsno against the queried HS prefix.

API parameters:
    The endpoint accepts only a 'keyword' param — no origin or destination.
    'origin' is used POST-retrieval to select the applicable FTA program from
    the 'special' column via the _COUNTRY_TO_PROGRAM lookup table.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

import requests

from aggregator.connectors.base import BaseConnector, ConnectorError
from aggregator.models import RawRate

# ---------------------------------------------------------------------------
# FTA program-indicator lookup
# Maps ISO 3166-1 alpha-2 origin → (program_code_in_special_column, fta_name)
# Only bilateral US FTAs are included; GSP/AGOA/CBERA (A,D,E) are omitted
# because they cover broad country lists and lack clear FTA-name attribution.
# ---------------------------------------------------------------------------
_COUNTRY_TO_PROGRAM: dict[str, tuple[str, str]] = {
    "AU": ("AU", "AUSFTA"),
    "BH": ("BH", "US-Bahrain FTA"),
    "CA": ("CA", "USMCA"),
    "CL": ("CL", "US-Chile FTA"),
    "CO": ("CO", "US-Colombia TPA"),
    "CR": ("P",  "CAFTA-DR"),   # Costa Rica
    "DO": ("P",  "CAFTA-DR"),   # Dominican Republic
    "GT": ("P",  "CAFTA-DR"),   # Guatemala
    "HN": ("P",  "CAFTA-DR"),   # Honduras
    "IL": ("IL", "US-Israel FTA"),
    "JO": ("JO", "US-Jordan FTA"),
    "KR": ("KR", "KORUS"),
    "MA": ("MA", "US-Morocco FTA"),
    "MX": ("MX", "USMCA"),
    "NI": ("P",  "CAFTA-DR"),   # Nicaragua
    "OM": ("OM", "US-Oman FTA"),
    "PA": ("PA", "US-Panama TPA"),
    "PE": ("PE", "US-Peru TPA"),
    "SG": ("SG", "US-Singapore FTA"),
    "SV": ("P",  "CAFTA-DR"),   # El Salvador
}

# Matches the first "rate (codes)" segment in the special column.
# Stops before any "See HSCODE (...)" continuation.
# Examples:
#   "Free (BH,CL,JO,KR)"          → group1="Free"    group2="BH,CL,JO,KR"
#   "$1.50/kg (AU,BH)"             → group1="$1.50/kg" group2="AU,BH"
#   "See 9822.04.40 (AU)"          → no match (starts with "See")
_SPECIAL_MAIN_RE = re.compile(r"^\s*([^(S][^(]*?)\s*\(\s*([^)]+?)\s*\)")


class USHTSConnector(BaseConnector):
    """Fetches MFN tariff rates from the USITC HTS public REST API."""

    name = "US HTS"

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int,
        authoritative_for: list[str],
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
        url = f"{self._base_url}/search"
        # NOTE: keyword is the only supported param — no origin/destination.
        params = {"keyword": hs_code}

        try:
            response = requests.get(url, params=params, timeout=self._timeout)
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
        Parse USITC /search response into RawRate records.

        Field mapping (verified against live API 2025-07-31):
            general → mfn_rate / duty_expression   (Column 1 General / MFN)
            special → preferential_rate / preferential_expression / applicable_fta
                      Origin filters which FTA segment applies.
            other   → Column 2 (NTR-denied countries only; not captured).
            htsno   → used to filter out Chapter 99 special-provision items.
        """
        rates: list[RawRate] = []

        if not isinstance(data, list):
            return rates

        # Derive the 4-digit HS prefix to filter unrelated Chapter 99 items.
        hs_prefix = re.sub(r"[^\d]", "", hs_code)[:4]

        for item in data:
            if not isinstance(item, dict):
                continue

            # Skip Chapter 99 special-provision items (htsno like "9903.41.15")
            # that the keyword search may return as cross-references.
            htsno = item.get("htsno") or ""
            htsno_digits = re.sub(r"[^\d]", "", str(htsno))
            if htsno_digits and not htsno_digits.startswith(hs_prefix):
                continue

            mfn_rate, duty_expression = _parse_rate_str(item.get("general"))
            pref_rate, pref_expr, fta_name = _parse_special_for_origin(
                item.get("special") or "", origin
            )

            if mfn_rate is None and duty_expression is None:
                continue

            rates.append(
                RawRate(
                    hs_code=hs_code,
                    origin=origin,
                    destination=destination,
                    effective_date=effective_date,
                    mfn_rate=mfn_rate,
                    duty_expression=duty_expression,
                    preferential_rate=pref_rate,
                    preferential_expression=pref_expr,
                    applicable_fta=fta_name,
                    source=self.name,
                )
            )

        return rates


# ---------------------------------------------------------------------------
# Module-level helpers (importable for targeted unit tests)
# ---------------------------------------------------------------------------

def _parse_rate_str(rate_str: str | None) -> tuple[float | None, str | None]:
    """
    Parse a raw HTS rate string into (mfn_rate, duty_expression).

    "Free"        → (0.0,  None)
    "4.5%"        → (4.5,  None)
    "100%"        → (100.0, None)
    "$1.803/kg"   → (None, "$1.803/kg")
    "" or None    → (None, None)   no rate data for this item
    """
    if not rate_str or not str(rate_str).strip():
        return None, None
    cleaned = str(rate_str).strip()
    if cleaned.lower() == "free":
        return 0.0, None
    try:
        return float(cleaned.rstrip("%")), None
    except ValueError:
        return None, cleaned


def _parse_special_for_origin(
    special_str: str,
    origin: str,
) -> tuple[float | None, str | None, str | None]:
    """
    Extract origin-specific preferential rate from the HTS 'special' column.

    Returns (preferential_rate, preferential_expression, applicable_fta).
    All three are None when no applicable FTA rate is found for this origin.

    Parsing rules:
    - Only the first "rate (CODES)" segment is parsed.
    - "See HSCODE (CODES)" references (quota-rate cross-references) are
      skipped in Phase 1; they require a secondary HS lookup.
    - Origin is matched by looking up its US program indicator in
      _COUNTRY_TO_PROGRAM.  Origins with no bilateral US FTA (e.g. VN, CN)
      return (None, None, None).
    """
    if not special_str or not special_str.strip():
        return None, None, None

    program_entry = _COUNTRY_TO_PROGRAM.get(origin.upper())
    if program_entry is None:
        return None, None, None

    program_code, fta_name = program_entry

    m = _SPECIAL_MAIN_RE.match(special_str)
    if not m:
        return None, None, None

    rate_str = m.group(1).strip()
    codes = {c.strip() for c in m.group(2).split(",")}

    if program_code not in codes:
        return None, None, None

    if rate_str.lower() == "free":
        return 0.0, None, fta_name
    try:
        return float(rate_str.rstrip("%")), None, fta_name
    except ValueError:
        return None, rate_str, fta_name
