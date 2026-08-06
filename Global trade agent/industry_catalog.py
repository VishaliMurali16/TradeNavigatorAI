"""
industry_catalog.py — single access layer for the industry catalog.

All app code calls get_industries() / get_industry_profile() / classify_shipment().
The underlying source (YAML today, DB later) is invisible to callers.
To promote to a database: replace _load_catalog() only; public API unchanged.
"""
from __future__ import annotations

import yaml
from functools import lru_cache
from pathlib import Path

_CATALOG_PATH = Path(__file__).parent / "industries.yaml"

# Synthetic sentinel representing "no industry filter applied".
# Prepended to the catalog list by get_industries() so the picker always
# shows it as the first / reset option.
_ALL_SENTINEL: dict = {
    "name": "all",
    "display_name": "All Industries",
    "descriptor": "Full cross-industry view — no filter applied",
    "hs_chapters": [],
    "hs_codes": [],
    "representative_lanes": [],
    "tariff_context": "",
    "client_data_filter": {"explicit_tag_field": "", "explicit_tag_value": ""},
    "enabled": True,
}


@lru_cache(maxsize=1)
def _load_catalog() -> list[dict]:
    """Parse industries.yaml once and cache for the process lifetime."""
    with open(_CATALOG_PATH) as f:
        data = yaml.safe_load(f)
    return data.get("industries", [])


def get_industries() -> list[dict]:
    """
    Return all enabled industry entries, with the 'all' sentinel first.
    Returns [sentinel] on any load error so the UI never breaks.
    """
    try:
        catalog = [ind for ind in _load_catalog() if ind.get("enabled", True)]
        return [_ALL_SENTINEL] + catalog
    except Exception:
        return [_ALL_SENTINEL]


def get_industry_profile(name: str) -> dict | None:
    """
    Return the full profile dict for the given name slug, or None if not found.
    Works for "all" (returns the sentinel) and any catalog entry.
    """
    for ind in get_industries():
        if ind["name"] == name:
            return ind
    return None


def default_industry() -> dict:
    """
    Return the 'all' sentinel as the default when no session selection exists.
    The 'all' view matches current app behaviour: no filtering applied.
    """
    return _ALL_SENTINEL


def classify_shipment(record: dict, industry: dict) -> bool:
    """
    Return True if `record` belongs to `industry`.

    Resolution strategy (Adjustment 2):

    1. 'all' lens — always True; the sentinel never filters.

    2. Explicit tag: if the record contains the field named by
       client_data_filter.explicit_tag_field, compare its value
       (case-insensitive) to explicit_tag_value.
         match     → True
         no match  → False  (explicit tag is authoritative; HS fallback skipped)

    3. HS-based fallback (no explicit tag field in record):
       a. Normalise the record's hs_code: strip dots/spaces, take first 6 digits.
       b. Check industry.hs_codes (normalised) — exact 6-digit match → True.
       c. Check industry.hs_chapters — first 2 digits of hs6 in chapter list → True.
       d. No match → False.

    Overlap rule: classify_shipment is called per industry independently.
    If a chapter appears in multiple industries and no hs_code disambiguates,
    this function returns True for all of them — the shipment appears in
    multiple industry views. An exact hs_codes hit in industry A does NOT
    suppress a chapter hit in industry B; they are independent evaluations.
    """
    if industry.get("name") == "all":
        return True

    cdf = industry.get("client_data_filter", {})
    tag_field = cdf.get("explicit_tag_field", "")
    tag_value = cdf.get("explicit_tag_value", "")

    # Step 1 — explicit tag
    if tag_field and tag_field in record:
        return str(record[tag_field]).strip().lower() == str(tag_value).strip().lower()

    # Step 2 — HS-based fallback
    raw_hs = str(record.get("hs_code", ""))
    hs6 = raw_hs.replace(".", "").replace(" ", "")[:6]
    if not hs6:
        return False

    # a. Exact hs_codes match
    industry_hs = [
        h.replace(".", "").replace(" ", "")[:6]
        for h in (industry.get("hs_codes") or [])
    ]
    if hs6 in industry_hs:
        return True

    # b. Chapter match
    try:
        chapter = int(hs6[:2])
    except ValueError:
        return False
    return chapter in (industry.get("hs_chapters") or [])
