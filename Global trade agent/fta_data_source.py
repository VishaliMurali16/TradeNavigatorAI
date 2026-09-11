"""
FTA & Preferential Trade Agent — data-source abstraction layer.

Modes
-----
"simulated"  (default) — synthetic data from fta_simulator.  Works out-of-the-box
                          with no uploads required.
"uploaded"             — data parsed from user-uploaded CSV / XLSX files held
                          in process memory.
"erp"                  (future) — live pull from a connected ERP / TMS system.
                                   Add an _erp_* derivation family below and
                                   register "erp" in each public function's
                                   routing block.  No other file changes needed.

The FTA page (app.py) and all its routes import from THIS module only.
Field provenance data is imported from fta_field_dict (no simulator dependency).

Security / persistence note
---------------------------
Uploaded DataFrames are held in the _state dict (in-process memory) and are
NEVER written to disk.  They persist for the lifetime of the Flask process.
For production:
  • Bind state to a per-session store (e.g. Redis-backed Flask-Session) so
    multi-user deployments cannot see each other's data.
  • Enforce file-size limits and MIME-type validation before calling pd.read_*.
  • Stream large files to object storage (S3 / ADLS) rather than buffering
    the full content in process memory.
  • Apply row-level access control when adding "erp" mode.
"""

from __future__ import annotations

import io
import threading
from typing import Any

import pandas as pd

from fta_field_dict import FIELD_DICTIONARY as _FIELD_DICTIONARY


# ── ERP / SAP → internal snake_case schema boundary ──────────────────────────
# Single translation point: ERP column names are accepted at upload, renamed
# immediately to snake_case in _rename_erp_cols(). Everything downstream uses
# snake_case exclusively — no SAP names permitted past this boundary.
_ERP_SHIPMENT_MAP: dict[str, str] = {
    "TOR_ID":               "shipment_id",
    "TOR_ITEM":             "shipment_item",
    "PRODUCT_ID":           "product_id",
    "PRODUCT_TEXT":         "product",
    "CCNGN":                "hs_code",
    "CTYDP":                "origin",
    "CTYAR":                "destination",
    "CUCOO":                "country_of_origin",
    "CUSVAL":               "value",
    "WAERS":                "currency",
    "AGREEMENT":            "fta_name",
    "MFN_RATE":             "mfn_rate",
    "PREF_RATE":            "preferential_rate",
    "PREF_STATUS":          "claimed_status",
    "RVC_PCT":              "rvc_pct",
    "RVC_THRESHOLD":        "roo_threshold_pct",
    "ROO_STATUS":           "roo_status",
    "SUPPLIER_ID":          "supplier_id",
    "SUPPLIER_NAME":        "supplier_name",
    "BOM_REGIONAL_CONTENT": "bom_regional_content",
    "ENTRY_DATE":           "entry_date",
    "POO_TYPE":             "poo_type",
    "VDECL_REQ_DATE":       "request_date",
    "VDECL_DEADLINE":       "deadline",
    "POO_STATUS":           "status",
    "LANE":                 "lane",
    # ── Non-SAP / consultant-export aliases ───────────────────────────────────
    # Column names found in BI reports, custom ERP extracts, and non-SAP TM
    # exports.  Map to the same snake_case targets as the SAP equivalents above.
    # Text status values (e.g. "Claimed", "Unclaimed") are normalised to E/U/N
    # by _normalise_coded_values() which runs immediately after _rename_erp_cols().
    "shipment_no":       "shipment_id",
    "description":       "product",
    "hs":                "hs_code",
    "from_country":      "origin",
    "to_country":        "destination",
    "declared_value":    "value",
    "trade_deal":        "fta_name",
    "claim_status":      "claimed_status",
    "standard_duty":     "mfn_rate",
    "mfn_duty":          "mfn_rate",
    "preferential_duty": "preferential_rate",
    "pref_duty":         "preferential_rate",
    "regional_content":  "rvc_pct",
    "content_threshold": "roo_threshold_pct",
    "roo_result":        "roo_status",
    "vendor":            "supplier_name",
    "bom_content":       "bom_regional_content",
    "ccy":               "currency",
    "PRODUCT_CATEGORY":  "product_category",   # uppercase ERP / CSV export alias
    "SHIPMENT_STATUS":   "shipment_status",
}

_ERP_COO_MAP: dict[str, str] = {
    "SUPPLIER_NAME":   "supplier_name",
    "SUPPLIER_ID":     "supplier_id",
    "CTYDP":           "origin",
    "CTYAR":           "destination",
    "VDECL_REQ_DATE":  "request_date",
    "VDECL_DEADLINE":  "deadline",
    "POO_STATUS":      "status",
    "POO_TYPE":        "poo_type",
    "DOC_TYPE":        "doc_type",
}

_ERP_BOM_MAP: dict[str, str] = {
    # SAP MM / PP field names
    "MATNR":             "product_id",
    "MAKTX":             "product",
    "CCNGN":             "hs_code",
    "IDNRK":             "component",
    "MENGE":             "component_value",
    "CTYDP":             "component_origin",
    # Neutral / non-SAP aliases
    "PRODUCT_ID":        "product_id",
    "PRODUCT_TEXT":      "product",
    "PRODUCT_NAME":      "product",
    "COMPONENT_ID":      "component",
    "COMPONENT_NAME":    "component",
    "COMPONENT_TEXT":    "component",
    "COMPONENT_VALUE":   "component_value",
    "COMPONENT_ORIGIN":  "component_origin",
    "material_number":   "product_id",
    "material":          "product_id",
    "description":       "product",
    "hs":                "hs_code",
    "value":             "component_value",
    "origin":            "component_origin",
    "country_of_origin": "component_origin",
    "PRODUCT_CATEGORY":  "product_category",   # uppercase ERP / CSV export alias
}

_ERP_ROO_MAP: dict[str, str] = {
    # SAP GTS field names
    "AGREEMENT":         "fta_name",
    "CCNGN_PREFIX":      "hs_code_prefix",
    "ROO_METHOD":        "rule_method",
    "RVC_THRESHOLD":     "rvc_threshold_pct",
    # Neutral / non-SAP aliases
    "FTA_NAME":          "fta_name",
    "HS_CODE_PREFIX":    "hs_code_prefix",
    "RULE_METHOD":       "rule_method",
    "RVC_THRESHOLD_PCT": "rvc_threshold_pct",
    "NOTES":             "notes",
    "NOTE":              "notes",
    "trade_deal":        "fta_name",
    "agreement":         "fta_name",
    "hs_prefix":         "hs_code_prefix",
    "method":            "rule_method",
    "threshold_pct":     "rvc_threshold_pct",
    "threshold":         "rvc_threshold_pct",
}


# ── Column specification (post-rename, snake_case) ────────────────────────────

# Core — required to render lanes, eligibility feed, and all four KPIs.
# origin / destination must be ISO 3166-1 alpha-2 codes (e.g. "KR", "US")
# so the aggregator can look up real tariff rates for each lane.
SHIPMENT_REQUIRED: list[str] = [
    "shipment_id",    # TOR_ID
    "product",        # PRODUCT_TEXT
    "hs_code",        # CCNGN
    "origin",         # CTYDP  — ISO2
    "destination",    # CTYAR  — ISO2
    "value",          # CUSVAL — transaction value in raw USD
    "fta_name",       # AGREEMENT
    "claimed_status", # PREF_STATUS — E | U | N
    "entry_date",     # ENTRY_DATE  — YYYYMMDD or YYYY-MM-DD
]

# Optional rate columns — accepted for cross-reference but NOT used for derivation.
# Tariff rates are sourced exclusively from the live aggregator.
SHIPMENT_RATES: list[str] = ["mfn_rate", "preferential_rate"]

# Optional — enables RoO Compliance Assessment section.
SHIPMENT_ROO: list[str] = [
    "rvc_pct",          # RVC_PCT       — Regional Value Content % actual
    "roo_threshold_pct", # RVC_THRESHOLD — Required RoO threshold %
]

# Optional — enriches Qualification Roadmap with supplier context.
SHIPMENT_ROADMAP: list[str] = [
    "supplier_name",        # SUPPLIER_NAME
    "bom_regional_content", # BOM_REGIONAL_CONTENT
]

# Required columns for the CoO / Proof-of-Origin file (post-rename, snake_case).
COO_REQUIRED: list[str] = [
    "supplier_name",  # SUPPLIER_NAME
    "origin",         # CTYDP  — ISO2
    "destination",    # CTYAR  — ISO2
    "request_date",   # VDECL_REQ_DATE — YYYYMMDD or YYYY-MM-DD
    "deadline",       # VDECL_DEADLINE — YYYYMMDD or YYYY-MM-DD
    "status",         # POO_STATUS — PENDING|RECEIVED|OVERDUE|VALIDATED
]

# Required columns for the Bill of Materials upload (post-rename, snake_case).
BOM_REQUIRED: list[str] = [
    "product_id",        # MATNR — SAP material number / internal product key
    "product",           # MAKTX — product description
    "hs_code",           # CCNGN — HS classification of the finished product
    "component",         # IDNRK — component / ingredient description
    "component_value",   # MENGE — transaction value of this component (USD)
    "component_origin",  # CTYDP — ISO 3166-1 alpha-2 country of origin for the component
]

# Required columns for the Rules-of-Origin rules upload (post-rename, snake_case).
ROO_REQUIRED: list[str] = [
    "fta_name",          # AGREEMENT — FTA name matching shipment upload (e.g. KORUS, USMCA)
    "hs_code_prefix",    # CCNGN_PREFIX — HS chapter/heading/subheading prefix (e.g. "0406" or "84")
    "rule_method",       # ROO_METHOD — RVC, CTC, or specific rule description
    "rvc_threshold_pct", # RVC_THRESHOLD — minimum qualifying RVC percentage
]

# ── Downloadable template CSV ─────────────────────────────────────────────────
# Uses SAP-native column names.  See fta_simulator.FIELD_DICTIONARY for
# provenance marks.  Row values are illustrative benchmark data only.

SHIPMENT_TEMPLATE_CSV = """\
TOR_ID,PRODUCT_TEXT,CCNGN,CTYDP,CTYAR,CUSVAL,AGREEMENT,PREF_STATUS,ENTRY_DATE,MFN_RATE,PREF_RATE,RVC_PCT,RVC_THRESHOLD,SUPPLIER_NAME,BOM_REGIONAL_CONTENT
6100000783,Automotive Parts,8708.29.00,MX,US,245500.00,USMCA,U,20260612,7.500,0.000,68,60,Alpha Automotive MX,68.0
6100000784,Electronic Components,8542.31.00,VN,EU,312800.00,EVFTA,U,20260703,12.000,0.000,72,40,Viet Electronics JSC,72.0
6100000785,Cotton Shirts,6105.10.00,VN,JP,87600.00,RCEP,U,20260520,9.000,2.500,37,40,Hanoi Apparel JSC,37.0
6100000791,PCB Assemblies,8534.00.00,KR,US,445600.00,KORUS,E,20260314,6.500,0.000,78,45,Seoul Electronics Co,78.0
6100000792,Auto Wiring Harness,8544.30.00,MX,US,389200.00,USMCA,E,20260408,7.500,0.000,82,60,Alpha Automotive MX,82.0
6100000797,Rare Earth Magnets,8505.11.00,CN,US,234700.00,N/A,N,20260505,5.000,5.000,22,40,China Magnetics Ltd,22.0
"""

# Field dictionary reference sheet row appended to template download
_FIELD_REF_HEADER = (
    "\n\n# FIELD DICTIONARY REFERENCE — SAP TM + GTS\n"
    "# 🟢 = confirmed real SAP field   🔴 = SAP-style, confirm against client SAP release\n"
    "# SAP_NAME,UNIVERSAL_NAME,PROVENANCE,FORMAT,NOTE\n"
)

def _field_ref_rows() -> str:
    lines = [_FIELD_REF_HEADER]
    for row in _FIELD_DICTIONARY:
        sap, univ, prov, fmt, note = row[0], row[1], row[2], row[3], row[4]
        src = row[5] if len(row) > 5 else ""
        lines.append(f"# {sap},{univ},{prov},{fmt},{note}")
    return "\n".join(lines) + "\n"

# Full template with field dictionary appended (for download)
SHIPMENT_TEMPLATE_CSV_WITH_DICT = SHIPMENT_TEMPLATE_CSV + _field_ref_rows()

COO_TEMPLATE_CSV = """\
SUPPLIER_NAME,CTYDP,CTYAR,VDECL_REQ_DATE,VDECL_DEADLINE,POO_STATUS,POO_TYPE
Seoul Dairy Co,KR,US,20260705,20260807,OVERDUE,EUR.1
Busan Foods Ltd,KR,US,20260712,20260812,PENDING,EUR.1
Hanoi Tech JSC,VN,US,20260718,20260818,RECEIVED,FORM-E
"""

# ── BoM template — snake_case column names (ready to upload without mapping) ─
# product_id must match product_id in the shipment upload for BoM-based RVC to link.
# component_origin must be ISO 3166-1 alpha-2 (e.g. KR, US, CN).
# component_value is the transaction value (any currency, consistent within a product_id).
BOM_TEMPLATE_CSV = """\
product_id,product,hs_code,component,component_value,component_origin
SMP-001,Aged Cheddar,0406.90,Whole Milk (domestic),1600.00,KR
SMP-001,Aged Cheddar,0406.90,Rennet (import),200.00,NZ
SMP-001,Aged Cheddar,0406.90,Bacterial Cultures (import),200.00,NZ
SMP-002,Gouda Cheese Blend,0406.90,Whole Milk (domestic),1200.00,KR
SMP-002,Gouda Cheese Blend,0406.90,Rennet (import),150.00,NZ
SMP-002,Gouda Cheese Blend,0406.90,Starter Cultures (import),150.00,NZ
SMP-004,Laptop Computer,8471.30,CPU and Chipset,380.00,CN
SMP-004,Laptop Computer,8471.30,LCD Screen and Backlight,210.00,CN
SMP-004,Laptop Computer,8471.30,Lithium Battery Pack,90.00,CN
"""

# ── RoO rules template — snake_case column names ──────────────────────────────
# fta_name must match the AGREEMENT/fta_name values in the shipment upload exactly.
# hs_code_prefix is matched as a prefix against the shipment HS code (strips dots and spaces).
# rvc_threshold_pct is the minimum RVC% for preferential qualification.
ROO_TEMPLATE_CSV = """\
fta_name,hs_code_prefix,rule_method,rvc_threshold_pct,notes
KORUS,0406,RVC,45,KORUS dairy — Regional Value Content threshold (HTSUS 0406)
KORUS,8471,RVC,45,KORUS computers — Regional Value Content threshold
USMCA,8708,RVC,60,USMCA auto-parts — tariff preference rule (HTSUS 8708)
EVFTA,8542,RVC,40,EU-Vietnam FTA electronics — RVC requirement (HS 8542)
"""

# ── Generic qualification actions keyed by FTA name ──────────────────────────

_FTA_ACTIONS: dict[str, tuple[str, str]] = {
    "USMCA":        ("Complete USMCA Certificate of Origin filing for unclaimed shipments",
                     "Audit near-miss products to close RVC gap via domestic/regional sourcing"),
    "EVFTA":        ("Submit EVFTA EUR.1 movement certificate or exporter origin declaration",
                     "File retroactive claims for unclaimed shipments within 12-month window"),
    "RCEP":         ("Obtain RCEP Form AK or self-certification for qualifying shipments",
                     "Review BOM for near-miss products — shift non-originating inputs to RCEP members"),
    "KORUS":        ("Renew KORUS Certificate of Origin for expiring supplier agreements",
                     "Verify HS sub-classification alignment with KORUS product-specific rules"),
    "EU-Korea FTA": ("File EUR.1 or Approved Exporter declaration for EU-Korea eligible shipments",
                     "Engage Korean manufacturer on systematic RVC tracking and documentation"),
    "CPTPP":        ("Submit CPTPP self-certification or CO form for qualifying shipments",
                     "Confirm tariff classification change documentation for applicable products"),
    "CAFTA-DR":     ("Complete CAFTA-DR Certificate of Origin for unclaimed shipments",
                     "Identify additional product lines eligible under CAFTA-DR tariff schedule"),
    "US-AUS FTA":   ("Submit US-AUS FTA tariff concession applications",
                     "Identify additional eligible product lines under AUSFTA schedule"),
    "AUSFTA":       ("Submit AUSFTA tariff concession applications",
                     "Identify additional eligible product lines under AUSFTA schedule"),
}
_FTA_ACTIONS_DEFAULT: tuple[str, str] = (
    "File Certificate of Origin and claim preferential tariff rate",
    "Audit product BOM against applicable FTA rules-of-origin requirements",
)

# ── Coded-value normalisation ─────────────────────────────────────────────────

def _map_coded_col(
    df: pd.DataFrame,
    col: str,
    value_map: dict,      # lower-cased key → canonical SAP code
    canonical_set: set,   # already-valid codes — left unchanged
    fallback: str,        # used in warning text only; applied downstream
    valid_desc: str,      # human-readable accepted-values description
) -> "tuple[pd.DataFrame, list[str], list[str]]":
    """
    Normalise values in *col* using *value_map*.
    Values already in *canonical_set* are skipped.
    Returns (df_copy, info_msgs, warning_msgs).

    info_msgs     — successful translations to report to the user (green/info).
    warning_msgs  — values that could NOT be mapped; these will hit the downstream
                    fallback (e.g. 'N' for PREF_STATUS) and affect KPI totals.
    """
    df = df.copy()
    infos:    list[str] = []
    warnings: list[str] = []

    raw = df[col].dropna().astype(str).str.strip()
    unique_raw = raw.unique()

    translations: dict[str, str] = {}   # original cell text → canonical code
    unmapped_vals: list[str] = []

    for v in unique_raw:
        if v.upper() in canonical_set:
            continue                          # already a valid SAP code
        canonical = value_map.get(v.lower())
        if canonical is not None:
            translations[v] = canonical
        else:
            unmapped_vals.append(v)

    if translations:
        def _remap(cell):
            if pd.isna(cell):
                return cell
            s = str(cell).strip()
            return translations.get(s, s)
        df[col] = df[col].apply(_remap)
        trans_str = ", ".join(
            f"'{k}' → {v}" for k, v in sorted(translations.items())
        )
        infos.append(f"Normalised {col}: {trans_str}.")

    if unmapped_vals:
        count = int(
            df[col].astype(str).str.strip().isin(unmapped_vals).sum()
        )
        warnings.append(
            f"{col}: {count} row(s) with unrecognised value(s) "
            f"{sorted(unmapped_vals)!r} — accepted {valid_desc}. "
            f"These rows will default to '{fallback}' and are excluded "
            f"from eligibility KPIs."
        )

    return df, infos, warnings


def _normalise_coded_values(
    df: pd.DataFrame,
) -> "tuple[pd.DataFrame, list[str], list[str]]":
    """
    Normalise all coded SAP fields in a shipment DataFrame in-place (copy):
      PREF_STATUS / claimed_status  →  E / U / N
      ROO_STATUS  / roo_status      →  Q / M / F

    Handles both SAP column names (PREF_STATUS, ROO_STATUS) and their
    snake_case equivalents (claimed_status, roo_status) so that alias-mapped
    uploads are normalised the same way as SAP-native ones.

    Returns (df_normalised, info_msgs, warning_msgs).
    """
    import fta_mapping as _fm   # lazy — avoids any startup circulars

    infos:    list[str] = []
    warnings: list[str] = []

    if "PREF_STATUS" in df.columns:
        df, i, w = _map_coded_col(
            df, "PREF_STATUS",
            _fm.PREF_STATUS_VALUES, {"E", "U", "N"},
            fallback="N",
            valid_desc="E (claimed), U (unclaimed), N (not-eligible)",
        )
        infos.extend(i);    warnings.extend(w)

    # Snake_case variant — present after alias resolution (_rename_erp_cols already ran)
    if "claimed_status" in df.columns:
        df, i, w = _map_coded_col(
            df, "claimed_status",
            _fm.PREF_STATUS_VALUES, {"E", "U", "N"},
            fallback="N",
            valid_desc="E (claimed), U (unclaimed), N (not-eligible)",
        )
        infos.extend(i);    warnings.extend(w)

    if "ROO_STATUS" in df.columns:
        df, i, w = _map_coded_col(
            df, "ROO_STATUS",
            _fm.ROO_STATUS_VALUES, {"Q", "M", "F"},
            fallback="F",
            valid_desc="Q (qualified), M (near-miss), F (fail)",
        )
        infos.extend(i);    warnings.extend(w)

    # Snake_case variant
    if "roo_status" in df.columns:
        df, i, w = _map_coded_col(
            df, "roo_status",
            _fm.ROO_STATUS_VALUES, {"Q", "M", "F"},
            fallback="F",
            valid_desc="Q (qualified), M (near-miss), F (fail)",
        )
        infos.extend(i);    warnings.extend(w)

    return df, infos, warnings


def _normalise_coo_coded_values(
    df: pd.DataFrame,
) -> "tuple[pd.DataFrame, list[str], list[str]]":
    """
    Normalise coded SAP fields in a CoO DataFrame in-place (copy):
      POO_STATUS   PENDING / RECEIVED / OVERDUE / VALIDATED

    Returns (df_normalised, info_msgs, warning_msgs).
    """
    import fta_mapping as _fm

    infos:    list[str] = []
    warnings: list[str] = []

    if "POO_STATUS" in df.columns:
        df, i, w = _map_coded_col(
            df, "POO_STATUS",
            _fm.POO_STATUS_VALUES,
            {"PENDING", "RECEIVED", "OVERDUE", "VALIDATED"},
            fallback="PENDING",
            valid_desc="PENDING, RECEIVED, OVERDUE, VALIDATED",
        )
        infos.extend(i);    warnings.extend(w)

    return df, infos, warnings


# ── Aggregator enrichment ─────────────────────────────────────────────────────

# Last set of (fta_name, origin, destination) → {mfn_rate_pct, preferential_rate_pct}
# for lanes that received a full update from the aggregator. Written atomically by
# _enrich_lanes_from_aggregator(); read by get_fta_shipments() to re-derive est_saving_k
# from the same rates the lane table displays (FIX 1: all derived figures move together).
# CPython dict assignment is atomic at the bytecode level, so no lock is needed here.
_enriched_rates: dict = {}


def _get_aggregator():
    """Return app._aggregator at call time, avoiding a circular import at module load."""
    try:
        import app as _app          # noqa: PLC0415
        return getattr(_app, "_aggregator", None)
    except Exception:
        return None


def _enrich_lanes_from_aggregator(lanes: list[dict]) -> list[dict]:
    """
    Overlay real tariff rates from the aggregator onto simulated FTA lanes.

    Each lane carries a ``representative_lane`` dict with ``hs_code``, ``origin``
    (ISO-3166 alpha-2), and ``destination`` (ISO-3166 alpha-2).  One aggregator
    query is issued per lane using that representative HS code.

    FIX-2 — full-or-nothing rule (honesty):
      - aggregator returns CanonicalRate with BOTH mfn_rate and preferential_rate
        → use both; re-derive all rate-dependent figures; rates_source="aggregator"
      - aggregator returns CanonicalRate with mfn_rate but preferential_rate=None
        → show real MFN; mark pref as None (never fabricate a rate);
           savings cannot be computed; rates_source="aggregator_mfn_only"
      - aggregator returns None (no connector data for this lane)
        → both rates stay None; rates_source="no_aggregator_data"

    FIX-3 — representative-rate limitation (honesty):
      Each lane is queried via a SINGLE representative HS code. For simulator lanes
      this is curated in fta_simulator.py; for uploaded lanes it is the most-common
      HS code by shipment count within that lane group (see _derive_lanes()).
      For lanes spanning multiple HS codes with different MFN rates the returned
      rate is representative-only, not a weighted average across the full product mix.
      Do not interpret the enriched lane MFN as exact for every shipment on the lane.
    """
    from datetime import date as _date
    global _enriched_rates

    agg = _get_aggregator()
    new_enriched: dict = {}
    result: list[dict] = []

    for lane in lanes:
        rep    = lane.get("representative_lane", {})
        hs6    = rep.get("hs_code", "")
        iso2_o = rep.get("origin", "")
        iso2_d = rep.get("destination", "")

        lane = dict(lane)   # shallow copy — never mutate the cached simulator list

        if not (hs6 and iso2_o and iso2_d) or agg is None:
            lane["rates_source"] = "no_aggregator_data"
            result.append(lane)
            continue

        try:
            # Read from the on-disk store first (no live HTTP on page load).
            # Fall back to a live connector query only when the lane isn't cached.
            canonical = agg._store.get(hs6, iso2_o, iso2_d, _date.today())
            if canonical is None:
                canonical = agg.query(hs6, iso2_o, iso2_d, _date.today())
        except Exception:
            canonical = None

        if canonical is None:
            lane["rates_source"] = "no_aggregator_data"

        elif canonical.mfn_rate is not None and canonical.preferential_rate is not None:
            # Full aggregator data — update rates and re-derive ALL rate-dependent figures
            # from the same updated values so every number on screen reconciles (FIX 1).
            lane["mfn_rate_pct"]          = canonical.mfn_rate
            lane["preferential_rate_pct"] = canonical.preferential_rate
            if canonical.applicable_fta:
                lane["fta_name"] = canonical.applicable_fta

            elig = lane["eligible_value_m"]
            clmd = lane["claimed_value_m"]
            lane["unclaimed_savings_k"] = round(
                (elig - clmd) * 1000
                * (lane["mfn_rate_pct"] - lane["preferential_rate_pct"]) / 100,
                1,
            )
            lane["retro_k"] = round(
                lane["unclaimed_savings_k"] * lane.get("retro_eligible_pct", 0) / 100,
                1,
            )
            lane["rates_source"] = "aggregator"

            # Cache for get_fta_shipments() — keyed by (fta_name, origin, destination)
            # using the display-format values so the shipment lookup matches directly.
            new_enriched[(lane["fta_name"], lane["origin"], lane["destination"])] = {
                "mfn_rate_pct":          lane["mfn_rate_pct"],
                "preferential_rate_pct": lane["preferential_rate_pct"],
            }

        elif canonical.mfn_rate is not None:
            # Partial aggregator data: MFN known, preferential_rate=None.
            # FIX-2 (simulator-removed context): show the real MFN but mark pref as
            # unavailable — do not fabricate a preferential rate. Savings cannot be
            # computed without pref, so unclaimed_savings_k and retro_k stay None.
            lane["mfn_rate_pct"]          = canonical.mfn_rate
            lane["preferential_rate_pct"] = None
            lane["unclaimed_savings_k"]   = None
            lane["retro_k"]               = None
            lane["rates_source"]          = "aggregator_mfn_only"

        else:
            lane["rates_source"] = "no_aggregator_data"

        result.append(lane)

    _enriched_rates = new_enriched   # atomic CPython assignment
    return result


# ── FTA member country sets for BoM-based RVC computation ────────────────────
# ISO 3166-1 alpha-2 codes.  Add new FTAs here to enable computation.
# FTAs absent from this dict → "cannot compute — FTA members not encoded for <name>".

_EU_MEMBERS: frozenset = frozenset({
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR",
    "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL",
    "PT", "RO", "SE", "SI", "SK",
})

_FTA_MEMBER_SETS: dict[str, frozenset] = {
    "KORUS":  frozenset({"US", "KR"}),
    "USMCA":  frozenset({"US", "MX", "CA"}),
    "EVFTA":  frozenset({"VN"}) | _EU_MEMBERS,
}

# ── Thread-safe in-memory state ───────────────────────────────────────────────

_lock = threading.Lock()
_state: dict[str, Any] = {
    # "empty" | "uploaded"  — tracked independently per data type
    "shipment_mode":        "empty",
    "coo_mode":             "empty",
    "bom_mode":             "empty",
    "roo_mode":             "empty",
    # Normalised pandas DataFrames (None when in simulated mode)
    "shipment_df":          None,
    "coo_df":               None,
    "bom_df":               None,
    "roo_df":               None,
    # Original filenames for UI display
    "shipment_filename":    None,
    "coo_filename":         None,
    "bom_filename":         None,
    "roo_filename":         None,
    # Which optional column groups were present in the last upload
    "shipment_has_roo":     False,
    "shipment_has_roadmap": False,
    # Last upload result — read-once by the page on next GET
    "upload_shipment_msg":  None,   # {"ok": bool, "errors": [...], "warnings": [...]}
    "upload_coo_msg":       None,
    "upload_bom_msg":       None,
    "upload_roo_msg":       None,
}

# ── Internal helpers ──────────────────────────────────────────────────────────

def _ro_status(rvc: float, threshold: float) -> str:
    """Canonical ROO_STATUS rule — identical to fta_simulator._derive_roo_status.
    Returns SAP-style codes: Q=Qualified  M=Near-Miss  F=Fail
    """
    if rvc >= threshold:
        return "Q"
    if rvc >= threshold - 5:
        return "M"
    return "F"


def _normalise_dats(raw) -> str:
    """Accept YYYYMMDD (SAP DATS) or YYYY-MM-DD; return YYYYMMDD for internal use."""
    s = str(raw).strip().replace("-", "")
    return s if len(s) == 8 and s.isdigit() else "00000000"


def _parse_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Deserialise CSV or XLSX bytes into a DataFrame.  Raises ValueError on bad format."""
    fn = filename.lower()
    # Preserve leading zeros for HS codes / prefixes across all upload types.
    _hs_str = {
        "CCNGN":          str,
        "hs_code":        str,
        "CCNGN_PREFIX":   str,
        "hs_code_prefix": str,
    }
    if fn.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes), dtype=_hs_str)
    if fn.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(file_bytes), dtype=_hs_str)
    raise ValueError(f"Unsupported file type '{filename}'. Upload a .csv or .xlsx file.")


def _normalise_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from column names and replace spaces with underscores."""
    df = df.copy()
    df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]
    return df


def _rename_erp_cols(df: pd.DataFrame, erp_map: dict[str, str]) -> pd.DataFrame:
    """Rename ERP/SAP column names to internal snake_case at the ingestion boundary.
    Matching is case-insensitive so PRODUCT_CATEGORY, Product_Category, and
    product_category all resolve to the same target.
    Columns not in the map (already snake_case) pass through unchanged."""
    upper_map = {k.upper(): v for k, v in erp_map.items()}
    rename = {col: upper_map[col.upper()] for col in df.columns if col.upper() in upper_map}
    return df.rename(columns=rename)


def _validate_shipment(
    df: pd.DataFrame,
) -> tuple[bool, list[str], list[str], bool, bool]:
    """
    Validate a normalised shipment DataFrame (SAP-native column names).

    Returns (ok, errors, warnings, has_roo, has_roadmap).
    ok is False if any required column is absent or the file is empty.
    """
    errors:   list[str] = []
    warnings: list[str] = []

    if df.empty:
        errors.append("File contains no data rows.")
        return False, errors, warnings, False, False

    missing_req = [c for c in SHIPMENT_REQUIRED if c not in df.columns]
    if missing_req:
        errors.append(f"Missing required columns: {', '.join(missing_req)}")

    if "claimed_status" in df.columns:
        valid_ps = {"E", "U", "N"}
        bad_ps = set(
            df["claimed_status"].dropna().astype(str).str.strip().str.upper().unique()
        ) - valid_ps
        if bad_ps:
            count_bad = int(
                (~df["claimed_status"].astype(str).str.strip().str.upper()
                   .isin(valid_ps) & df["claimed_status"].notna()).sum()
            )
            warnings.append(
                f"claimed_status: {count_bad} row(s) have unrecognised "
                f"values {sorted(bad_ps)!r} — "
                "accepted E (claimed), U (unclaimed), N (not-eligible). "
                "These rows are treated as N (not eligible) and excluded "
                "from eligibility KPIs."
            )

    has_roo     = all(c in df.columns for c in SHIPMENT_ROO)
    has_roadmap = all(c in df.columns for c in SHIPMENT_ROADMAP)

    if not has_roo:
        miss = [c for c in SHIPMENT_ROO if c not in df.columns]
        warnings.append(
            f"Optional RoO columns absent ({', '.join(miss)}) — "
            "RoO Compliance Assessment section will show a placeholder."
        )
    if not has_roadmap:
        miss = [c for c in SHIPMENT_ROADMAP if c not in df.columns]
        warnings.append(
            f"Optional sourcing columns absent ({', '.join(miss)}) — "
            "Qualification Roadmap will use generic action text."
        )

    return len(errors) == 0, errors, warnings, has_roo, has_roadmap


def _validate_coo(df: pd.DataFrame) -> tuple[bool, list[str], list[str]]:
    """Validate a normalised (snake_case) CoO DataFrame.
    Returns (ok, errors, warnings).
    """
    errors:   list[str] = []
    warnings: list[str] = []

    if df.empty:
        errors.append("File contains no data rows.")
        return False, errors, warnings

    missing = [c for c in COO_REQUIRED if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")

    if "status" in df.columns:
        valid_s = {"PENDING", "RECEIVED", "OVERDUE", "VALIDATED"}
        bad_s = set(
            df["status"].dropna().astype(str).str.strip().str.upper().unique()
        ) - valid_s
        if bad_s:
            count_bad = int(
                (~df["status"].astype(str).str.strip().str.upper()
                   .isin(valid_s) & df["status"].notna()).sum()
            )
            warnings.append(
                f"status: {count_bad} row(s) have unrecognised "
                f"values {sorted(bad_s)!r} — "
                "accepted PENDING, RECEIVED, OVERDUE, VALIDATED. "
                "These rows are treated as PENDING."
            )

    return len(errors) == 0, errors, warnings


def _validate_bom(
    df: pd.DataFrame,
) -> "tuple[bool, list[str], list[str]]":
    """Validate a normalised (snake_case) BoM DataFrame.
    Returns (ok, errors, warnings).
    """
    errors:   list[str] = []
    warnings: list[str] = []

    if df.empty:
        errors.append("File contains no data rows.")
        return False, errors, warnings

    missing = [c for c in BOM_REQUIRED if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
        return False, errors, warnings

    # Warn about blank component_value or component_origin rows (per-product detail)
    val_blank = int(
        df["component_value"].isna().sum()
        + (~df["component_value"].isna()
           & (df["component_value"].astype(str).str.strip() == "")).sum()
    )
    ori_blank = int(
        df["component_origin"].isna().sum()
        + (~df["component_origin"].isna()
           & (df["component_origin"].astype(str).str.strip() == "")).sum()
    )
    if val_blank:
        warnings.append(
            f"{val_blank} row(s) have blank component_value — "
            "those products will show 'RVC incomplete — qualification cannot be confirmed'."
        )
    if ori_blank:
        warnings.append(
            f"{ori_blank} row(s) have blank component_origin — "
            "those products will show 'RVC incomplete — qualification cannot be confirmed'."
        )

    return True, errors, warnings


def _validate_roo(
    df: pd.DataFrame,
) -> "tuple[bool, list[str], list[str]]":
    """Validate a normalised (snake_case) RoO rules DataFrame.
    Returns (ok, errors, warnings).
    """
    errors:   list[str] = []
    warnings: list[str] = []

    if df.empty:
        errors.append("File contains no data rows.")
        return False, errors, warnings

    missing = [c for c in ROO_REQUIRED if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
        return False, errors, warnings

    non_numeric = pd.to_numeric(df["rvc_threshold_pct"], errors="coerce").isna()
    if non_numeric.any():
        bad = df.loc[non_numeric, "rvc_threshold_pct"].tolist()[:3]
        warnings.append(
            f"rvc_threshold_pct has non-numeric values {bad!r} — "
            "affected rules will be skipped during RVC qualification."
        )

    return True, errors, warnings


# ── Data derivation: uploaded DataFrame → internal dicts ─────────────────────

def _derive_shipments(
    df: pd.DataFrame,
    has_roo: bool,
    bom_df: "pd.DataFrame | None" = None,
    roo_df: "pd.DataFrame | None" = None,
) -> list[dict]:
    """Map normalised (snake_case) shipment DataFrame to internal dict schema."""
    result: list[dict] = []
    _elig_map = {"E": "eligible-claimed", "U": "eligible-unclaimed", "N": "not-eligible"}

    for _, row in df.iterrows():
        raw_ps = str(row.get("claimed_status", "N")).strip().upper()
        pref_s = raw_ps if raw_ps in ("E", "U", "N") else "N"

        value_usd = float(row.get("value", 0) or 0)
        value_k   = round(value_usd / 1_000, 1)

        mfn_raw  = row.get("mfn_rate")
        pref_raw = row.get("preferential_rate")
        mfn_val  = float(mfn_raw)  if mfn_raw  is not None and pd.notna(mfn_raw)  else None
        pref_val = float(pref_raw) if pref_raw is not None and pd.notna(pref_raw) else None
        duty_saving = (
            round(max(0.0, value_usd * (mfn_val - pref_val) / 100), 2)
            if mfn_val is not None and pref_val is not None else None
        )

        # Determine product_id for BoM lookup
        _pid = ""
        for _col in ("product_id", "shipment_id"):
            _v = str(row.get(_col, "")).strip()
            if _v and _v not in {"—", "nan"}:
                _pid = _v
                break
        # Column fallback values — only when BoM/rules not uploaded
        _rvc_col = (
            float(row["rvc_pct"])
            if bom_df is None and has_roo and pd.notna(row.get("rvc_pct"))
            else None
        )
        _thr_col = (
            float(row["roo_threshold_pct"])
            if roo_df is None and has_roo and pd.notna(row.get("roo_threshold_pct"))
            else None
        )
        resolved = _resolve_roo(
            product_id=_pid,
            fta_name=str(row.get("fta_name", "N/A")),
            hs_code=str(row.get("hs_code", "")),
            bom_df=bom_df,
            roo_df=roo_df,
            rvc_col=_rvc_col,
            thr_col=_thr_col,
        )
        rvc = resolved["rvc_pct"]
        thr = resolved["roo_threshold_pct"]
        roo = resolved["roo_status"]

        raw_date   = row.get("entry_date", "")
        entry_date = _normalise_dats(raw_date) if pd.notna(raw_date) and raw_date != "" else "00000000"

        result.append({
            "shipment_id":          str(row.get("shipment_id",   "—")),
            "shipment_item":        str(row.get("shipment_item", "000010")),
            "product_id":           str(row.get("product_id",    "—")),
            "product":              str(row.get("product",       "Unknown")),
            "hs_code":              str(row.get("hs_code",       "—")),
            "origin":               str(row.get("origin",        "—")),
            "destination":          str(row.get("destination",   "—")),
            "country_of_origin":    str(row.get("country_of_origin", row.get("origin", "—"))),
            "value":                value_usd,
            "value_k":              value_k,
            "currency":             str(row.get("currency", "USD")),
            "fta_name":             str(row.get("fta_name", "N/A")),
            "mfn_rate":             mfn_val,
            "preferential_rate":    pref_val,
            "claimed_status":       pref_s,
            "eligibility":          _elig_map.get(pref_s, "not-eligible"),
            "rvc_pct":              rvc,          # float | None
            "roo_threshold_pct":    thr,          # float | None
            "roo_status":           roo,          # Q/M/F | None
            "supplier_id":          str(row.get("supplier_id",          "—")),
            "supplier_name":        str(row.get("supplier_name",        "—")),
            "bom_regional_content": float(row.get("bom_regional_content", 0) or 0),
            "entry_date":           entry_date,
            "est_saving_k":         None,  # populated by get_fta_shipments() after enrichment
            "duty_saving":          duty_saving,
            "product_category":     str(row.get("product_category", "")).strip(),
            "shipment_status":      str(row.get("shipment_status",  "")).strip(),
        })

    return result


def _derive_lanes(df: pd.DataFrame) -> list[dict]:
    """
    Aggregate uploaded shipment rows into trade lanes.

    Lane key: (fta_name, origin, destination).
    claimed_status=N excluded from eligible/claimed totals.

    Rates (mfn_rate_pct, preferential_rate_pct, unclaimed_savings_k, retro_k) are
    set to None here and populated by _enrich_lanes_from_aggregator() after this
    function returns. Sorting also happens in get_fta_lanes() after enrichment.
    eligible_value_m = Σ value for claimed_status in (E, U)
    claimed_value_m  = Σ value for claimed_status = E
    """
    eligible_df = df[
        df["claimed_status"].str.strip().str.upper().isin(["E", "U"])
    ].copy()

    lanes: list[dict] = []
    counter = 1

    for (fta, ctydp, ctyar), grp in eligible_df.groupby(
        ["fta_name", "origin", "destination"], sort=False,
    ):
        total_val   = grp["value"].sum()
        claimed_val = grp.loc[
            grp["claimed_status"].str.strip().str.upper() == "E", "value"
        ].sum()

        util_pct = round(claimed_val / total_val * 100, 1) if total_val else 0.0

        # Representative HS for aggregator query: most-common HS code by shipment count
        # on this lane. FIX-3: single-HS limitation — if the lane spans multiple HS codes
        # with different MFN rates, the returned rate is representative-only.
        rep_hs = ""
        if "hs_code" in grp.columns:
            hs_counts = grp["hs_code"].dropna().value_counts()
            if not hs_counts.empty:
                rep_hs = str(hs_counts.index[0]).replace(".", "").replace(" ", "")[:6]

        lanes.append({
            "lane_id":               f"UL-{counter:03d}",
            "origin":                str(ctydp),
            "destination":           str(ctyar),
            "fta_name":              str(fta),
            "eligible_value_m":      round(total_val   / 1_000_000, 3),
            "claimed_value_m":       round(claimed_val / 1_000_000, 3),
            "mfn_rate_pct":          None,  # populated by _enrich_lanes_from_aggregator
            "preferential_rate_pct": None,  # populated by _enrich_lanes_from_aggregator
            "utilization_pct":       util_pct,
            "unclaimed_savings_k":   None,  # populated by _enrich_lanes_from_aggregator
            "retro_eligible_pct":    0,     # not derivable from upload
            "retro_k":               None,  # populated by _enrich_lanes_from_aggregator
            "rates_source":          "pending",
            "representative_lane":   {
                "hs_code":     rep_hs,
                "origin":      str(ctydp),
                "destination": str(ctyar),
            },
        })
        counter += 1

    return lanes


def _derive_roo_assessments(df: pd.DataFrame) -> list[dict]:
    """
    Aggregate RoO status per (product, hs_code, fta_name) from uploaded data.
    Uses value-weighted average rvc_pct; roo_status via _ro_status().
    """
    result: list[dict] = []

    for (product, hs, fta), grp in df.groupby(
        ["product", "hs_code", "fta_name"], sort=False
    ):
        valid = grp[grp["rvc_pct"].notna() & grp["roo_threshold_pct"].notna()]
        if valid.empty:
            continue

        v_sum   = valid["value"].sum()
        avg_rvc = round(
            (valid["value"] * valid["rvc_pct"]).sum() / v_sum
            if v_sum else valid["rvc_pct"].mean(),
            1,
        )
        threshold = float(valid["roo_threshold_pct"].mode().iloc[0])
        status    = _ro_status(avg_rvc, threshold)
        gap_pct   = round(max(0.0, threshold - avg_rvc), 1)

        if status == "Q":
            note = "Passes RVC threshold — maintain CoO documentation."
        elif status == "M":
            note = (
                f"Within {gap_pct}% of the {threshold}% RVC threshold — "
                "minor BOM adjustments may qualify this product."
            )
        else:
            note = (
                f"RVC gap of {gap_pct}% below the {threshold}% threshold — "
                "significant sourcing changes required."
            )

        _grp_cat = (
            str(grp["product_category"].dropna().iloc[0]).strip()
            if "product_category" in grp.columns and not grp["product_category"].dropna().empty
            else ""
        )
        result.append({
            "product":           str(product),
            "hs_code":           str(hs),
            "fta_name":          str(fta),
            "product_category":  _grp_cat,
            "roo_test_type":     "Regional Value Content",
            "rvc_pct":           avg_rvc,
            "roo_threshold_pct": threshold,
            "roo_status":        status,
            "gap_pct":           gap_pct,
            "compliance_note":   note,
        })

    return result


def _compute_bom_rvc(
    bom_df: pd.DataFrame,
    product_id: str,
    fta_name: str,
) -> dict:
    """
    Compute RVC% for one product_id from the BoM DataFrame.

    Completeness guard: if ANY component_value or component_origin is blank
    for this product, refuses to compute — a wrong 'Qualifies' is worse than
    'incomplete'.

    Returns one of:
      {"ok": True,  "rvc_pct": float, "rvc_source": "computed_from_bom"}
      {"ok": False, "rvc_pct": None,  "rvc_source": "computed_from_bom", "reason": str}
    """
    rows = bom_df[
        bom_df["product_id"].astype(str).str.strip() == str(product_id).strip()
    ]

    if rows.empty:
        return {
            "ok": False, "rvc_pct": None, "rvc_source": "computed_from_bom",
            "reason": f"product_id '{product_id}' not found in BoM",
        }

    # Completeness guard: any blank value or origin → refuse to compute
    val_blank = rows["component_value"].isna() | (
        rows["component_value"].astype(str).str.strip() == ""
    )
    ori_blank = rows["component_origin"].isna() | (
        rows["component_origin"].astype(str).str.strip() == ""
    )
    if val_blank.any() or ori_blank.any():
        return {
            "ok": False, "rvc_pct": None, "rvc_source": "computed_from_bom",
            "reason": "RVC incomplete — qualification cannot be confirmed",
        }

    member_set = _FTA_MEMBER_SETS.get(str(fta_name).strip())
    if member_set is None:
        return {
            "ok": False, "rvc_pct": None, "rvc_source": "computed_from_bom",
            "reason": f"cannot compute — FTA members not encoded for {fta_name}",
        }

    try:
        vals = rows["component_value"].astype(float)
    except Exception:
        return {
            "ok": False, "rvc_pct": None, "rvc_source": "computed_from_bom",
            "reason": "RVC incomplete — component_value contains non-numeric data",
        }

    total = vals.sum()
    if total == 0:
        return {
            "ok": False, "rvc_pct": None, "rvc_source": "computed_from_bom",
            "reason": "RVC incomplete — total component value is zero",
        }

    origins    = rows["component_origin"].astype(str).str.strip().str.upper()
    qualifying = vals[origins.isin(member_set)].sum()
    rvc_pct    = round(float(qualifying) / float(total) * 100, 1)

    return {
        "ok": True, "rvc_pct": rvc_pct, "rvc_source": "computed_from_bom", "reason": None,
    }


def _lookup_roo_threshold(
    roo_df: pd.DataFrame,
    fta_name: str,
    hs_code: str,
) -> dict:
    """
    Find the best-matching RoO rule for (fta_name, hs_code).
    Matches by fta_name (exact, case-insensitive) and longest hs_code_prefix
    that is a prefix of hs_code (dots and spaces stripped before comparison).

    Returns one of:
      {"ok": True,  "rvc_threshold_pct": float, "rule_method": str, "threshold_source": "uploaded_roo_rules"}
      {"ok": False, "rvc_threshold_pct": None,  "threshold_source": "no_rule", "reason": str}
    """
    fta_rows = roo_df[
        roo_df["fta_name"].astype(str).str.strip().str.upper()
        == str(fta_name).strip().upper()
    ]

    if fta_rows.empty:
        return {
            "ok": False, "rvc_threshold_pct": None,
            "threshold_source": "no_rule",
            "reason": f"no RoO rule uploaded for FTA '{fta_name}'",
        }

    hs_clean  = str(hs_code).replace(".", "").replace(" ", "")
    best_thr: float | None  = None
    best_method: str        = "RVC"
    best_len: int           = -1

    for _, rule in fta_rows.iterrows():
        prefix = str(rule.get("hs_code_prefix", "")).replace(".", "").replace(" ", "").strip()
        if not prefix:
            continue
        if hs_clean.startswith(prefix) and len(prefix) > best_len:
            try:
                thr = float(rule["rvc_threshold_pct"])
            except (ValueError, TypeError):
                continue
            best_thr    = thr
            best_method = str(rule.get("rule_method", "RVC"))
            best_len    = len(prefix)

    if best_thr is None:
        return {
            "ok": False, "rvc_threshold_pct": None,
            "threshold_source": "no_rule",
            "reason": f"no RoO rule uploaded for HS {hs_code} under {fta_name}",
        }

    return {
        "ok": True, "rvc_threshold_pct": best_thr,
        "rule_method": best_method,
        "threshold_source": "uploaded_roo_rules",
        "reason": None,
    }


def _resolve_roo(
    product_id: str,
    fta_name: str,
    hs_code: str,
    bom_df: "pd.DataFrame | None",
    roo_df: "pd.DataFrame | None",
    rvc_col: "float | None" = None,
    thr_col: "float | None" = None,
) -> dict:
    """
    Single source of truth for RVC%, threshold, and verdict for one
    (product_id, fta_name, hs_code) triple.  Both _derive_shipments() and
    _derive_roo_assessments_enhanced() call this so the two tables can never
    disagree about the same product.

    Precedence:
      RVC       — BoM via _compute_bom_rvc() > rvc_col (pre-aggregated column value)
      Threshold — uploaded rules via _lookup_roo_threshold() > thr_col (column value)

    Returns a dict with keys:
      rvc_pct, roo_threshold_pct, roo_status (Q/M/F or None),
      rvc_source, threshold_source, verdict, gap_pct, rule_method,
      rvc_reason, threshold_reason  (for provenance labels in the enhanced path)
    """
    # ── RVC ──────────────────────────────────────────────────────────────────
    rvc_pct:    float | None = None
    rvc_source: str          = "unavailable"
    rvc_reason: str | None   = None

    if bom_df is not None:
        rvc_source = "computed_from_bom"
        if product_id:
            bom_res = _compute_bom_rvc(bom_df, product_id, fta_name)
            if bom_res["ok"]:
                rvc_pct = bom_res["rvc_pct"]
            else:
                rvc_reason = bom_res["reason"]
        else:
            rvc_reason = "product_id not found in shipment data — BoM lookup unavailable"
    elif rvc_col is not None:
        rvc_pct    = rvc_col
        rvc_source = "client_column"

    # ── Threshold ─────────────────────────────────────────────────────────────
    rvc_threshold:    float | None = None
    threshold_source: str          = "no_rule"
    threshold_reason: str | None   = None
    rule_method:      str          = "Regional Value Content"

    if roo_df is not None:
        thr_res = _lookup_roo_threshold(roo_df, fta_name, hs_code)
        threshold_source = thr_res["threshold_source"]
        if thr_res["ok"]:
            rvc_threshold = thr_res["rvc_threshold_pct"]
            rule_method   = thr_res.get("rule_method", "RVC")
        else:
            threshold_reason = thr_res["reason"]
    elif thr_col is not None:
        rvc_threshold    = thr_col
        threshold_source = "client_column"

    # ── Verdict ───────────────────────────────────────────────────────────────
    roo_status: str | None = None
    gap_pct:    float      = 0.0
    verdict:    str        = ""

    if rvc_reason:
        verdict = rvc_reason
    elif rvc_threshold is None:
        verdict = "No RoO rule uploaded"
    elif threshold_reason and threshold_source == "no_rule":
        verdict = "No RoO rule uploaded"
    else:
        gap_pct    = round(max(0.0, rvc_threshold - rvc_pct), 1)
        roo_status = _ro_status(rvc_pct, rvc_threshold)
        verdict    = (
            "Qualifies"         if roo_status == "Q" else
            "Near-Miss"         if roo_status == "M" else
            "Does not qualify"
        )

    return {
        "rvc_pct":           rvc_pct,
        "roo_threshold_pct": rvc_threshold,
        "roo_status":        roo_status,
        "rvc_source":        rvc_source,
        "threshold_source":  threshold_source,
        "verdict":           verdict,
        "gap_pct":           gap_pct,
        "rule_method":       rule_method,
        "rvc_reason":        rvc_reason,
        "threshold_reason":  threshold_reason,
    }


def _derive_roo_assessments_enhanced(
    df: pd.DataFrame,
    has_roo_cols: bool,
    bom_df: "pd.DataFrame | None",
    roo_df: "pd.DataFrame | None",
) -> list[dict]:
    """
    RoO assessment with BoM-computed RVC and uploaded-rules threshold.

    Precedence:
      RVC       — BoM (per product_id / shipment_id) > shipment rvc_pct column
      Threshold — uploaded RoO rules (longest-prefix match) > shipment roo_threshold_pct column

    Honesty constraints:
      - Incomplete BoM (any blank component value/origin) → roo_status=None, reason shown
      - No rule for FTA/HS → roo_status=None, "no RoO rule uploaded"
      - Never fabricate a component or threshold
    """
    result: list[dict] = []

    for (product, hs, fta), grp in df.groupby(
        ["product", "hs_code", "fta_name"], sort=False
    ):
        # ── Product ID for BoM lookup ──────────────────────────────────────────
        _pid: str = ""
        for _col in ("product_id", "shipment_id"):
            if _col in grp.columns:
                _cands = grp[_col].dropna().astype(str).str.strip()
                _cands = _cands[~_cands.isin({"", "—", "nan"})]
                if not _cands.empty:
                    _pid = _cands.iloc[0]
                    break

        # ── Pre-compute column fallbacks (used only when BoM/rules not uploaded) ─
        _rvc_col: "float | None" = None
        _thr_col: "float | None" = None
        if bom_df is None and has_roo_cols:
            valid = grp[grp["rvc_pct"].notna()]
            if not valid.empty:
                v_sum    = valid["value"].sum()
                _rvc_col = round(
                    (valid["value"] * valid["rvc_pct"]).sum() / v_sum
                    if v_sum else valid["rvc_pct"].mean(),
                    1,
                )
        if roo_df is None and has_roo_cols:
            valid_thr = grp[grp["roo_threshold_pct"].notna()]
            if not valid_thr.empty:
                _thr_col = float(valid_thr["roo_threshold_pct"].mode().iloc[0])

        # ── Shared resolver (same logic as _derive_shipments) ─────────────────
        resolved         = _resolve_roo(_pid, fta, hs, bom_df, roo_df, _rvc_col, _thr_col)
        rvc_pct          = resolved["rvc_pct"]
        rvc_source       = resolved["rvc_source"]
        rvc_reason       = resolved["rvc_reason"]
        rvc_threshold    = resolved["roo_threshold_pct"]
        threshold_source = resolved["threshold_source"]
        threshold_reason = resolved["threshold_reason"]
        roo_status       = resolved["roo_status"]
        gap_pct          = resolved["gap_pct"]
        rule_method      = resolved["rule_method"]
        verdict          = resolved["verdict"]

        # ── Provenance display labels ──────────────────────────────────────────
        _rvc_lbl = (
            f"{rvc_pct}% (from BoM)" if rvc_source == "computed_from_bom"
            else f"{rvc_pct}% (client-provided)"
        ) if rvc_pct is not None else "—"

        _thr_lbl = (
            f"{rvc_threshold}% (uploaded rule)" if threshold_source == "uploaded_roo_rules"
            else f"{rvc_threshold}% (client-provided)"
        ) if rvc_threshold is not None else "—"

        # ── Compliance note ────────────────────────────────────────────────────
        compliance_note: str = ""
        if rvc_reason:
            compliance_note = f"RVC: {rvc_reason}. Threshold: {_thr_lbl}."
        elif verdict == "No RoO rule uploaded":
            compliance_note = (
                f"RVC {_rvc_lbl}. {threshold_reason}."
                if threshold_reason
                else f"RVC {_rvc_lbl}. No RoO threshold — upload a RoO rules file."
            )
        elif roo_status == "Q":
            compliance_note = (
                f"RVC {_rvc_lbl} meets {_thr_lbl} threshold — "
                "maintain CoO documentation."
            )
        elif roo_status == "M":
            compliance_note = (
                f"RVC {_rvc_lbl} is within {gap_pct}% of {_thr_lbl} threshold — "
                "minor BOM adjustments may qualify this product."
            )
        else:
            compliance_note = (
                f"RVC {_rvc_lbl} is {gap_pct}% below {_thr_lbl} threshold — "
                "significant sourcing changes required."
            )

        result.append({
            "product":           str(product),
            "hs_code":           str(hs),
            "fta_name":          str(fta),
            "roo_test_type":     rule_method,
            "rvc_pct":           rvc_pct,
            "roo_threshold_pct": rvc_threshold,
            "roo_status":        roo_status,
            "verdict":           verdict,
            "gap_pct":           gap_pct,
            "compliance_note":   compliance_note,
            "rvc_source":        rvc_source,
            "threshold_source":  threshold_source,
        })

    return result


def _derive_qualification_roadmap(
    lanes: list[dict], df: pd.DataFrame, has_roadmap: bool
) -> list[dict]:
    """Generate qualification actions for under-utilised lanes (utilization_pct < 75%)."""
    roadmap: list[dict] = []

    for lane in lanes:
        if lane["utilization_pct"] >= 75:
            continue

        fta      = lane["fta_name"]
        pri, sec = _FTA_ACTIONS.get(fta, _FTA_ACTIONS_DEFAULT)

        if has_roadmap and not df.empty and "supplier_name" in df.columns:
            lane_uncl = df[
                (df["fta_name"] == fta)
                & (df["origin"] == lane["origin"])
                & (df["destination"] == lane["destination"])
                & (df["claimed_status"].str.strip().str.upper() == "U")
            ]
            suppliers = list(lane_uncl["supplier_name"].dropna().unique())[:2]
            if suppliers:
                sec = f"Priority suppliers: {', '.join(str(s) for s in suppliers)}. {sec}"

        gap = 100 - lane["utilization_pct"]
        if gap > 40:
            effort, timeline = "High",   "6–10 weeks"
        elif gap > 20:
            effort, timeline = "Medium", "4–6 weeks"
        else:
            effort, timeline = "Low",    "2–3 weeks"

        roadmap.append({
            "lane_id":             lane["lane_id"],
            "origin":              lane["origin"],
            "destination":         lane["destination"],
            "lane_display":        f"{lane['origin']} → {lane['destination']}",
            "fta_name":            fta,
            "utilization_pct":     lane["utilization_pct"],
            "unclaimed_savings_k": lane["unclaimed_savings_k"],
            "primary_action":      pri,
            "secondary_action":    sec,
            "effort":              effort,
            "timeline":            timeline,
        })

    roadmap.sort(key=lambda x: (x["unclaimed_savings_k"] or 0), reverse=True)
    return roadmap


def _derive_period_label(df: pd.DataFrame) -> str:
    """Human-readable date range from the entry_date column (accepts YYYYMMDD or YYYY-MM-DD)."""
    if "entry_date" not in df.columns:
        return "Uploaded data"
    # Normalise to YYYY-MM-DD before parsing
    normalised = df["entry_date"].apply(
        lambda v: f"{str(v)[:4]}-{str(v)[4:6]}-{str(v)[6:]}"
        if len(str(v).replace("-", "")) == 8 and str(v).replace("-", "").isdigit()
        else str(v)
    )
    dates = pd.to_datetime(normalised, errors="coerce").dropna()
    if dates.empty:
        return "Uploaded data"
    lo = dates.min().strftime("%b %Y")
    hi = dates.max().strftime("%b %Y")
    return f"Upload: {lo} – {hi}" if lo != hi else f"Upload: {lo}"


# ── State management (public) ─────────────────────────────────────────────────

def get_source_info() -> dict:
    """Snapshot of current data-source state for the UI."""
    with _lock:
        s_df  = _state["shipment_df"]
        return {
            "shipment_mode":        _state["shipment_mode"],
            "coo_mode":             _state["coo_mode"],
            "bom_mode":             _state["bom_mode"],
            "roo_mode":             _state["roo_mode"],
            "shipment_filename":    _state["shipment_filename"],
            "coo_filename":         _state["coo_filename"],
            "bom_filename":         _state["bom_filename"],
            "roo_filename":         _state["roo_filename"],
            "has_roo":              _state["shipment_has_roo"],
            "has_roadmap":          _state["shipment_has_roadmap"],
            "roo_missing_cols":     [c for c in SHIPMENT_ROO
                                     if s_df is not None and c not in s_df.columns],
            "roadmap_missing_cols": [c for c in SHIPMENT_ROADMAP
                                     if s_df is not None and c not in s_df.columns],
        }


def take_upload_messages() -> "tuple[dict | None, dict | None, dict | None, dict | None]":
    """Return and clear pending upload result messages (shipment, coo, bom, roo)."""
    with _lock:
        s_msg = _state["upload_shipment_msg"]
        c_msg = _state["upload_coo_msg"]
        b_msg = _state["upload_bom_msg"]
        r_msg = _state["upload_roo_msg"]
        _state["upload_shipment_msg"] = None
        _state["upload_coo_msg"]      = None
        _state["upload_bom_msg"]      = None
        _state["upload_roo_msg"]      = None
    return s_msg, c_msg, b_msg, r_msg


def reset_to_empty() -> None:
    """Clear all uploads and return to the empty (no-data) state."""
    with _lock:
        _state.update({
            "shipment_mode":        "empty",
            "coo_mode":             "empty",
            "bom_mode":             "empty",
            "roo_mode":             "empty",
            "shipment_df":          None,
            "coo_df":               None,
            "bom_df":               None,
            "roo_df":               None,
            "shipment_filename":    None,
            "coo_filename":         None,
            "bom_filename":         None,
            "roo_filename":         None,
            "shipment_has_roo":     False,
            "shipment_has_roadmap": False,
            "upload_shipment_msg":  None,
            "upload_coo_msg":       None,
            "upload_bom_msg":       None,
            "upload_roo_msg":       None,
        })


def upload_shipment_data(file_bytes: bytes, filename: str) -> dict:
    """
    Parse, validate and store shipment data.  Returns {"ok", "errors", "warnings"}.
    On success, switches shipment_mode to "uploaded".
    """
    try:
        raw_df = _parse_file(file_bytes, filename)
    except Exception as exc:
        result: dict = {"ok": False, "errors": [str(exc)], "warnings": []}
        with _lock:
            _state["upload_shipment_msg"] = result
        return result

    df = _normalise_cols(raw_df)
    df = _rename_erp_cols(df, _ERP_SHIPMENT_MAP)   # ERP→snake_case boundary
    # Normalise coded values AFTER alias resolution so text status values like
    # "Claimed"/"Unclaimed" are translated to E/U/N before validation fires.
    df, norm_infos, norm_warns = _normalise_coded_values(df)
    # Blank/NaN fta_name (ERP rows with no FTA) → "N/A" so groupby retains the lane
    if "fta_name" in df.columns:
        df["fta_name"] = df["fta_name"].fillna("N/A").replace("", "N/A")
    ok, errors, warnings, has_roo, has_roadmap = _validate_shipment(df)
    warnings = norm_infos + norm_warns + warnings

    if ok:
        with _lock:
            _state.update({
                "shipment_mode":        "uploaded",
                "shipment_df":          df,
                "shipment_filename":    filename,
                "shipment_has_roo":     has_roo,
                "shipment_has_roadmap": has_roadmap,
            })

    result = {"ok": ok, "errors": errors, "warnings": warnings}
    with _lock:
        _state["upload_shipment_msg"] = result
    return result


def upload_coo_data(file_bytes: bytes, filename: str) -> dict:
    """
    Parse, validate and store CoO request data.  Returns {"ok", "errors", "warnings"}.
    On success, switches coo_mode to "uploaded".
    """
    try:
        raw_df = _parse_file(file_bytes, filename)
    except Exception as exc:
        result: dict = {"ok": False, "errors": [str(exc)], "warnings": []}
        with _lock:
            _state["upload_coo_msg"] = result
        return result

    df = _normalise_cols(raw_df)
    df = _rename_erp_cols(df, _ERP_COO_MAP)   # ERP→snake_case boundary
    df, norm_infos, norm_warns = _normalise_coo_coded_values(df)
    ok, errors, warnings = _validate_coo(df)
    warnings = norm_infos + norm_warns + warnings   # prepend normalisation context

    if ok:
        with _lock:
            _state.update({
                "coo_mode":      "uploaded",
                "coo_df":        df,
                "coo_filename":  filename,
            })

    result = {"ok": ok, "errors": errors, "warnings": warnings}
    with _lock:
        _state["upload_coo_msg"] = result
    return result


def upload_bom_data(file_bytes: bytes, filename: str) -> dict:
    """
    Parse, validate and store Bill of Materials data.
    Returns {"ok", "errors", "warnings"}.
    On success, switches bom_mode to "uploaded".
    """
    try:
        raw_df = _parse_file(file_bytes, filename)
    except Exception as exc:
        result: dict = {"ok": False, "errors": [str(exc)], "warnings": []}
        with _lock:
            _state["upload_bom_msg"] = result
        return result

    df = _normalise_cols(raw_df)
    df = _rename_erp_cols(df, _ERP_BOM_MAP)
    # _normalise_coded_values is a no-op for BoM but keeps pipeline order consistent
    df, norm_infos, norm_warns = _normalise_coded_values(df)
    ok, errors, warnings = _validate_bom(df)
    warnings = norm_infos + norm_warns + warnings

    if ok:
        df = df.copy()
        df["component_value"] = pd.to_numeric(df["component_value"], errors="coerce")
        with _lock:
            _state.update({
                "bom_mode":     "uploaded",
                "bom_df":       df,
                "bom_filename": filename,
            })

    result = {"ok": ok, "errors": errors, "warnings": warnings}
    with _lock:
        _state["upload_bom_msg"] = result
    return result


def upload_roo_data(file_bytes: bytes, filename: str) -> dict:
    """
    Parse, validate and store Rules-of-Origin rules data.
    Returns {"ok", "errors", "warnings"}.
    On success, switches roo_mode to "uploaded".
    """
    try:
        raw_df = _parse_file(file_bytes, filename)
    except Exception as exc:
        result: dict = {"ok": False, "errors": [str(exc)], "warnings": []}
        with _lock:
            _state["upload_roo_msg"] = result
        return result

    df = _normalise_cols(raw_df)
    df = _rename_erp_cols(df, _ERP_ROO_MAP)
    # _normalise_coded_values is a no-op for RoO rules but keeps pipeline order consistent
    df, norm_infos, norm_warns = _normalise_coded_values(df)
    ok, errors, warnings = _validate_roo(df)
    warnings = norm_infos + norm_warns + warnings

    if ok:
        df = df.copy()
        df["rvc_threshold_pct"] = pd.to_numeric(df["rvc_threshold_pct"], errors="coerce")
        with _lock:
            _state.update({
                "roo_mode":     "uploaded",
                "roo_df":       df,
                "roo_filename": filename,
            })

    result = {"ok": ok, "errors": errors, "warnings": warnings}
    with _lock:
        _state["upload_roo_msg"] = result
    return result


# ── Column-mapping helpers (used by the AJAX upload flow) ────────────────────

def parse_for_mapping(file_bytes: bytes, filename: str):
    """
    Parse an uploaded file and return (df, column_list, samples_dict).
    Columns are normalised (strip/upper) but NOT renamed yet.
    """
    import fta_mapping as _fm
    raw_df  = _parse_file(file_bytes, filename)
    df      = _normalise_cols(raw_df)
    columns = list(df.columns)
    samples = _fm.extract_samples(df)
    return df, columns, samples


def is_native_format(columns: list[str]) -> bool:
    """
    True when the file can be loaded by upload_shipment_data without a mapping dialog:
    - Already in internal snake_case schema (every SHIPMENT_REQUIRED column present), OR
    - SAP/ERP column names (upload_shipment_data calls _rename_erp_cols internally).
    """
    # Internal snake_case format
    if all(c in columns for c in SHIPMENT_REQUIRED):
        return True
    # SAP ERP format — _rename_erp_cols maps these to snake_case at the boundary
    _SAP_REQUIRED = {"TOR_ID", "PRODUCT_TEXT", "CCNGN", "CTYDP", "CTYAR", "CUSVAL", "PREF_STATUS", "ENTRY_DATE"}
    return _SAP_REQUIRED.issubset(set(columns))


def apply_mapping_and_load_shipments(
    df,                         # pandas DataFrame (normalised but not renamed)
    mapping: dict,              # {SAP_NAME: uploaded_col_or_None, ...}
) -> dict:
    """
    Rename df according to confirmed mapping, validate, derive, store in _state.
    Returns {"ok": bool, "errors": [...], "warnings": [...]}.
    """
    import pandas as pd  # ensure available

    # Build rename dict: uploaded_col -> SAP_NAME
    rename = {col: sap for sap, col in mapping.items() if col}
    df_renamed = df.rename(columns=rename)
    df_renamed  = _normalise_cols(df_renamed)

    # Normalise coded field values BEFORE the ERP→snake_case rename so that
    # _normalise_coded_values can find "PREF_STATUS" (SAP name).
    df_renamed, norm_infos, norm_warns = _normalise_coded_values(df_renamed)

    # Translate SAP column names → internal snake_case so _validate_shipment
    # and all downstream derivation functions see the expected schema.
    df_renamed = _rename_erp_cols(df_renamed, _ERP_SHIPMENT_MAP)
    if "fta_name" in df_renamed.columns:
        df_renamed["fta_name"] = df_renamed["fta_name"].fillna("N/A").replace("", "N/A")

    ok, errors, warnings, has_roo, has_roadmap = _validate_shipment(df_renamed)
    # Prepend normalisation context so users know what was auto-translated.
    warnings = norm_infos + norm_warns + warnings

    # We accept the data even when optional sections (RoO, Roadmap) are absent;
    # _validate_shipment returns ok=False only when required cols are missing.
    if not ok:
        result: dict = {"ok": False, "errors": errors, "warnings": warnings}
        with _lock:
            _state["upload_shipment_msg"] = result
        return result

    with _lock:
        _state.update({
            "shipment_mode":        "uploaded",
            "shipment_df":          df_renamed,
            "shipment_filename":    "(column-mapped upload)",
            "shipment_has_roo":     has_roo,
            "shipment_has_roadmap": has_roadmap,
        })
        _state["upload_shipment_msg"] = {
            "ok": True, "errors": [], "warnings": warnings,
        }

    return {"ok": True, "errors": [], "warnings": warnings}


# ── Public API — same signatures as fta_simulator ────────────────────────────

def get_fta_lanes() -> list:
    with _lock:
        mode = _state["shipment_mode"]
        df   = _state["shipment_df"]
    if mode != "uploaded":
        return []
    lanes = _derive_lanes(df)
    enriched = _enrich_lanes_from_aggregator(lanes)
    # Sort after enrichment so ordering reflects real savings (None lanes sort last)
    enriched.sort(key=lambda x: (x["unclaimed_savings_k"] or 0), reverse=True)
    return enriched


def get_fta_lanes_by_status(status_bucket: str = "all") -> list:
    """
    Like get_fta_lanes() but pre-filters shipments by SHIPMENT_STATUS.
    status_bucket:
      "all"        — no filter (identical to get_fta_lanes)
      "shipped"    — SHIPMENT_STATUS in {Delivered, Shipped}
      "in_transit" — SHIPMENT_STATUS == In-Transit (any spelling variant)
    Returns [] if shipment_status column is absent and bucket != "all".
    """
    with _lock:
        mode = _state["shipment_mode"]
        df   = _state["shipment_df"]
    if mode != "uploaded":
        return []
    if status_bucket == "shipped":
        if "shipment_status" not in df.columns:
            return []
        df = df[df["shipment_status"].str.strip().str.upper().isin(["DELIVERED", "SHIPPED"])]
    elif status_bucket == "in_transit":
        if "shipment_status" not in df.columns:
            return []
        df = df[df["shipment_status"].str.strip().str.upper().isin(
            ["IN-TRANSIT", "IN_TRANSIT", "INTRANSIT", "IN TRANSIT"]
        )]
    if df.empty:
        return []
    lanes = _derive_lanes(df)
    enriched = _enrich_lanes_from_aggregator(lanes)
    enriched.sort(key=lambda x: (x["unclaimed_savings_k"] or 0), reverse=True)
    return enriched


def get_fta_shipments() -> list:
    with _lock:
        mode    = _state["shipment_mode"]
        df      = _state["shipment_df"]
        has_roo = _state["shipment_has_roo"]
        bom_df  = _state["bom_df"] if _state["bom_mode"] == "uploaded" else None
        roo_df  = _state["roo_df"] if _state["roo_mode"] == "uploaded" else None
    if mode != "uploaded":
        return []

    shipments = _derive_shipments(df, has_roo, bom_df, roo_df)

    # Compute est_saving_k per shipment using per-HS aggregator rates so that
    # accessory lanes with different MFN rates (e.g. 4202.12 @ 5.7% vs 8544.42 @ 2.6%)
    # each use their own rate rather than a single lane-representative rate.
    # Lookup precedence:
    #   1. Per-HS direct store query (agg._store.get(hs6, origin, destination, today))
    #   2. Lane-level rate cached in _enriched_rates (fallback)
    from datetime import date as _date
    agg        = _get_aggregator()
    today      = _date.today()
    lane_rates = _enriched_rates

    if agg is None and not lane_rates:
        return shipments

    result = []
    for s in shipments:
        if s["eligibility"] != "eligible-unclaimed":
            result.append(s)
            continue

        mfn = pref = None

        # 1. Per-HS store lookup
        if agg:
            hs6 = "".join(c for c in str(s["hs_code"]) if c.isdigit())[:6]
            try:
                canonical = agg._store.get(hs6, s["origin"], s["destination"], today)
                if canonical and canonical.mfn_rate is not None and canonical.preferential_rate is not None:
                    mfn  = canonical.mfn_rate
                    pref = canonical.preferential_rate
            except Exception:
                pass

        # 2. Lane-level fallback
        if mfn is None:
            enriched = lane_rates.get((s["fta_name"], s["origin"], s["destination"]))
            if enriched:
                mfn  = enriched["mfn_rate_pct"]
                pref = enriched["preferential_rate_pct"]

        if mfn is not None and pref is not None:
            s = dict(s)
            s["est_saving_k"] = round(max(0.0, s["value_k"] * (mfn - pref) / 100), 1)

        result.append(s)
    return result


def get_coo_requests() -> list:
    with _lock:
        mode = _state["coo_mode"]
        df   = _state["coo_df"]
    if mode != "uploaded":
        return []

    valid_status = {"PENDING", "RECEIVED", "OVERDUE", "VALIDATED"}
    result: list[dict] = []
    for _, row in df.iterrows():
        raw_s = str(row.get("status", "PENDING")).strip().upper()
        poo_s = raw_s if raw_s in valid_status else "PENDING"
        result.append({
            "supplier_id":   str(row.get("supplier_id",   "—")),
            "supplier_name": str(row.get("supplier_name", "—")),
            "origin":        str(row.get("origin",        "—")),
            "destination":   str(row.get("destination",   "—")),
            "poo_type":      str(row.get("poo_type",      "—")),
            "doc_type":      str(row.get("doc_type",      "—")).strip(),
            "request_date":  _normalise_dats(row.get("request_date", "")),
            "deadline":      _normalise_dats(row.get("deadline",      "")),
            "status":        poo_s,
        })
    return result


def get_fta_kpis() -> dict:
    """
    All four KPIs derived from the same lanes + CoO data the tables display.

    Formula:
        Utilization Rate  = Σ claimed_value_m / Σ eligible_value_m
        Unclaimed Oppty   = Σ lane.unclaimed_savings_k (aggregator-covered lanes only)
        Retroactive Claims= None — retro eligibility is not in the upload schema;
                            rendering None as "Not available" (FIX A) is honest;
                            showing 0 would falsely imply "checked and found none"
        CoOs Outstanding  = count of coo where status ∈ {pending,overdue,received}
    """
    with _lock:
        s_mode = _state["shipment_mode"]
        df     = _state["shipment_df"]

    if s_mode != "uploaded":
        return {
            "empty":                    True,
            "utilization_pct":          None,
            "unclaimed_opportunity_m":  None,
            "retroactive_claims_k":     None,
            "coo_outstanding":          None,
            "period_label":             "—",
            "retro_window_label":       "Not available",
        }

    lanes = get_fta_lanes()
    coos  = get_coo_requests()

    total_eligible_m  = sum(l["eligible_value_m"] for l in lanes)
    total_claimed_m   = sum(l["claimed_value_m"]  for l in lanes)
    # Only sum lanes where aggregator supplied both rates (unclaimed_savings_k is not None)
    total_unclaimed_k = sum(
        l["unclaimed_savings_k"] for l in lanes
        if l["unclaimed_savings_k"] is not None
    )
    coo_outstanding = sum(1 for c in coos if c["status"] in ("PENDING", "OVERDUE", "RECEIVED"))

    util_pct = (
        round(total_claimed_m / total_eligible_m * 100, 1)
        if total_eligible_m else 0.0
    )

    return {
        "utilization_pct":          util_pct,
        "unclaimed_opportunity_m":  round(total_unclaimed_k / 1_000, 2),
        "retroactive_claims_k":     None,   # FIX A: uncomputable from upload, not zero
        "coo_outstanding":          coo_outstanding,
        "period_label":             _derive_period_label(df) if df is not None else "Uploaded data",
        "retro_window_label":       "Not available from upload",
    }


def get_roo_assessments():
    """
    Returns a list of RoO assessment dicts, or a sentinel dict describing why
    assessment is unavailable.

    Precedence (RVC):
      1. Computed from uploaded BoM (per product_id / shipment_id fallback)
      2. Client-provided rvc_pct column in shipment upload
      3. Unavailable

    Precedence (threshold):
      1. Uploaded RoO rules (matched by fta_name + longest hs_code_prefix)
      2. Client-provided roo_threshold_pct column in shipment upload
      3. 'no RoO rule uploaded'

    State A  — no shipment upload   → {"unavailable": True, "reason": "no_upload"}
    State B  — uploaded, no RVC source at all → {"unavailable": True, "missing_cols": [...]}
    State C  — no BoM/RoO uploads: pure shipment-column path (UNCHANGED from before)
    State C+ — BoM or RoO rules uploaded: enhanced path with provenance fields
               (rvc_source, threshold_source, verdict added to each row dict)
    """
    with _lock:
        s_mode    = _state["shipment_mode"]
        s_df      = _state["shipment_df"]
        has_roo   = _state["shipment_has_roo"]
        bom_mode  = _state["bom_mode"]
        bom_df    = _state["bom_df"]
        roo_mode  = _state["roo_mode"]
        roo_df    = _state["roo_df"]

    has_bom       = (bom_mode == "uploaded" and bom_df is not None)
    has_roo_rules = (roo_mode == "uploaded" and roo_df is not None)

    # State A: no shipment data
    if s_mode != "uploaded":
        return {"unavailable": True, "reason": "no_upload"}

    # State B: no RVC source at all (no BoM, no shipment RoO cols)
    if not has_bom and not has_roo:
        missing = [c for c in SHIPMENT_ROO
                   if s_df is not None and c not in s_df.columns]
        return {
            "unavailable":  True,
            "missing_cols": missing if missing else list(SHIPMENT_ROO),
        }

    # State C: pure shipment-column path — UNCHANGED from before
    if not has_bom and not has_roo_rules:
        return _derive_roo_assessments(s_df)

    # State C+: enhanced path (at least one of BoM / RoO rules uploaded)
    return _derive_roo_assessments_enhanced(
        s_df, has_roo,
        bom_df if has_bom else None,
        roo_df if has_roo_rules else None,
    )


def get_qualification_roadmap() -> list:
    with _lock:
        mode        = _state["shipment_mode"]
        df          = _state["shipment_df"]
        has_roadmap = _state["shipment_has_roadmap"]

    if mode != "uploaded":
        return []

    lanes = get_fta_lanes()
    return _derive_qualification_roadmap(
        lanes,
        df if df is not None else pd.DataFrame(),
        has_roadmap,
    )
