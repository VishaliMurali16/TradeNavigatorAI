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
fta_simulator is used only inside this file.

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


# ── Column specification ──────────────────────────────────────────────────────

# Core — required to render lanes, eligibility feed, and all four KPIs.
# origin_country / destination_country must be ISO 3166-1 alpha-2 codes (e.g. "KR", "US")
# so the aggregator can look up real tariff rates for each lane.
SHIPMENT_REQUIRED: list[str] = [
    "shipment_id",
    "product",
    "hs_code",
    "origin_country",
    "destination_country",
    "value",             # transaction value in raw USD, e.g. 245500 = $245,500
    "applicable_fta",
    "claimed_status",    # "claimed" | "unclaimed" | "not-eligible"
    "entry_date",        # ISO date string: YYYY-MM-DD
]

# Optional rate columns — accepted for cross-reference but NOT used for derivation.
# Tariff rates are sourced exclusively from the live aggregator.
SHIPMENT_RATES: list[str] = ["mfn_rate", "preferential_rate"]

# Optional — enables RoO Compliance Assessment section.
SHIPMENT_ROO: list[str] = [
    "regional_value_content_pct",  # actual RVC of the shipment (%)
    "roo_threshold_pct",            # RVC threshold required under the FTA (%)
]

# Optional — enriches Qualification Roadmap with supplier context.
SHIPMENT_ROADMAP: list[str] = [
    "supplier_name",
    "bom_regional_content",  # free-text: regional content breakdown in BOM
]

# Required columns for the CoO Requests file.
COO_REQUIRED: list[str] = [
    "supplier",
    "lane",
    "request_date",
    "deadline",
    "status",   # "pending" | "received" | "overdue" | "validated"
]

# ── Downloadable template CSV content ────────────────────────────────────────
# Rows are representative; column order matches SHIPMENT_REQUIRED then optionals.

SHIPMENT_TEMPLATE_CSV = """\
shipment_id,product,hs_code,origin_country,destination_country,value,applicable_fta,claimed_status,entry_date,regional_value_content_pct,roo_threshold_pct,supplier_name,bom_regional_content
SMP-001,Aged Cheddar,0406.90,KR,US,180000,KORUS,unclaimed,2026-06-15,72,45,Seoul Dairy Co,72% Korean-origin dairy inputs
SMP-002,Gouda Cheese Blend,0406.90,KR,US,220000,KORUS,unclaimed,2026-07-01,68,45,Seoul Dairy Co,68% Korean-origin dairy inputs
SMP-003,Processed Dairy Mix,0406.90,KR,US,95000,KORUS,claimed,2026-05-20,80,45,Busan Foods Ltd,80% Korean-origin dairy
SMP-004,Laptop Computer,8471.30,VN,US,340000,N/A,unclaimed,2026-06-22,35,40,Hanoi Tech JSC,35% ASEAN value-added content
SMP-005,Desktop Workstation,8471.30,CN,US,195000,N/A,unclaimed,2026-07-20,28,40,Shenzhen Systems,28% regional content
"""

COO_TEMPLATE_CSV = """\
supplier,lane,request_date,deadline,status
Seoul Dairy Co,KR → US,2026-07-05,2026-08-07,overdue
Busan Foods Ltd,KR → US,2026-07-12,2026-08-12,pending
Hanoi Tech JSC,VN → US,2026-07-18,2026-08-18,received
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


# ── Thread-safe in-memory state ───────────────────────────────────────────────

_lock = threading.Lock()
_state: dict[str, Any] = {
    # "empty" | "uploaded"  — tracked independently per data type
    "shipment_mode":        "empty",
    "coo_mode":             "empty",
    # Normalised pandas DataFrames (None when in simulated mode)
    "shipment_df":          None,
    "coo_df":               None,
    # Original filenames for UI display
    "shipment_filename":    None,
    "coo_filename":         None,
    # Which optional column groups were present in the last upload
    "shipment_has_roo":     False,
    "shipment_has_roadmap": False,
    # Last upload result — read-once by the page on next GET
    "upload_shipment_msg":  None,   # {"ok": bool, "errors": [...], "warnings": [...]}
    "upload_coo_msg":       None,
}

# ── Internal helpers ──────────────────────────────────────────────────────────

def _ro_status(rvc: float, threshold: float) -> str:
    """Canonical RoO status rule — identical to fta_simulator._derive_ro_status."""
    if rvc >= threshold:
        return "qualified"
    if rvc >= threshold - 5:
        return "near-miss"
    return "fail"


def _parse_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Deserialise CSV or XLSX bytes into a DataFrame.  Raises ValueError on bad format."""
    fn = filename.lower()
    _hs_str = {"hs_code": str}  # preserve leading zeros (e.g. 0406.90 must not become 406.9)
    if fn.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes), dtype=_hs_str)
    if fn.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(file_bytes), dtype=_hs_str)
    raise ValueError(f"Unsupported file type '{filename}'. Upload a .csv or .xlsx file.")


def _normalise_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase + strip whitespace in column names for fault-tolerant matching."""
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _validate_shipment(
    df: pd.DataFrame,
) -> tuple[bool, list[str], list[str], bool, bool]:
    """
    Validate a normalised shipment DataFrame.

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
        valid_cs = {"claimed", "unclaimed", "not-eligible"}
        bad_cs = set(df["claimed_status"].dropna().str.strip().str.lower().unique()) - valid_cs
        if bad_cs:
            warnings.append(
                f"Unrecognised claimed_status values {bad_cs!r} "
                "will be treated as 'not-eligible'."
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
    """Validate a normalised CoO DataFrame. Returns (ok, errors, warnings)."""
    errors:   list[str] = []
    warnings: list[str] = []

    if df.empty:
        errors.append("File contains no data rows.")
        return False, errors, warnings

    missing = [c for c in COO_REQUIRED if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")

    if "status" in df.columns:
        valid_s = {"pending", "received", "overdue", "validated"}
        bad_s = set(df["status"].dropna().str.strip().str.lower().unique()) - valid_s
        if bad_s:
            warnings.append(
                f"Unrecognised status values {bad_s!r} will be treated as 'pending'."
            )

    return len(errors) == 0, errors, warnings


# ── Data derivation: uploaded DataFrame → internal dicts ─────────────────────

def _derive_shipments(df: pd.DataFrame, has_roo: bool) -> list[dict]:
    """Map shipment DataFrame rows to the internal shipment dict schema."""
    eligibility_map = {
        "claimed":      "claimed",
        "unclaimed":    "eligible-unclaimed",
        "not-eligible": "not-eligible",
    }
    result: list[dict] = []

    for _, row in df.iterrows():
        raw_cs      = str(row.get("claimed_status", "")).strip().lower()
        eligibility = eligibility_map.get(raw_cs, "not-eligible")

        value_usd = float(row.get("value", 0) or 0)
        value_k   = round(value_usd / 1_000, 1)

        # est_saving_k is re-derived from aggregator rates in get_fta_shipments()
        # after lane enrichment. None here means "rate not yet available".
        est_saving_k = None

        if has_roo:
            rvc_raw = row.get("regional_value_content_pct")
            thr_raw = row.get("roo_threshold_pct")
            if pd.notna(rvc_raw) and pd.notna(thr_raw):
                rvc = float(rvc_raw)
                thr = float(thr_raw)
                ro  = _ro_status(rvc, thr)
            else:
                rvc, thr = 0.0, 0.0
                ro = "qualified" if eligibility == "claimed" else "fail"
        else:
            rvc, thr = 0.0, 0.0
            ro = "qualified" if eligibility == "claimed" else "fail"

        raw_date   = row.get("entry_date", "")
        entry_date = str(raw_date)[:10] if pd.notna(raw_date) and raw_date != "" else "—"

        result.append({
            "shipment_id":       str(row.get("shipment_id", "—")),
            "entry_date":        entry_date,
            "product":           str(row.get("product", "Unknown")),
            "hs_code":           str(row.get("hs_code", "—")),
            "origin":            str(row.get("origin_country", "—")),
            "destination":       str(row.get("destination_country", "—")),
            "value_k":           value_k,
            "fta_name":          str(row.get("applicable_fta", "N/A")),
            "eligibility":       eligibility,
            "est_saving_k":      est_saving_k,
            "rvc_pct":           rvc,
            "rvc_threshold_pct": thr,
            "ro_status":         ro,
        })

    return result


def _derive_lanes(df: pd.DataFrame) -> list[dict]:
    """
    Aggregate shipment rows into trade lanes.

    Lane key: (applicable_fta, origin_country, destination_country).
    not-eligible shipments are excluded from eligible/claimed totals.

    Rates (mfn_rate_pct, preferential_rate_pct, unclaimed_savings_k, retro_k) are
    set to None here and populated by _enrich_lanes_from_aggregator() after this
    function returns. Sorting also happens in get_fta_lanes() after enrichment.
    """
    eligible_df = df[
        df["claimed_status"].str.strip().str.lower().isin(["claimed", "unclaimed"])
    ].copy()

    lanes: list[dict] = []
    counter = 1

    for (fta, origin, dest), grp in eligible_df.groupby(
        ["applicable_fta", "origin_country", "destination_country"],
        sort=False,
    ):
        total_val   = grp["value"].sum()
        claimed_val = grp.loc[
            grp["claimed_status"].str.strip().str.lower() == "claimed", "value"
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
            "origin":                str(origin),
            "destination":           str(dest),
            "fta_name":              str(fta),
            "eligible_value_m":      round(total_val   / 1_000_000, 3),
            "claimed_value_m":       round(claimed_val / 1_000_000, 3),
            "mfn_rate_pct":          None,    # populated by _enrich_lanes_from_aggregator
            "preferential_rate_pct": None,    # populated by _enrich_lanes_from_aggregator
            "utilization_pct":       util_pct,
            "unclaimed_savings_k":   None,    # populated by _enrich_lanes_from_aggregator
            "retro_eligible_pct":    0,        # not derivable from upload
            "retro_k":               None,    # populated by _enrich_lanes_from_aggregator
            "rates_source":          "pending",
            "representative_lane":   {
                "hs_code":     rep_hs,
                "origin":      str(origin),
                "destination": str(dest),
            },
        })
        counter += 1

    return lanes


def _derive_roo_assessments(df: pd.DataFrame) -> list[dict]:
    """
    Aggregate RoO status per (product, hs_code, applicable_fta).
    Uses value-weighted average RVC; derives ro_status via the same rule as
    fta_simulator (so the badge colours are always consistent).
    """
    result: list[dict] = []

    for (product, hs_code, fta), grp in df.groupby(
        ["product", "hs_code", "applicable_fta"], sort=False
    ):
        valid = grp[
            grp["regional_value_content_pct"].notna()
            & grp["roo_threshold_pct"].notna()
        ]
        if valid.empty:
            continue

        v_sum   = valid["value"].sum()
        avg_rvc = round(
            (valid["value"] * valid["regional_value_content_pct"]).sum() / v_sum
            if v_sum else valid["regional_value_content_pct"].mean(),
            1,
        )
        threshold = float(valid["roo_threshold_pct"].mode().iloc[0])

        status  = _ro_status(avg_rvc, threshold)
        gap_pct = round(max(0.0, threshold - avg_rvc), 1)

        if status == "qualified":
            note = "Passes RVC threshold. Maintain CoO documentation."
        elif status == "near-miss":
            note = (
                f"Within {gap_pct}% of RVC threshold — "
                "minor BOM adjustments may qualify this product."
            )
        else:
            note = (
                f"RVC gap of {gap_pct}% below threshold — "
                "significant sourcing changes required."
            )

        # Append supplier names if the column is present
        if "supplier_name" in df.columns:
            suppliers = list(grp["supplier_name"].dropna().unique())[:3]
            if suppliers:
                note += f" Suppliers: {', '.join(str(s) for s in suppliers)}."

        result.append({
            "product":           str(product),
            "hs_code":           str(hs_code),
            "fta_name":          str(fta),
            "roo_test_type":     "Regional Value Content",
            "rvc_pct":           avg_rvc,
            "rvc_threshold_pct": threshold,
            "ro_status":         status,
            "gap_pct":           gap_pct,
            "compliance_note":   note,
        })

    return result


def _derive_qualification_roadmap(
    lanes: list[dict], df: pd.DataFrame, has_roadmap: bool
) -> list[dict]:
    """
    Generate qualification actions for under-utilised lanes (< 75% utilization).
    Action text is FTA-specific.  If roadmap columns are present, supplier names
    from unclaimed shipments are surfaced in the secondary action.
    """
    roadmap: list[dict] = []

    for lane in lanes:
        if lane["utilization_pct"] >= 75:
            continue

        fta      = lane["fta_name"]
        pri, sec = _FTA_ACTIONS.get(fta, _FTA_ACTIONS_DEFAULT)

        # Enrich secondary action with supplier names from unclaimed shipments
        if has_roadmap and not df.empty and "supplier_name" in df.columns:
            lane_uncl = df[
                (df["applicable_fta"] == fta)
                & (df["origin_country"] == lane["origin"])
                & (df["destination_country"] == lane["destination"])
                & (df["claimed_status"].str.strip().str.lower() == "unclaimed")
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
            "lane":                f'{lane["origin"]} → {lane["destination"]}',
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
    """Human-readable date range from the entry_date column."""
    if "entry_date" not in df.columns:
        return "Uploaded data"
    dates = pd.to_datetime(df["entry_date"], errors="coerce").dropna()
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
            "shipment_filename":    _state["shipment_filename"],
            "coo_filename":         _state["coo_filename"],
            "has_roo":              _state["shipment_has_roo"],
            "has_roadmap":          _state["shipment_has_roadmap"],
            "roo_missing_cols":     [c for c in SHIPMENT_ROO
                                     if s_df is not None and c not in s_df.columns],
            "roadmap_missing_cols": [c for c in SHIPMENT_ROADMAP
                                     if s_df is not None and c not in s_df.columns],
        }


def take_upload_messages() -> tuple[dict | None, dict | None]:
    """Return and clear pending upload result messages (shipment_msg, coo_msg)."""
    with _lock:
        s_msg = _state["upload_shipment_msg"]
        c_msg = _state["upload_coo_msg"]
        _state["upload_shipment_msg"] = None
        _state["upload_coo_msg"]      = None
    return s_msg, c_msg


def reset_to_empty() -> None:
    """Clear all uploads and return to the empty (no-data) state."""
    with _lock:
        _state.update({
            "shipment_mode":        "empty",
            "coo_mode":             "empty",
            "shipment_df":          None,
            "coo_df":               None,
            "shipment_filename":    None,
            "coo_filename":         None,
            "shipment_has_roo":     False,
            "shipment_has_roadmap": False,
            "upload_shipment_msg":  None,
            "upload_coo_msg":       None,
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
    ok, errors, warnings, has_roo, has_roadmap = _validate_shipment(df)

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
    ok, errors, warnings = _validate_coo(df)

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


def get_fta_shipments() -> list:
    with _lock:
        mode    = _state["shipment_mode"]
        df      = _state["shipment_df"]
        has_roo = _state["shipment_has_roo"]
    if mode != "uploaded":
        return []

    shipments = _derive_shipments(df, has_roo)

    # Re-derive est_saving_k from aggregator rates (FIX 1: all rate-derived figures
    # move together). _enriched_rates is populated by get_fta_lanes(); it only
    # contains lanes where the aggregator supplied both MFN and pref rates.
    rates = _enriched_rates
    if not rates:
        return shipments

    result = []
    for s in shipments:
        key      = (s["fta_name"], s["origin"], s["destination"])
        enriched = rates.get(key)
        if enriched and s["eligibility"] == "eligible-unclaimed":
            s    = dict(s)
            mfn  = enriched["mfn_rate_pct"]
            pref = enriched["preferential_rate_pct"]
            s["est_saving_k"] = round(max(0.0, s["value_k"] * (mfn - pref) / 100), 1)
        result.append(s)
    return result


def get_coo_requests() -> list:
    with _lock:
        mode = _state["coo_mode"]
        df   = _state["coo_df"]
    if mode != "uploaded":
        return []

    status_norm = {"pending": "pending", "received": "received",
                   "overdue": "overdue",  "validated": "validated"}
    result: list[dict] = []
    for _, row in df.iterrows():
        raw_s = str(row.get("status", "pending")).strip().lower()
        result.append({
            "supplier":     str(row.get("supplier",     "—")),
            "lane":         str(row.get("lane",         "—")),
            "request_date": str(row.get("request_date", "—"))[:10],
            "deadline":     str(row.get("deadline",     "—"))[:10],
            "status":       status_norm.get(raw_s, "pending"),
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
    coo_outstanding = sum(1 for c in coos if c["status"] in ("pending", "overdue", "received"))

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
    Returns a list of RoO assessment dicts (same schema as fta_simulator), or
    {"unavailable": True, "missing_cols": [...]} when RoO columns are absent.
    The caller must check for the "unavailable" key before iterating.
    """
    with _lock:
        mode    = _state["shipment_mode"]
        df      = _state["shipment_df"]
        has_roo = _state["shipment_has_roo"]
        missing = [c for c in SHIPMENT_ROO
                   if df is not None and c not in df.columns]

    if mode != "uploaded":
        return {"unavailable": True, "reason": "no_upload"}

    if not has_roo:
        return {
            "unavailable":   True,
            "missing_cols":  missing if missing else list(SHIPMENT_ROO),
        }

    return _derive_roo_assessments(df)


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
