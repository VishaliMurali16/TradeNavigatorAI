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

import fta_simulator as _sim
from fta_simulator import COUNTRY_NAMES as _CTRY

# ── Column specification (SAP-native field names) ─────────────────────────────
# See fta_simulator.FIELD_DICTIONARY for provenance marks (🟢/🔴).

# Core — required to render lanes, eligibility feed, and all four KPIs.
SHIPMENT_REQUIRED: list[str] = [
    "TOR_ID",       # Freight Order ID          🟢
    "PRODUCT_TEXT", # Product Description        🔴
    "CCNGN",        # Commodity / Tariff Code    🟢
    "CTYDP",        # Country of Departure       🟢  ISO2 e.g. MX
    "CTYAR",        # Country of Destination     🟢  ISO2 e.g. US
    "CUSVAL",       # Customs Value (USD)        🔴  decimal e.g. 245500.00
    "AGREEMENT",    # Trade Agreement / FTA      🟢
    "PREF_STATUS",  # Preference Status          🔴  E|U|N
    "ENTRY_DATE",   # Customs Entry Date         🔴  YYYYMMDD or YYYY-MM-DD accepted
    "MFN_RATE",     # MFN Duty Rate %            🔴  e.g. 7.500
    "PREF_RATE",    # Preferential Duty Rate %   🔴  e.g. 0.000
]

# Optional — enables RoO Compliance Assessment section.
SHIPMENT_ROO: list[str] = [
    "RVC_PCT",       # Regional Value Content % actual  🔴
    "RVC_THRESHOLD", # Required RoO Threshold %         🔴
]

# Optional — enriches Qualification Roadmap with supplier context.
SHIPMENT_ROADMAP: list[str] = [
    "SUPPLIER_NAME",        # Supplier Name           🔴
    "BOM_REGIONAL_CONTENT", # BoM Regional Content %  🔴
]

# Required columns for the CoO / Proof-of-Origin file.
COO_REQUIRED: list[str] = [
    "SUPPLIER_NAME",   # Supplier Name          🔴
    "CTYDP",           # Country of Departure   🟢  ISO2
    "CTYAR",           # Country of Destination 🟢  ISO2
    "VDECL_REQ_DATE",  # Request Date           🔴  YYYYMMDD or YYYY-MM-DD
    "VDECL_DEADLINE",  # Deadline               🔴  YYYYMMDD or YYYY-MM-DD
    "POO_STATUS",      # Proof of Origin Status 🔴  PENDING|RECEIVED|OVERDUE|VALIDATED
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
    for row in _sim.FIELD_DICTIONARY:
        sap, univ, prov, fmt, note = row[0], row[1], row[2], row[3], row[4]
        src = row[5] if len(row) > 5 else ""
        lines.append(f"# {sap},{univ},{prov},{fmt},{note}")
    return "\n".join(lines) + "\n"

# Full template with field dictionary appended (for download)
SHIPMENT_TEMPLATE_CSV_WITH_DICT = SHIPMENT_TEMPLATE_CSV + _field_ref_rows()

COO_TEMPLATE_CSV = """\
SUPPLIER_NAME,CTYDP,CTYAR,VDECL_REQ_DATE,VDECL_DEADLINE,POO_STATUS
Alpha Automotive MX,MX,US,20260701,20260715,OVERDUE
Viet Textiles JSC,VN,EU,20260705,20260722,PENDING
Seoul Electronics Co,KR,US,20260710,20260810,PENDING
Jakarta Metals PT,ID,JP,20260708,20260728,RECEIVED
Lima Copper SAC,PE,CA,20260718,20260818,VALIDATED
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

# ── Thread-safe in-memory state ───────────────────────────────────────────────

_lock = threading.Lock()
_state: dict[str, Any] = {
    # "simulated" | "uploaded"  — tracked independently per data type
    "shipment_mode":        "simulated",
    "coo_mode":             "simulated",
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
    if fn.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes))
    if fn.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(file_bytes))
    raise ValueError(f"Unsupported file type '{filename}'. Upload a .csv or .xlsx file.")


def _normalise_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from column names; preserve case (SAP columns are UPPERCASE)."""
    df = df.copy()
    df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]
    return df


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

    if "PREF_STATUS" in df.columns:
        valid_ps = {"E", "U", "N"}
        bad_ps = (
            set(df["PREF_STATUS"].dropna().str.strip().str.upper().unique()) - valid_ps
        )
        if bad_ps:
            warnings.append(
                f"Unrecognised PREF_STATUS values {bad_ps!r} — "
                "accepted: E (claimed), U (unclaimed), N (not-eligible). "
                "Unrecognised values treated as N."
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
    """Validate a normalised CoO DataFrame (SAP-native column names).
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

    if "POO_STATUS" in df.columns:
        valid_s = {"PENDING", "RECEIVED", "OVERDUE", "VALIDATED"}
        bad_s = (
            set(df["POO_STATUS"].dropna().str.strip().str.upper().unique()) - valid_s
        )
        if bad_s:
            warnings.append(
                f"Unrecognised POO_STATUS values {bad_s!r} — "
                "accepted: PENDING, RECEIVED, OVERDUE, VALIDATED. "
                "Unrecognised values treated as PENDING."
            )

    return len(errors) == 0, errors, warnings


# ── Data derivation: uploaded DataFrame → internal dicts ─────────────────────

def _derive_shipments(df: pd.DataFrame, has_roo: bool) -> list[dict]:
    """Map uploaded shipment DataFrame (SAP-native columns) to internal dict schema."""
    result: list[dict] = []

    for _, row in df.iterrows():
        raw_ps   = str(row.get("PREF_STATUS", "N")).strip().upper()
        pref_s   = raw_ps if raw_ps in ("E", "U", "N") else "N"

        cusval   = float(row.get("CUSVAL", 0) or 0)
        mfn      = float(row.get("MFN_RATE", 0) or 0)
        pref     = float(row.get("PREF_RATE", 0) or 0)

        duty_saving = (
            round(max(0.0, cusval * (mfn - pref) / 100), 2)
            if pref_s == "U" else 0.0
        )

        if has_roo:
            rvc_raw = row.get("RVC_PCT")
            thr_raw = row.get("RVC_THRESHOLD")
            if pd.notna(rvc_raw) and pd.notna(thr_raw):
                rvc = float(rvc_raw)
                thr = float(thr_raw)
                roo = _ro_status(rvc, thr)
            else:
                rvc, thr = 0.0, 0.0
                roo = "Q" if pref_s == "E" else "F"
        else:
            rvc, thr = 0.0, 0.0
            roo = "Q" if pref_s == "E" else "F"

        raw_date   = row.get("ENTRY_DATE", "")
        entry_date = _normalise_dats(raw_date) if pd.notna(raw_date) and raw_date != "" else "00000000"

        result.append({
            "TOR_ID":               str(row.get("TOR_ID", "—")),
            "TOR_ITEM":             str(row.get("TOR_ITEM", "000010")),
            "PRODUCT_ID":           str(row.get("PRODUCT_ID", "—")),
            "PRODUCT_TEXT":         str(row.get("PRODUCT_TEXT", "Unknown")),
            "CCNGN":                str(row.get("CCNGN", "—")),
            "CTYDP":                str(row.get("CTYDP", "—")),
            "CTYAR":                str(row.get("CTYAR", "—")),
            "CUCOO":                str(row.get("CUCOO", row.get("CTYDP", "—"))),
            "CUSVAL":               cusval,
            "WAERS":                str(row.get("WAERS", "USD")),
            "AGREEMENT":            str(row.get("AGREEMENT", "N/A")),
            "MFN_RATE":             mfn,
            "PREF_RATE":            pref,
            "PREF_STATUS":          pref_s,
            "RVC_PCT":              rvc,
            "RVC_THRESHOLD":        thr,
            "ROO_STATUS":           roo,
            "SUPPLIER_ID":          str(row.get("SUPPLIER_ID", "—")),
            "SUPPLIER_NAME":        str(row.get("SUPPLIER_NAME", "—")),
            "BOM_REGIONAL_CONTENT": float(row.get("BOM_REGIONAL_CONTENT", 0) or 0),
            "ENTRY_DATE":           entry_date,
            "duty_saving":          duty_saving,
        })

    return result


def _derive_lanes(df: pd.DataFrame) -> list[dict]:
    """
    Aggregate uploaded shipment rows into trade lanes (SAP-native field names).

    Lane key: (AGREEMENT, CTYDP, CTYAR).
    PREF_STATUS=N excluded from ELIGIBLE/CLAIMED totals.

    ELIGIBLE_VALUE  = Σ CUSVAL for PREF_STATUS in (E, U)
    CLAIMED_VALUE   = Σ CUSVAL for PREF_STATUS = E
    UNCLAIMED_SAVINGS_K = Σ duty_saving for PREF_STATUS = U  [in $K]
    """
    eligible_df = df[
        df["PREF_STATUS"].str.strip().str.upper().isin(["E", "U"])
    ].copy()

    lanes: list[dict] = []
    counter = 1

    for (fta, ctydp, ctyar), grp in eligible_df.groupby(
        ["AGREEMENT", "CTYDP", "CTYAR"], sort=False,
    ):
        total_val   = grp["CUSVAL"].sum()
        claimed_val = grp.loc[
            grp["PREF_STATUS"].str.strip().str.upper() == "E", "CUSVAL"
        ].sum()
        uncl_grp = grp[grp["PREF_STATUS"].str.strip().str.upper() == "U"]

        if total_val > 0:
            wt_mfn  = (grp["CUSVAL"] * grp["MFN_RATE"]).sum()  / total_val
            wt_pref = (grp["CUSVAL"] * grp["PREF_RATE"]).sum() / total_val
        else:
            wt_mfn = wt_pref = 0.0

        unclaimed_k = round(
            (uncl_grp["CUSVAL"] * (uncl_grp["MFN_RATE"] - uncl_grp["PREF_RATE"]) / 100).sum()
            / 1000,
            1,
        )
        util_pct = round(claimed_val / total_val * 100, 1) if total_val else 0.0

        lanes.append({
            "lane_id":            f"UL-{counter:03d}",
            "CTYDP":              str(ctydp),
            "CTYAR":              str(ctyar),
            "AGREEMENT":          str(fta),
            "ELIGIBLE_VALUE_M":   round(total_val   / 1_000_000, 3),
            "CLAIMED_VALUE_M":    round(claimed_val / 1_000_000, 3),
            "MFN_RATE":           round(wt_mfn,  3),
            "PREF_RATE":          round(wt_pref, 3),
            "UTILIZATION_PCT":    util_pct,
            "UNCLAIMED_SAVINGS_K": unclaimed_k,
            "retro_eligible_pct": 0,
            "retro_k":            0.0,
        })
        counter += 1

    lanes.sort(key=lambda x: x["UNCLAIMED_SAVINGS_K"], reverse=True)
    return lanes


def _derive_roo_assessments(df: pd.DataFrame) -> list[dict]:
    """
    Aggregate RoO status per (PRODUCT_TEXT, CCNGN, AGREEMENT) from uploaded data.
    Uses value-weighted average RVC_PCT; ROO_STATUS via same rule as fta_simulator.
    """
    result: list[dict] = []

    for (product, ccngn, fta), grp in df.groupby(
        ["PRODUCT_TEXT", "CCNGN", "AGREEMENT"], sort=False
    ):
        valid = grp[grp["RVC_PCT"].notna() & grp["RVC_THRESHOLD"].notna()]
        if valid.empty:
            continue

        v_sum   = valid["CUSVAL"].sum()
        avg_rvc = round(
            (valid["CUSVAL"] * valid["RVC_PCT"]).sum() / v_sum
            if v_sum else valid["RVC_PCT"].mean(),
            1,
        )
        threshold = float(valid["RVC_THRESHOLD"].mode().iloc[0])
        status    = _ro_status(avg_rvc, threshold)
        gap_pct   = round(max(0.0, threshold - avg_rvc), 1)

        status_labels = {"Q": "Qualified", "M": "Near-Miss", "F": "Fail"}
        if status == "Q":
            note = "Passes RVC_PCT threshold. Maintain CoO documentation."
        elif status == "M":
            note = (
                f"Within {gap_pct}% of RVC_THRESHOLD — "
                "minor BOM adjustments may qualify this product."
            )
        else:
            note = (
                f"RVC_PCT gap of {gap_pct}% below RVC_THRESHOLD — "
                "significant sourcing changes required."
            )

        if "SUPPLIER_NAME" in df.columns:
            suppliers = list(grp["SUPPLIER_NAME"].dropna().unique())[:3]
            if suppliers:
                note += f" Suppliers: {', '.join(str(s) for s in suppliers)}."

        result.append({
            "PRODUCT_TEXT":    str(product),
            "CCNGN":           str(ccngn),
            "AGREEMENT":       str(fta),
            "roo_test_type":   "Regional Value Content",
            "RVC_PCT":         avg_rvc,
            "RVC_THRESHOLD":   threshold,
            "ROO_STATUS":      status,
            "gap_pct":         gap_pct,
            "compliance_note": note,
        })

    return result


def _derive_qualification_roadmap(
    lanes: list[dict], df: pd.DataFrame, has_roadmap: bool
) -> list[dict]:
    """Generate qualification actions for under-utilised lanes (UTILIZATION_PCT < 75%).
    Uses SAP-native field names throughout.
    """
    roadmap: list[dict] = []

    for lane in lanes:
        if lane["UTILIZATION_PCT"] >= 75:
            continue

        fta      = lane["AGREEMENT"]
        pri, sec = _FTA_ACTIONS.get(fta, _FTA_ACTIONS_DEFAULT)

        if has_roadmap and not df.empty and "SUPPLIER_NAME" in df.columns:
            lane_uncl = df[
                (df["AGREEMENT"] == fta)
                & (df["CTYDP"] == lane["CTYDP"])
                & (df["CTYAR"] == lane["CTYAR"])
                & (df["PREF_STATUS"].str.strip().str.upper() == "U")
            ]
            suppliers = list(lane_uncl["SUPPLIER_NAME"].dropna().unique())[:2]
            if suppliers:
                sec = f"Priority suppliers: {', '.join(str(s) for s in suppliers)}. {sec}"

        gap = 100 - lane["UTILIZATION_PCT"]
        if gap > 40:
            effort, timeline = "High",   "6–10 weeks"
        elif gap > 20:
            effort, timeline = "Medium", "4–6 weeks"
        else:
            effort, timeline = "Low",    "2–3 weeks"

        ctydp_name = _CTRY.get(lane["CTYDP"], lane["CTYDP"])
        ctyar_name = _CTRY.get(lane["CTYAR"], lane["CTYAR"])

        roadmap.append({
            "lane_id":              lane["lane_id"],
            "CTYDP":                lane["CTYDP"],
            "CTYAR":                lane["CTYAR"],
            "lane_display":         f"{ctydp_name} → {ctyar_name}",
            "AGREEMENT":            fta,
            "UTILIZATION_PCT":      lane["UTILIZATION_PCT"],
            "UNCLAIMED_SAVINGS_K":  lane["UNCLAIMED_SAVINGS_K"],
            "primary_action":       pri,
            "secondary_action":     sec,
            "effort":               effort,
            "timeline":             timeline,
        })

    roadmap.sort(key=lambda x: x["UNCLAIMED_SAVINGS_K"], reverse=True)
    return roadmap


def _derive_period_label(df: pd.DataFrame) -> str:
    """Human-readable date range from the ENTRY_DATE column (accepts YYYYMMDD or YYYY-MM-DD)."""
    if "ENTRY_DATE" not in df.columns:
        return "Uploaded data"
    # Normalise to YYYY-MM-DD before parsing
    normalised = df["ENTRY_DATE"].apply(
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


def reset_to_simulated() -> None:
    """Reset both data types back to simulated mode."""
    with _lock:
        _state.update({
            "shipment_mode":        "simulated",
            "coo_mode":             "simulated",
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
    True when every column in SHIPMENT_REQUIRED is already present —
    i.e. the file already uses SAP field names and needs no mapping dialog.
    """
    return all(c in columns for c in SHIPMENT_REQUIRED)


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

    ok, errors, warnings, has_roo, has_roadmap = _validate_shipment(df_renamed)

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
        return _sim.get_fta_lanes()          # ← simulated (or future: erp)
    return _derive_lanes(df)


def get_fta_shipments() -> list:
    with _lock:
        mode    = _state["shipment_mode"]
        df      = _state["shipment_df"]
        has_roo = _state["shipment_has_roo"]
    if mode != "uploaded":
        return _sim.get_fta_shipments()
    return _derive_shipments(df, has_roo)


def get_coo_requests() -> list:
    with _lock:
        mode = _state["coo_mode"]
        df   = _state["coo_df"]
    if mode != "uploaded":
        return _sim.get_coo_requests()

    valid_status = {"PENDING", "RECEIVED", "OVERDUE", "VALIDATED"}
    result: list[dict] = []
    for _, row in df.iterrows():
        raw_s   = str(row.get("POO_STATUS", "PENDING")).strip().upper()
        poo_s   = raw_s if raw_s in valid_status else "PENDING"
        ctydp   = str(row.get("CTYDP", "—"))
        ctyar   = str(row.get("CTYAR", "—"))
        result.append({
            "SUPPLIER_ID":    str(row.get("SUPPLIER_ID",   "—")),
            "SUPPLIER_NAME":  str(row.get("SUPPLIER_NAME", "—")),
            "CTYDP":          ctydp,
            "CTYAR":          ctyar,
            "POO_TYPE":       str(row.get("POO_TYPE", "—")),
            "VDECL_REQ_DATE": _normalise_dats(row.get("VDECL_REQ_DATE", "")),
            "VDECL_DEADLINE": _normalise_dats(row.get("VDECL_DEADLINE", "")),
            "POO_STATUS":     poo_s,
        })
    return result


def get_fta_kpis() -> dict:
    """
    All four KPIs derived from the same lanes + CoO data the tables display.
    Guaranteed consistent regardless of source mode — no independent figures.

    Formula (identical to fta_simulator.get_fta_kpis):
        Utilization Rate  = Σ claimed_value_m / Σ eligible_value_m
        Unclaimed Oppty   = Σ lane.unclaimed_savings_k / 1000  (→ $M)
        Retroactive Claims= Σ lane.retro_k  (0 for uploaded — not in schema)
        CoOs Outstanding  = count of coo where status ∈ {pending,overdue,received}
    """
    with _lock:
        s_mode = _state["shipment_mode"]
        df     = _state["shipment_df"]

    if s_mode != "uploaded":
        return _sim.get_fta_kpis()

    lanes = get_fta_lanes()
    coos  = get_coo_requests()

    total_eligible_m  = sum(l["ELIGIBLE_VALUE_M"]    for l in lanes)
    total_claimed_m   = sum(l["CLAIMED_VALUE_M"]     for l in lanes)
    total_unclaimed_k = sum(l["UNCLAIMED_SAVINGS_K"] for l in lanes)
    coo_outstanding   = sum(1 for c in coos
                            if c["POO_STATUS"] in ("OVERDUE", "PENDING", "RECEIVED"))

    util_pct = (
        round(total_claimed_m / total_eligible_m * 100, 1)
        if total_eligible_m else 0.0
    )

    return {
        "utilization_pct":         util_pct,
        "unclaimed_opportunity_m":  round(total_unclaimed_k / 1_000, 2),
        "retroactive_claims_k":    0.0,   # retro eligibility not in upload schema
        "coo_outstanding":         coo_outstanding,
        "period_label":            _derive_period_label(df) if df is not None else "Uploaded data",
        "retro_window_label":      "Not available from uploaded data",
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
        return _sim.get_roo_assessments()

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
        return _sim.get_qualification_roadmap()

    lanes = get_fta_lanes()
    return _derive_qualification_roadmap(
        lanes,
        df if df is not None else pd.DataFrame(),
        has_roadmap,
    )
