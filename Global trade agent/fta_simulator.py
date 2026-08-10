# DEAD MODULE — intentionally unimported.
# The FTA agent uses aggregator rates + client upload only.
# Do not re-wire: fta_data_source.py routes all non-uploaded paths to clean empty
# states; there is no "simulated" mode. See fta_data_source.py for the live seam.

"""
FTA & Preferential Trade Agent — synthetic data simulator.
All values are fixed (deterministic) for reproducible UI rendering.

Shared time basis
-----------------
PERIOD_START / PERIOD_END define the trailing-12-month window used by every
aggregate figure (lane eligible/claimed values, unclaimed savings, KPIs).
Shipment entry dates all fall within this window.
The CoO Supplier Tracker is a live worklist of *current* outstanding requests
with future deadlines — it is NOT scoped to the historical period.
"""

# ── Shared time basis ────────────────────────────────────────────────────────
PERIOD_START = "2025-08-05"   # first day of the trailing-12-month window
PERIOD_END   = "2026-08-05"   # last day  (= today, illustrative)
PERIOD_LABEL = "T-12: Aug 2025 – Aug 2026"

# Customs retroactive-claim window: 12 months from entry date (same as the period)
RETRO_WINDOW_MONTHS = 12


def get_fta_kpis() -> dict:
    """All KPIs derived from the same underlying data as the tables.

    Time basis: all $ figures cover PERIOD_START – PERIOD_END (PERIOD_LABEL).

    Utilization Rate    = sum(claimed_value_m) / sum(eligible_value_m) — lanes, same period
    Unclaimed Oppty     = sum(unclaimed_savings_k)                      — lanes, same period
    Retroactive Claims  = sum(lane.retro_k) where retro_eligible_pct > 0
                          retro_k = unclaimed_savings_k × retro_eligible_pct / 100
                          retro_eligible_pct = share of that lane's unclaimed entries
                          whose entry dates fall within the 12-month retro-claim window
                          (RETRO_WINDOW_MONTHS from PERIOD_END), i.e. the full period.
                          Retro Claims is always a strict subset of Unclaimed Opportunity.
    CoOs Outstanding    = count of live requests with status ∈ {pending, overdue, received}
                          This is a current worklist — NOT scoped to the historical period.
    """
    lanes = get_fta_lanes()
    coos  = get_coo_requests()

    total_eligible_m   = sum(l["eligible_value_m"]   for l in lanes)
    total_claimed_m    = sum(l["claimed_value_m"]     for l in lanes)
    total_unclaimed_k  = sum(l["unclaimed_savings_k"] for l in lanes)
    total_retro_k      = round(sum(l["retro_k"]       for l in lanes), 1)
    coo_outstanding    = sum(1 for c in coos if c["status"] in ("pending", "overdue", "received"))

    return {
        "utilization_pct":         round(total_claimed_m / total_eligible_m * 100, 1),
        "unclaimed_opportunity_m":  round(total_unclaimed_k / 1000, 1),
        "retroactive_claims_k":    total_retro_k,
        "coo_outstanding":         coo_outstanding,
        # Time-basis labels for display
        "period_label":            PERIOD_LABEL,
        "retro_window_label":      f"{RETRO_WINDOW_MONTHS}-month retro window",
    }


def get_fta_lanes() -> list:
    """
    Return 11 trade lanes with FTA eligibility and utilization metrics.
    Pre-sorted descending by unclaimed_savings_k.
    """
    lanes = [
        {
            "lane_id": "LN-001",
            "origin": "Mexico",
            "destination": "USA",
            "fta_name": "USMCA",
            "eligible_value_m": 12.4,
            "claimed_value_m": 7.8,
            "utilization_pct": 62.9,
            "unclaimed_savings_k": 285,
            "mfn_rate_pct": 7.5,
            "preferential_rate_pct": 0.0,
            # Representative HS for aggregator query. One code per lane — see FIX-3 note
            # in fta_data_source._enrich_lanes_from_aggregator().
            "representative_lane": {"hs_code": "8708.29", "origin": "MX", "destination": "US"},
        },
        {
            "lane_id": "LN-002",
            "origin": "Vietnam",
            "destination": "EU",
            "fta_name": "EVFTA",
            "eligible_value_m": 9.8,
            "claimed_value_m": 5.6,
            "utilization_pct": 57.1,
            "unclaimed_savings_k": 248,
            "mfn_rate_pct": 12.0,
            "preferential_rate_pct": 0.0,
            # EU is not a single ISO-3166 country; DE used as representative EU market.
            "representative_lane": {"hs_code": "8542.31", "origin": "VN", "destination": "DE"},
        },
        {
            "lane_id": "LN-003",
            "origin": "Vietnam",
            "destination": "Japan",
            "fta_name": "RCEP",
            "eligible_value_m": 6.9,
            "claimed_value_m": 4.2,
            "utilization_pct": 60.9,
            "unclaimed_savings_k": 214,
            "mfn_rate_pct": 9.0,
            "preferential_rate_pct": 2.5,
            "representative_lane": {"hs_code": "6105.10", "origin": "VN", "destination": "JP"},
        },
        {
            "lane_id": "LN-004",
            "origin": "Korea",
            "destination": "USA",
            "fta_name": "KORUS",
            "eligible_value_m": 8.3,
            "claimed_value_m": 5.9,
            "utilization_pct": 71.1,
            "unclaimed_savings_k": 195,
            "mfn_rate_pct": 6.5,
            "preferential_rate_pct": 0.0,
            # DemoConnector has data for 0406.90 KR→US (dairy/KORUS): MFN 7.2%, pref 0.0%.
            # In production replace with the lane's highest-volume HS code.
            "representative_lane": {"hs_code": "0406.90", "origin": "KR", "destination": "US"},
        },
        {
            "lane_id": "LN-005",
            "origin": "Korea",
            "destination": "Germany",
            "fta_name": "EU-Korea FTA",
            "eligible_value_m": 4.8,
            "claimed_value_m": 3.2,
            "utilization_pct": 66.7,
            "unclaimed_savings_k": 176,
            "mfn_rate_pct": 6.5,
            "preferential_rate_pct": 0.0,
            "representative_lane": {"hs_code": "8528.72", "origin": "KR", "destination": "DE"},
        },
        {
            "lane_id": "LN-006",
            "origin": "Canada",
            "destination": "USA",
            "fta_name": "USMCA",
            "eligible_value_m": 7.6,
            "claimed_value_m": 6.1,
            "utilization_pct": 80.3,
            "unclaimed_savings_k": 148,
            "mfn_rate_pct": 5.0,
            "preferential_rate_pct": 0.0,
            "representative_lane": {"hs_code": "8483.10", "origin": "CA", "destination": "US"},
        },
        {
            "lane_id": "LN-007",
            "origin": "Indonesia",
            "destination": "Japan",
            "fta_name": "RCEP",
            "eligible_value_m": 5.4,
            "claimed_value_m": 3.8,
            "utilization_pct": 70.4,
            "unclaimed_savings_k": 120,
            "mfn_rate_pct": 8.0,
            "preferential_rate_pct": 2.0,
            "representative_lane": {"hs_code": "2921.41", "origin": "ID", "destination": "JP"},
        },
        {
            "lane_id": "LN-008",
            "origin": "USA",
            "destination": "Australia",
            "fta_name": "US-AUS FTA",
            "eligible_value_m": 3.9,
            "claimed_value_m": 2.8,
            "utilization_pct": 71.8,
            "unclaimed_savings_k": 96,
            "mfn_rate_pct": 5.0,
            "preferential_rate_pct": 0.0,
            "representative_lane": {"hs_code": "8471.30", "origin": "US", "destination": "AU"},
        },
        {
            "lane_id": "LN-009",
            "origin": "Malaysia",
            "destination": "Japan",
            "fta_name": "CPTPP",
            "eligible_value_m": 5.1,
            "claimed_value_m": 4.1,
            "utilization_pct": 80.4,
            "unclaimed_savings_k": 88,
            "mfn_rate_pct": 8.0,
            "preferential_rate_pct": 0.0,
            "representative_lane": {"hs_code": "0304.62", "origin": "MY", "destination": "JP"},
        },
        {
            "lane_id": "LN-010",
            "origin": "Peru",
            "destination": "Canada",
            "fta_name": "CPTPP",
            "eligible_value_m": 3.2,
            "claimed_value_m": 2.1,
            "utilization_pct": 65.6,
            "unclaimed_savings_k": 78,
            "mfn_rate_pct": 9.5,
            "preferential_rate_pct": 0.0,
            "representative_lane": {"hs_code": "7408.11", "origin": "PE", "destination": "CA"},
        },
        {
            "lane_id": "LN-011",
            "origin": "Guatemala",
            "destination": "USA",
            "fta_name": "CAFTA-DR",
            "eligible_value_m": 4.2,
            "claimed_value_m": 3.5,
            "utilization_pct": 83.3,
            "unclaimed_savings_k": 54,
            "mfn_rate_pct": 6.0,
            "preferential_rate_pct": 0.0,
            "representative_lane": {"hs_code": "0709.30", "origin": "GT", "destination": "US"},
        },
    ]
    # Derive unclaimed savings from first principles; sort largest gap first.
    # Formula: (eligible - claimed) × (MFN rate − preferential rate)
    #
    # retro_eligible_pct: share of a lane's unclaimed savings that falls within
    # the 12-month retroactive-claim window (0 = lane too new / no prior entries
    # in window; higher = more historical unclaimed entries available to amend).
    _retro_pct = {
        "LN-001": 15,   # USMCA MX→USA  — active lane, moderate retro pool
        "LN-002": 18,   # EVFTA VN→EU   — large volume, 18 months of entries
        "LN-003": 25,   # RCEP  VN→JP   — RCEP newer, 25% of gap in window
        "LN-004": 30,   # KORUS KR→USA  — KORUS mature, Q4–Q1 entries claimable
        "LN-005": 20,   # EU-Korea KR→DE — some retro entries available
        "LN-006": 15,   # USMCA CA→USA  — small unclaimed, some retro eligible
        "LN-007": 30,   # RCEP  ID→JP   — significant retro window
        "LN-008": 25,   # US-AUS        — some prior-year entries unclaimed
        "LN-009":  0,   # CPTPP MY→JP   — claims already filed retroactively
        "LN-010":  0,   # CPTPP PE→CA   — lane too recent for retro window
        "LN-011":  0,   # CAFTA-DR GT→USA — entries exceed retro period
    }
    for lane in lanes:
        lane["unclaimed_savings_k"] = round(
            (lane["eligible_value_m"] - lane["claimed_value_m"]) * 1000
            * (lane["mfn_rate_pct"] - lane["preferential_rate_pct"]) / 100,
            1,
        )
        lane["retro_eligible_pct"] = _retro_pct.get(lane["lane_id"], 0)
        lane["retro_k"] = round(
            lane["unclaimed_savings_k"] * lane["retro_eligible_pct"] / 100, 1
        )
    lanes.sort(key=lambda x: x["unclaimed_savings_k"], reverse=True)
    return lanes


def get_fta_shipments() -> list:
    """
    Return 18 shipments covering all three eligibility and RoO statuses.

    All entry_date values fall within PERIOD_START – PERIOD_END (PERIOD_LABEL).
    Eligible-unclaimed entries are dated in the last 3–4 months of the period
    (most actionable); claimed and not-eligible entries are spread across the year.

    ro_status is DERIVED from rvc_pct vs rvc_threshold_pct — never hardcoded:
        qualified  = rvc_pct >= rvc_threshold_pct
        near-miss  = rvc_threshold_pct - 5 <= rvc_pct < rvc_threshold_pct
        fail       = rvc_pct < rvc_threshold_pct - 5

    RVC thresholds used (illustrative):
        USMCA 60%, EVFTA 40%, RCEP 40% (general) / 60% (Chapter 29 chemicals),
        KORUS 45%, EU-Korea 55%, CPTPP 40%, CAFTA-DR 35%, N/A 40% (notional).
    """
    shipments = [
        # ── eligible-unclaimed ─────────────────────────────────────────────
        {
            "shipment_id": "SHP-001",
            "entry_date": "2026-06-12",
            "product": "Automotive Parts",
            "hs_code": "8708.29",
            "origin": "Mexico",
            "destination": "USA",
            "value_k": 245.5,
            "fta_name": "USMCA",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 18.4,
            "rvc_threshold_pct": 60,
            "rvc_pct": 68,   # 68 >= 60 → qualified
        },
        {
            "shipment_id": "SHP-002",
            "entry_date": "2026-07-03",
            "product": "Electronic Components",
            "hs_code": "8542.31",
            "origin": "Vietnam",
            "destination": "EU",
            "value_k": 312.8,
            "fta_name": "EVFTA",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 37.5,
            "rvc_threshold_pct": 40,
            "rvc_pct": 72,   # 72 >= 40 → qualified
        },
        {
            "shipment_id": "SHP-003",
            "entry_date": "2026-05-20",
            "product": "Cotton Shirts",
            "hs_code": "6105.10",
            "origin": "Vietnam",
            "destination": "Japan",
            "value_k": 87.6,
            "fta_name": "RCEP",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 7.9,
            "rvc_threshold_pct": 40,
            "rvc_pct": 37,   # 35 <= 37 < 40 → near-miss
        },
        {
            "shipment_id": "SHP-004",
            "entry_date": "2026-07-15",
            "product": "Steel Tubes",
            "hs_code": "7304.31",
            "origin": "Korea",
            "destination": "USA",
            "value_k": 198.4,
            "fta_name": "KORUS",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 12.9,
            "rvc_threshold_pct": 45,
            "rvc_pct": 65,   # 65 >= 45 → qualified
        },
        {
            "shipment_id": "SHP-005",
            "entry_date": "2026-06-28",
            "product": "Machinery Parts",
            "hs_code": "8483.10",
            "origin": "Canada",
            "destination": "USA",
            "value_k": 156.2,
            "fta_name": "USMCA",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 7.8,
            "rvc_threshold_pct": 60,
            "rvc_pct": 57,   # 55 <= 57 < 60 → near-miss
        },
        {
            "shipment_id": "SHP-006",
            "entry_date": "2026-07-22",
            "product": "Electronic Displays",
            "hs_code": "8528.72",
            "origin": "Korea",
            "destination": "Germany",
            "value_k": 278.3,
            "fta_name": "EU-Korea FTA",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 18.1,
            "rvc_threshold_pct": 55,
            "rvc_pct": 74,   # 74 >= 55 → qualified
        },
        {
            "shipment_id": "SHP-007",
            "entry_date": "2026-06-05",
            "product": "Aluminum Rods",
            "hs_code": "7604.10",
            "origin": "Mexico",
            "destination": "USA",
            "value_k": 92.4,
            "fta_name": "USMCA",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 6.9,
            "rvc_threshold_pct": 60,
            "rvc_pct": 56,   # 55 <= 56 < 60 → near-miss
        },
        {
            "shipment_id": "SHP-008",
            "entry_date": "2026-07-30",
            "product": "Woven Apparel",
            "hs_code": "6203.42",
            "origin": "Vietnam",
            "destination": "EU",
            "value_k": 68.9,
            "fta_name": "EVFTA",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 8.3,
            "rvc_threshold_pct": 40,
            "rvc_pct": 61,   # 61 >= 40 → qualified
        },
        # ── claimed ────────────────────────────────────────────────────────
        {
            "shipment_id": "SHP-009",
            "entry_date": "2026-03-14",
            "product": "PCB Assemblies",
            "hs_code": "8534.00",
            "origin": "Korea",
            "destination": "USA",
            "value_k": 445.6,
            "fta_name": "KORUS",
            "eligibility": "claimed",
            "est_saving_k": 29.0,
            "rvc_threshold_pct": 45,
            "rvc_pct": 78,   # 78 >= 45 → qualified
        },
        {
            "shipment_id": "SHP-010",
            "entry_date": "2026-04-08",
            "product": "Auto Wiring Harness",
            "hs_code": "8544.30",
            "origin": "Mexico",
            "destination": "USA",
            "value_k": 389.2,
            "fta_name": "USMCA",
            "eligibility": "claimed",
            "est_saving_k": 29.2,
            "rvc_threshold_pct": 60,
            "rvc_pct": 82,   # 82 >= 60 → qualified
        },
        {
            "shipment_id": "SHP-011",
            "entry_date": "2025-11-20",
            "product": "Seafood Products",
            "hs_code": "0304.62",
            "origin": "Malaysia",
            "destination": "Japan",
            "value_k": 134.8,
            "fta_name": "CPTPP",
            "eligibility": "claimed",
            "est_saving_k": 8.1,
            "rvc_threshold_pct": 40,
            "rvc_pct": 70,   # 70 >= 40 → qualified
        },
        {
            "shipment_id": "SHP-012",
            "entry_date": "2025-12-10",
            "product": "Agricultural Produce",
            "hs_code": "0709.30",
            "origin": "Guatemala",
            "destination": "USA",
            "value_k": 78.4,
            "fta_name": "CAFTA-DR",
            "eligibility": "claimed",
            "est_saving_k": 4.7,
            "rvc_threshold_pct": 35,
            "rvc_pct": 85,   # 85 >= 35 → qualified
        },
        {
            "shipment_id": "SHP-013",
            "entry_date": "2026-01-15",
            "product": "Chemical Compounds",
            "hs_code": "2921.41",
            "origin": "Indonesia",
            "destination": "Japan",
            "value_k": 167.3,
            "fta_name": "RCEP",
            "eligibility": "claimed",
            "est_saving_k": 13.4,
            "rvc_threshold_pct": 60,  # RCEP Ch.29 chemicals require 60% RVC
            "rvc_pct": 58,   # 55 <= 58 < 60 → near-miss
        },
        {
            "shipment_id": "SHP-014",
            "entry_date": "2026-02-28",
            "product": "Copper Wire",
            "hs_code": "7408.11",
            "origin": "Peru",
            "destination": "Canada",
            "value_k": 112.6,
            "fta_name": "CPTPP",
            "eligibility": "claimed",
            "est_saving_k": 10.7,
            "rvc_threshold_pct": 40,
            "rvc_pct": 67,   # 67 >= 40 → qualified
        },
        # ── not-eligible ───────────────────────────────────────────────────
        {
            "shipment_id": "SHP-015",
            "entry_date": "2026-05-05",
            "product": "Rare Earth Magnets",
            "hs_code": "8505.11",
            "origin": "China",
            "destination": "USA",
            "value_k": 234.7,
            "fta_name": "N/A",
            "eligibility": "not-eligible",
            "est_saving_k": 0.0,
            "rvc_threshold_pct": 40,
            "rvc_pct": 22,   # 22 < 35 → fail
        },
        {
            "shipment_id": "SHP-016",
            "entry_date": "2026-04-22",
            "product": "Pharmaceuticals",
            "hs_code": "3004.90",
            "origin": "India",
            "destination": "EU",
            "value_k": 189.4,
            "fta_name": "N/A",
            "eligibility": "not-eligible",
            "est_saving_k": 0.0,
            "rvc_threshold_pct": 40,
            "rvc_pct": 18,   # 18 < 35 → fail
        },
        {
            "shipment_id": "SHP-017",
            "entry_date": "2025-09-18",
            "product": "Rubber Products",
            "hs_code": "4016.99",
            "origin": "Thailand",
            "destination": "USA",
            "value_k": 145.8,
            "fta_name": "N/A",
            "eligibility": "not-eligible",
            "est_saving_k": 0.0,
            "rvc_threshold_pct": 40,
            "rvc_pct": 28,   # 28 < 35 → fail
        },
        {
            "shipment_id": "SHP-018",
            "entry_date": "2025-10-14",
            "product": "Luxury Watches",
            "hs_code": "9102.19",
            "origin": "Switzerland",
            "destination": "USA",
            "value_k": 678.2,
            "fta_name": "N/A",
            "eligibility": "not-eligible",
            "est_saving_k": 0.0,
            "rvc_threshold_pct": 40,
            "rvc_pct": 12,   # 12 < 35 → fail
        },
    ]
    # Derive ro_status from RVC vs threshold — single source of truth.
    for s in shipments:
        rvc = s["rvc_pct"]
        thr = s["rvc_threshold_pct"]
        if rvc >= thr:
            s["ro_status"] = "qualified"
        elif rvc >= thr - 5:
            s["ro_status"] = "near-miss"
        else:
            s["ro_status"] = "fail"
    return shipments


def get_coo_requests() -> list:
    """
    Return 9 Certificate of Origin supplier requests.
    Statuses: 2 overdue, 3 pending, 2 received, 2 validated.
    """
    return [
        {
            "supplier": "Alpha Automotive MX",
            "lane": "Mexico → USA",
            "request_date": "2026-07-01",
            "deadline": "2026-07-15",
            "status": "overdue",
        },
        {
            "supplier": "Viet Textiles JSC",
            "lane": "Vietnam → EU",
            "request_date": "2026-07-05",
            "deadline": "2026-07-22",
            "status": "overdue",
        },
        {
            "supplier": "Seoul Electronics Co",
            "lane": "Korea → USA",
            "request_date": "2026-07-10",
            "deadline": "2026-08-10",
            "status": "pending",
        },
        {
            "supplier": "Hanoi Apparel JSC",
            "lane": "Vietnam → Japan",
            "request_date": "2026-07-12",
            "deadline": "2026-08-05",
            "status": "pending",
        },
        {
            "supplier": "Toronto Parts Inc",
            "lane": "Canada → USA",
            "request_date": "2026-07-15",
            "deadline": "2026-08-15",
            "status": "pending",
        },
        {
            "supplier": "Jakarta Metals PT",
            "lane": "Indonesia → Japan",
            "request_date": "2026-07-08",
            "deadline": "2026-07-28",
            "status": "received",
        },
        {
            "supplier": "KL Seafood Bhd",
            "lane": "Malaysia → Japan",
            "request_date": "2026-07-10",
            "deadline": "2026-07-30",
            "status": "received",
        },
        {
            "supplier": "Lima Copper SAC",
            "lane": "Peru → Canada",
            "request_date": "2026-07-18",
            "deadline": "2026-08-18",
            "status": "validated",
        },
        {
            "supplier": "Guatemala Agro SA",
            "lane": "Guatemala → USA",
            "request_date": "2026-07-20",
            "deadline": "2026-08-20",
            "status": "validated",
        },
    ]


# ── RoO derivation helper (shared rule) ─────────────────────────────────────

def _derive_ro_status(rvc_pct: int, rvc_threshold_pct: int) -> str:
    """qualified / near-miss / fail — identical rule used in get_fta_shipments."""
    if rvc_pct >= rvc_threshold_pct:
        return "qualified"
    if rvc_pct >= rvc_threshold_pct - 5:
        return "near-miss"
    return "fail"


def get_roo_assessments() -> list:
    """
    Per-product Rules-of-Origin compliance view for key products.

    RVC values for products that also appear in get_fta_shipments() use the
    identical figures — cross-check: SHP-001 → row 0, SHP-005 → row 1,
    SHP-007 → row 2, SHP-002 → row 3, SHP-003 → row 4, SHP-008 → row 5,
    SHP-004 → row 6, SHP-013 → row 7, SHP-006 → row 8, SHP-014 → row 9.

    ro_status and gap_pct are derived — never hardcoded.
    """
    products = [
        # ── USMCA ───────────────────────────────────────────────────────────
        {
            "product": "Automotive Parts",
            "hs_code": "8708.29",
            "fta_name": "USMCA",
            "roo_test_type": "Regional Value Content",
            "rvc_threshold_pct": 60,
            "rvc_pct": 68,       # SHP-001
            "compliance_note": "Passes net-cost RVC. CoO documentation current.",
        },
        {
            "product": "Machinery Parts",
            "hs_code": "8483.10",
            "fta_name": "USMCA",
            "roo_test_type": "Regional Value Content",
            "rvc_threshold_pct": 60,
            "rvc_pct": 57,       # SHP-005
            "compliance_note": "3 pts below threshold. Review non-originating input costs.",
        },
        {
            "product": "Aluminum Rods",
            "hs_code": "7604.10",
            "fta_name": "USMCA",
            "roo_test_type": "Regional Value Content",
            "rvc_threshold_pct": 60,
            "rvc_pct": 56,       # SHP-007
            "compliance_note": "4 pts below threshold. Shift billet to domestic supplier to close gap.",
        },
        # ── EVFTA ───────────────────────────────────────────────────────────
        {
            "product": "Electronic Components",
            "hs_code": "8542.31",
            "fta_name": "EVFTA",
            "roo_test_type": "Tariff Classification Change",
            "rvc_threshold_pct": 40,
            "rvc_pct": 72,       # SHP-002
            "compliance_note": "CTC and value-add rule both satisfied. CoO received.",
        },
        {
            "product": "Woven Apparel",
            "hs_code": "6203.42",
            "fta_name": "EVFTA",
            "roo_test_type": "Specific Process (Double Transformation)",
            "rvc_threshold_pct": 40,
            "rvc_pct": 61,       # SHP-008
            "compliance_note": "Spinning and weaving both performed in Vietnam. EVFTA Form A issued.",
        },
        # ── RCEP ────────────────────────────────────────────────────────────
        {
            "product": "Cotton Shirts",
            "hs_code": "6105.10",
            "fta_name": "RCEP",
            "roo_test_type": "Regional Value Content",
            "rvc_threshold_pct": 40,
            "rvc_pct": 37,       # SHP-003
            "compliance_note": "3 pts short. Shift fabric sourcing from China to Vietnamese mills.",
        },
        {
            "product": "Chemical Compounds",
            "hs_code": "2921.41",
            "fta_name": "RCEP",
            "roo_test_type": "Regional Value Content",
            "rvc_threshold_pct": 60,  # Chapter 29 chemicals require 60%
            "rvc_pct": 58,       # SHP-013
            "compliance_note": "Ch.29 requires 60% RVC. 2 pts short — review reagent sourcing mix.",
        },
        # ── KORUS ───────────────────────────────────────────────────────────
        {
            "product": "Steel Tubes",
            "hs_code": "7304.31",
            "fta_name": "KORUS",
            "roo_test_type": "Regional Value Content",
            "rvc_threshold_pct": 45,
            "rvc_pct": 65,       # SHP-004
            "compliance_note": "20 pts above threshold. Routine CoO renewal due Q4 2026.",
        },
        # ── EU-Korea FTA ────────────────────────────────────────────────────
        {
            "product": "Electronic Displays",
            "hs_code": "8528.72",
            "fta_name": "EU-Korea FTA",
            "roo_test_type": "Specific Process + RVC",
            "rvc_threshold_pct": 55,
            "rvc_pct": 74,       # SHP-006
            "compliance_note": "Specific manufacturing ops satisfied; RVC 74% > 55%. Fully compliant.",
        },
        # ── CPTPP ───────────────────────────────────────────────────────────
        {
            "product": "Copper Wire",
            "hs_code": "7408.11",
            "fta_name": "CPTPP",
            "roo_test_type": "Tariff Classification Change",
            "rvc_threshold_pct": 40,
            "rvc_pct": 67,       # SHP-014
            "compliance_note": "Heading shift Ch.26 (ore) → Ch.74 (wire) confirmed. Compliant.",
        },
    ]
    for p in products:
        p["ro_status"] = _derive_ro_status(p["rvc_pct"], p["rvc_threshold_pct"])
        p["gap_pct"]   = max(0, p["rvc_threshold_pct"] - p["rvc_pct"])
    return products


def get_qualification_roadmap() -> list:
    """
    Forward-looking recommended actions for under-utilised lanes (<75% utilization).

    Savings figures are pulled from get_fta_lanes() at call time — the roadmap
    never hardcodes a dollar figure that could drift from the lane table.
    """
    _metadata = {
        "LN-002": {
            "primary_action": "Enforce double-transformation CoO for EVFTA textiles",
            "secondary_action": "File retroactive claims for Q1–Q2 unclaimed shipments",
            "effort": "Medium",
            "timeline": "4–6 weeks",
        },
        "LN-001": {
            "primary_action": "Complete USMCA CoO renewals for 4 overdue suppliers",
            "secondary_action": "Qualify near-miss products by shifting to USMCA-origin billet",
            "effort": "Medium",
            "timeline": "3–5 weeks",
        },
        "LN-003": {
            "primary_action": "Shift fabric sourcing to Vietnamese mills to meet RCEP 40% RVC",
            "secondary_action": "Request updated CoO from Viet Textiles JSC (currently overdue)",
            "effort": "High",
            "timeline": "6–10 weeks",
        },
        "LN-004": {
            "primary_action": "Renew KORUS certificates for 3 electronics suppliers",
            "secondary_action": "Verify HS sub-classification for steel tube product lines",
            "effort": "Low",
            "timeline": "2–3 weeks",
        },
        "LN-010": {
            "primary_action": "Confirm CPTPP tariff-shift documentation for copper wire",
            "secondary_action": "File retroactive claim via amended entry for Q2 shipments",
            "effort": "Low",
            "timeline": "2–4 weeks",
        },
        "LN-005": {
            "primary_action": "Complete EU-Korea specific-process attestation for displays",
            "secondary_action": "Engage Korean manufacturer on systematic RVC tracking",
            "effort": "Medium",
            "timeline": "4–6 weeks",
        },
        "LN-007": {
            "primary_action": "Validate RCEP originating-goods declaration for Indonesian metals",
            "secondary_action": "Instruct ASEAN customs broker to apply RCEP tariff line at entry",
            "effort": "Low",
            "timeline": "2–3 weeks",
        },
        "LN-008": {
            "primary_action": "Submit US-AUS FTA tariff concession orders for machinery exports",
            "secondary_action": "Identify additional product lines eligible under AUSFTA Schedule",
            "effort": "Medium",
            "timeline": "3–5 weeks",
        },
    }
    roadmap = []
    for lane in get_fta_lanes():
        if lane["utilization_pct"] < 75 and lane["lane_id"] in _metadata:
            entry = {
                "lane_id":             lane["lane_id"],
                "lane":                f'{lane["origin"]} → {lane["destination"]}',
                "fta_name":            lane["fta_name"],
                "utilization_pct":     lane["utilization_pct"],
                "unclaimed_savings_k": lane["unclaimed_savings_k"],  # from computed lane data
            }
            entry.update(_metadata[lane["lane_id"]])
            roadmap.append(entry)
    roadmap.sort(key=lambda x: x["unclaimed_savings_k"], reverse=True)
    return roadmap
