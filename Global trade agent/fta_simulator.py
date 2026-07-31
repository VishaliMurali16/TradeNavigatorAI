"""
FTA & Preferential Trade Agent — synthetic data simulator.
All values are fixed (deterministic) for reproducible UI rendering.
"""


def get_fta_kpis() -> dict:
    """Return top-level KPIs for the FTA utilization dashboard.

    unclaimed_opportunity_m is summed from the lane-level formula so it always
    matches the Utilization Gap table.
    """
    total_k = sum(l["unclaimed_savings_k"] for l in get_fta_lanes())
    return {
        "utilization_pct": 61,
        "unclaimed_opportunity_m": round(total_k / 1000, 1),
        "retroactive_claims_k": 312,
        "coo_pending": 7,
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
        },
    ]
    # Derive unclaimed savings from first principles; sort largest gap first.
    # Formula: (eligible - claimed) × (MFN rate − preferential rate)
    for lane in lanes:
        lane["unclaimed_savings_k"] = round(
            (lane["eligible_value_m"] - lane["claimed_value_m"]) * 1000
            * (lane["mfn_rate_pct"] - lane["preferential_rate_pct"]) / 100,
            1,
        )
    lanes.sort(key=lambda x: x["unclaimed_savings_k"], reverse=True)
    return lanes


def get_fta_shipments() -> list:
    """
    Return 18 shipments covering all three eligibility and RoO statuses.

    ro_status is DERIVED from rvc_pct vs rvc_threshold_pct — never hardcoded:
        qualified  = rvc_pct >= rvc_threshold_pct
        near-miss  = rvc_threshold_pct - 5 <= rvc_pct < rvc_threshold_pct
        fail       = rvc_pct < rvc_threshold_pct - 5

    RVC thresholds used (illustrative):
        USMCA 60%, EVFTA 40%, RCEP 40% (general) / 60% (Chapter 29 chemicals),
        KORUS 45%, EU-Korea 55%, CPTPP 40%, CAFTA-DR 35%, N/A 40% (notional).
    """
    shipments = [
        # ── eligible-unclaimed ──────────────────────────────────────────
        {
            "shipment_id": "SHP-001",
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
            "product": "Cotton Shirts",
            "hs_code": "6105.10",
            "origin": "Vietnam",
            "destination": "Japan",
            "value_k": 87.6,
            "fta_name": "RCEP",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 7.9,
            "rvc_threshold_pct": 40,
            "rvc_pct": 37,   # 35 <= 37 < 40 → near-miss (was 41: above threshold, wrong)
        },
        {
            "shipment_id": "SHP-004",
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
            "product": "Aluminum Rods",
            "hs_code": "7604.10",
            "origin": "Mexico",
            "destination": "USA",
            "value_k": 92.4,
            "fta_name": "USMCA",
            "eligibility": "eligible-unclaimed",
            "est_saving_k": 6.9,
            "rvc_threshold_pct": 60,
            "rvc_pct": 56,   # 55 <= 56 < 60 → near-miss (was 48: below threshold-5, wrong)
        },
        {
            "shipment_id": "SHP-008",
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
        # ── claimed ──────────────────────────────────────────────────────
        {
            "shipment_id": "SHP-009",
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
            "product": "Chemical Compounds",
            "hs_code": "2921.41",
            "origin": "Indonesia",
            "destination": "Japan",
            "value_k": 167.3,
            "fta_name": "RCEP",
            "eligibility": "claimed",
            "est_saving_k": 13.4,
            "rvc_threshold_pct": 60,  # RCEP Ch.29 chemicals require 60% RVC
            "rvc_pct": 58,   # 55 <= 58 < 60 → near-miss (was threshold=40: wrong, 58 would qualify)
        },
        {
            "shipment_id": "SHP-014",
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
        # ── not-eligible ─────────────────────────────────────────────────
        {
            "shipment_id": "SHP-015",
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
            "product": "Rubber Products",
            "hs_code": "4016.99",
            "origin": "Thailand",
            "destination": "USA",
            "value_k": 145.8,
            "fta_name": "N/A",
            "eligibility": "not-eligible",
            "est_saving_k": 0.0,
            "rvc_threshold_pct": 40,
            "rvc_pct": 28,   # 28 < 35 → fail (was 35: on near-miss boundary, ambiguous)
        },
        {
            "shipment_id": "SHP-018",
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
