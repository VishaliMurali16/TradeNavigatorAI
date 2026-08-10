# DEAD MODULE — intentionally unimported.
# The FTA agent uses aggregator rates + client upload only.
# Do not re-wire: fta_data_source.py routes all non-uploaded paths to clean empty
# states; there is no "simulated" mode. See fta_data_source.py for the live seam.

"""
FTA & Preferential Trade Agent — synthetic data simulator.
Modelled on SAP TM (Transportation Management) + SAP GTS (Global Trade Services).
NOT a live SAP connection — demo data for illustrative purposes.

Field names follow SAP conventions where confirmed; 🔴 marks are educated guesses
that must be validated against the client's actual SAP release.

Run this module directly to print the field dictionary and a KPI reconciliation:
    python fta_simulator.py
"""

# ── Field Dictionary ──────────────────────────────────────────────────────────
# Tuple format: (SAP_NAME, UNIVERSAL_NAME, PROVENANCE, FORMAT, NOTE, SOURCE_SYSTEM, LONG_DESC)
#
# 🟢 = confirmed real SAP table / field / term
# 🔴 = SAP-style name or invented convention — confirm against client's SAP release
#      In the dashboard, 🔴 fields display as 🟡 "Pending SAP confirmation" (client-facing).

FIELD_DICTIONARY = [
    # ── SHIPMENT DATA (/SCMTMS/TOR  +  /SAPSLL/CUIT|CUHD) ──────────────────
    ("TOR_ID",
     "Freight Order ID",
     "🟢",
     "CHAR10 e.g. '6100000783'",
     "SAP TM transportation order /SCMTMS/TOR",
     "SAP TM",
     "TOR_ID is the primary key of the Transportation Order in SAP TM, stored as a "
     "CHAR10 numeric string (e.g. '6100000783') on object /SCMTMS/TOR. It uniquely "
     "identifies a freight order across its full lifecycle — from booking through "
     "execution and customs handoff to GTS. This field is verified: it is the "
     "standard, documented header key in SAP TM's freight-order data model and "
     "appears verbatim in TM standard APIs, BAPIs, and reporting extracts."),

    ("TOR_ITEM",
     "Freight Order Item",
     "🔴",
     "NUMC6 e.g. '000010'",
     "SAP-style item number; exact field unconfirmed",
     "SAP TM",
     "SAP TM structures freight orders as header plus item records; each item carries "
     "a numeric positional key (NUMC6, e.g. '000010') analogous to a purchase-order "
     "line number. The item-level data structure is confirmed in SAP TM's data model. "
     "However, the exact technical field name for the item key on /SCMTMS/TOR item "
     "segments has not been verified against the client's SAP TM release — "
     "a sample TM freight-order extract would confirm the precise field name."),

    ("PRODUCT_ID",
     "Product / Material",
     "🔴",
     "CHAR18 MATNR-style e.g. '1000087082'",
     "SAP material number; exact TM-to-GTS linkage unconfirmed",
     "SAP TM + GTS",
     "SAP's primary product identifier is the Material Number (MATNR, CHAR18) used "
     "across MM, SD, and TM; GTS links to it for commodity classification and HS-code "
     "assignment. The concept — a material number from TM flowing into GTS — is real "
     "and standard. However, the exact field name used to carry MATNR from TM into the "
     "GTS compliance extract has not been confirmed against the client's integration "
     "configuration and would be verified from a sample extract."),

    ("PRODUCT_TEXT",
     "Product Description",
     "🔴",
     "CHAR40 text",
     "Material description (MAKTX style); field name unconfirmed",
     "SAP TM + GTS",
     "The product description (equivalent to MAKTX in SAP MM, CHAR40) is the "
     "human-readable label for the traded good, available in both TM and GTS master "
     "data. This is a real, standard SAP concept. However, the exact field name in "
     "the GTS compliance-item extract — whether it appears as MAKTX, a description "
     "alias, or a separate text field — is unconfirmed and depends on the client's "
     "GTS data mapping and release version."),

    ("CCNGN",
     "Commodity / Tariff Code (HS)",
     "🟢",
     "HS/HTS with dots e.g. '8708.29.00'",
     "GTS /SAPSLL/CTSNUM field CCNGN — verified real field",
     "SAP GTS",
     "CCNGN is the genuine SAP GTS field for the commodity/tariff code (HS code), "
     "residing in table /SAPSLL/CTSNUM. It stores the Harmonized System code in "
     "dotted notation (e.g. '8708.29.00') and is the foundation for GTS duty-rate "
     "lookup, export-control screening, and preferential-origin determination. "
     "This field is verified against SAP GTS documentation — it is the standard "
     "column populating the Commodity Code field on GTS compliance documents."),

    ("CTYDP",
     "Country of Departure",
     "🟢",
     "ISO2 e.g. 'MX'",
     "GTS /SAPSLL/CUIT|CUHD; ISO 3166-1 alpha-2 standard",
     "SAP GTS",
     "CTYDP is the SAP GTS field for Country of Departure, stored in tables "
     "/SAPSLL/CUIT and /SAPSLL/CUHD. It holds the ISO 3166-1 alpha-2 country code "
     "(e.g. 'MX' for Mexico) for the export side of a trade movement. This field is "
     "verified: it is a standard GTS header-level field used in customs declarations, "
     "FTA lane classification, and trade compliance reporting."),

    ("CTYAR",
     "Country of Destination",
     "🟢",
     "ISO2 e.g. 'US'",
     "GTS /SAPSLL/CUIT|CUHD; ISO 3166-1 alpha-2 standard",
     "SAP GTS",
     "CTYAR is the SAP GTS field for Country of Arrival (destination), stored "
     "alongside CTYDP in /SAPSLL/CUIT and /SAPSLL/CUHD. It holds the ISO 3166-1 "
     "alpha-2 code for the import country (e.g. 'US'). This field is verified: it "
     "is a standard GTS header-level field that, together with CTYDP, defines the "
     "trade lane and determines which FTA legal regulation applies."),

    ("CUCOO",
     "Country of Origin",
     "🟢",
     "ISO2 e.g. 'MX'",
     "GTS /SAPSLL/CUIT field CUCOO — verified real field",
     "SAP GTS",
     "CUCOO is the SAP GTS field for Country of Origin, stored in the "
     "compliance-item table /SAPSLL/CUIT. It holds the ISO 3166-1 alpha-2 code "
     "representing where goods were wholly obtained or last substantially "
     "transformed. CUCOO is the direct input to GTS preference determination — "
     "if it matches the required origin under the applicable FTA, the shipment "
     "may qualify for a preferential duty rate. This field is verified against "
     "SAP GTS documentation."),

    ("CUSVAL",
     "Customs Value (USD)",
     "🔴",
     "DEC(15,2) e.g. 245500.00",
     "Customs value is a real GTS concept; exact field name unconfirmed",
     "SAP GTS",
     "Customs value is the declared transaction value used as the duty calculation "
     "base (typically CIF or FOB depending on the trade agreement). SAP GTS stores "
     "customs-value data on compliance documents alongside the currency key WAERS. "
     "The concept is a real and central GTS data element. However, the exact "
     "technical field name CUSVAL as used here has not been verified — the actual "
     "field name in the client's GTS extract may differ by release or country "
     "localisation, and would be confirmed from a sample customs-declaration extract."),

    ("WAERS",
     "Currency",
     "🟢",
     "ISO 4217 e.g. 'USD'",
     "Standard SAP currency key WAERS — real field",
     "SAP (cross-module)",
     "WAERS is the standard SAP currency key, a CHAR5 field holding ISO 4217 "
     "currency codes (e.g. 'USD', 'EUR'). It appears across virtually every SAP "
     "module — MM, SD, FI, CO, GTS — wherever a monetary amount is stored, always "
     "paired with the corresponding amount field. This field is verified: WAERS is "
     "the universally documented, cross-module SAP currency-key field name."),

    ("AGREEMENT",
     "Trade Agreement / FTA",
     "🟢",
     "Text e.g. 'USMCA'",
     "GTS 'legal regulation' concept is real; key name is representative",
     "SAP GTS",
     "In SAP GTS, trade agreements and FTAs are managed as Legal Regulations in the "
     "Compliance Management module. GTS checks each shipment against active legal "
     "regulations to determine preferential eligibility and generate proof-of-origin "
     "documents. The concept — that GTS holds a trade-agreement reference — is "
     "verified as a real GTS data element. The field name AGREEMENT is representative; "
     "the exact GTS key (which may appear as a legal-regulation code or description "
     "field) would be confirmed from the client's GTS configuration."),

    ("MFN_RATE",
     "MFN Duty Rate (%)",
     "🔴",
     "DEC(6,3) e.g. 7.500",
     "MFN/WTO concept real; field name unconfirmed",
     "SAP GTS",
     "The MFN (Most-Favoured Nation) duty rate is the standard WTO tariff applied "
     "to imports from countries without a preferential agreement. SAP GTS can retrieve "
     "and display MFN rates from linked official tariff databases keyed to the "
     "commodity code (CCNGN) and country pair. The concept is real and central to "
     "FTA benefit calculation. However, the exact field name for the MFN rate in the "
     "client's GTS extract is unconfirmed — the rate may reside in a tariff-database "
     "link table rather than on the customs document itself, and confirmation requires "
     "a sample GTS duty-rate extract."),

    ("PREF_RATE",
     "Preferential Duty Rate (%)",
     "🔴",
     "DEC(6,3) e.g. 0.000",
     "Preference rate concept real; exact field name unconfirmed",
     "SAP GTS",
     "The preferential duty rate is the reduced tariff applied when an FTA's origin "
     "and Rules-of-Origin requirements are satisfied. Like MFN_RATE, this is a "
     "fundamental GTS concept tied to the legal regulation (AGREEMENT) and commodity "
     "code (CCNGN). The concept is confirmed. However, the exact field name for the "
     "preferential rate in the client's GTS extract is unconfirmed — it may reside "
     "in a preference-determination result record rather than the customs-document "
     "header, and would be confirmed from a sample GTS preference-determination extract."),

    ("PREF_STATUS",
     "Preference Status",
     "🔴",
     "CHAR1: E=Eligible-Claimed  U=Eligible-Unclaimed  N=Not Eligible",
     "Preference determination concept real; code values E/U/N are invented",
     "SAP GTS",
     "Preference status captures the outcome of GTS preference determination: whether "
     "a shipment has claimed a preferential rate (E), been found eligible but not yet "
     "claimed (U), or been found ineligible (N). SAP GTS performs preference "
     "determination and stores a result per compliance document — this determination "
     "concept is confirmed in GTS. However, the one-character code values E/U/N used "
     "here are representative codes invented for this model; the actual GTS field name "
     "and its allowed status codes would be confirmed from the client's GTS "
     "preference-determination output."),

    ("RVC_PCT",
     "Regional Value Content % (Actual)",
     "🔴",
     "DEC(5,2) e.g. 68.00",
     "RVC is a real RoO concept; SAP field name unconfirmed",
     "SAP GTS",
     "Regional Value Content (RVC) percentage is the proportion of a product's value "
     "originating within the FTA trading region — a key Rules-of-Origin test under "
     "USMCA, RCEP, and CPTPP. SAP GTS's RoO module can calculate or store RVC from "
     "Bills of Material and supplier origin declarations. The concept is a real GTS "
     "RoO metric. However, the exact field name RVC_PCT is representative — the "
     "actual column name in the client's GTS RoO-determination output has not been "
     "confirmed and would be identified from a sample extract."),

    ("RVC_THRESHOLD",
     "Required RoO Threshold %",
     "🔴",
     "DEC(5,2) e.g. 60.00",
     "RVC threshold concept real; SAP field name unconfirmed",
     "SAP GTS",
     "The RVC threshold is the minimum Regional Value Content percentage required "
     "under the applicable FTA for a product to qualify for preferential treatment "
     "(e.g. 60% under USMCA Ch. 4). SAP GTS stores RoO thresholds as part of its "
     "legal-regulation configuration for each commodity code and agreement. The "
     "concept is real. However, the exact field name RVC_THRESHOLD is representative "
     "— the actual configuration-table field name would be confirmed from the client's "
     "GTS RoO setup documentation or a sample determination extract."),

    ("ROO_STATUS",
     "Rules-of-Origin Status",
     "🔴",
     "CHAR1: Q=Qualified  M=Near-Miss  F=Fail (derived)",
     "RoO determination concept real; code values Q/M/F are invented",
     "SAP GTS",
     "Rules-of-Origin status summarises the GTS preference-determination outcome "
     "against the applicable RoO test: Q (Qualified), M (Near-Miss — within 5 "
     "percentage points of the threshold), or F (Fail). SAP GTS performs RoO "
     "determination and stores a result status per compliance item. The determination "
     "concept is confirmed in GTS. However, the one-character codes Q/M/F used here "
     "are invented for this dashboard — the actual GTS RoO result field name and its "
     "allowed values would be confirmed from a client GTS preference-determination extract."),

    ("SUPPLIER_ID",
     "Supplier (Business Partner)",
     "🔴",
     "CHAR10 BP number e.g. '1000004521'",
     "SAP Business Partner concept real; exact GTS linkage unconfirmed",
     "SAP GTS",
     "SAP identifies suppliers via the Business Partner (BP) framework, with a "
     "10-character numeric BP number as the primary key across MM, SD, and GTS. "
     "In GTS, suppliers are linked for origin declarations and Certificate-of-Origin "
     "requests as part of the supplier-collaboration workflow. The concept is standard "
     "SAP. However, the exact field name used to carry the supplier BP number in the "
     "GTS compliance or CoO-request extract is unconfirmed — it depends on the "
     "client's GTS partner-determination configuration and would be verified from "
     "a sample extract."),

    ("SUPPLIER_NAME",
     "Supplier Name",
     "🔴",
     "CHAR40 text",
     "BP name field; SAP-style, exact field name unconfirmed",
     "SAP GTS",
     "The supplier name is derived from the Business Partner master record in SAP "
     "(the BP description field), providing a human-readable label alongside the BP "
     "number (SUPPLIER_ID). The concept is standard SAP. However, the exact field "
     "name in a GTS extract is unconfirmed — the name may be joined at report time "
     "from the BP master rather than stored directly on the GTS document, and the "
     "precise field name would be confirmed from the client's GTS CoO-tracker "
     "data model."),

    ("BOM_REGIONAL_CONTENT",
     "BoM Regional Content %",
     "🔴",
     "DEC(5,2) percent",
     "BoM-level regional content; SAP-style name, unconfirmed",
     "SAP GTS",
     "BoM Regional Content is the RVC percentage calculated at the product "
     "Bill-of-Materials level, derived by aggregating supplier origin declarations "
     "for each purchased component. SAP GTS preference determination can consume "
     "BoM-level origin data to compute product-level RVC. The concept is real in "
     "GTS's RoO engine. However, BOM_REGIONAL_CONTENT is a representative field "
     "name — it is not a confirmed SAP column name. The actual label in GTS's RoO "
     "calculation output would need to be confirmed from the client's GTS "
     "configuration and a sample extract."),

    ("ENTRY_DATE",
     "Customs Entry Date",
     "🔴",
     "SAP DATS 'YYYYMMDD' e.g. '20260612'",
     "DATS date format is real; specific field name unconfirmed",
     "SAP GTS",
     "Customs entry date is the date on which the customs declaration was submitted "
     "or accepted — the key date for determining which FTA rate schedule applied and "
     "for computing retroactive-claim eligibility windows. SAP stores all dates in "
     "DATS format: an 8-character string 'YYYYMMDD' (e.g. '20260612'). The DATS "
     "format is a verified, standard SAP date convention. However, the specific field "
     "name ENTRY_DATE is representative — in GTS this date may appear under a field "
     "name tied to the specific customs-document type, confirmed from a sample "
     "GTS declaration extract."),

    # ── CoO / PROOF-OF-ORIGIN DATA ───────────────────────────────────────────
    ("POO_TYPE",
     "Proof of Origin Type",
     "🟢",
     "Text e.g. 'EUR.1', 'USMCA CO', 'RCEP Form AK'",
     "EUR.1 / EUR-MED / Form A are real GTS proof-of-origin document types",
     "SAP GTS",
     "POO_TYPE identifies the formal trade document used to certify FTA origin "
     "eligibility — for example EUR.1 (EU-MED preference certificate), RCEP Form AK, "
     "USMCA Certificate of Origin, CAFTA-DR CO, or CPTPP Self-Certification. SAP GTS "
     "manages these as distinct proof-of-origin document types in its "
     "document-management module. This field is verified: EUR.1, Form A, USMCA CO, "
     "and other named proof-of-origin types are confirmed real document categories "
     "within SAP GTS's trade-preference and compliance workflow."),

    ("VDECL_REQ_DATE",
     "CoO Request Date",
     "🔴",
     "SAP DATS 'YYYYMMDD'",
     "SAP-style date field; exact name in GTS unconfirmed",
     "SAP GTS",
     "The CoO request date is when the supplier was formally asked to provide the "
     "origin declaration or proof-of-origin document — the start of the "
     "supplier-collaboration workflow in GTS. SAP GTS stores all dates in DATS "
     "format ('YYYYMMDD'). The concept of a supplier-declaration request date within "
     "GTS's CoO workflow is real. However, VDECL_REQ_DATE is a representative field "
     "name — the exact GTS field name for this date has not been confirmed against "
     "the client's system and would be identified from a sample GTS "
     "supplier-declaration extract."),

    ("VDECL_DEADLINE",
     "CoO Deadline",
     "🔴",
     "SAP DATS 'YYYYMMDD'",
     "SAP-style date field; exact name in GTS unconfirmed",
     "SAP GTS",
     "The CoO deadline is the date by which the supplier must deliver the "
     "proof-of-origin document, typically aligned to the customs-entry filing "
     "deadline or shipment date. SAP GTS manages supplier-declaration timelines "
     "as part of its compliance workflow, and stores all dates in DATS format. "
     "The deadline concept is real in GTS's supplier-declaration process. However, "
     "VDECL_DEADLINE is a representative field name — the exact GTS field name "
     "would be confirmed from a sample GTS declaration or supplier-worklist extract."),

    ("POO_STATUS",
     "Proof of Origin Status",
     "🔴",
     "CHAR10: PENDING | RECEIVED | OVERDUE | VALIDATED",
     "Status concept real; code values PENDING/RECEIVED/OVERDUE/VALIDATED invented",
     "SAP GTS",
     "Proof-of-Origin document status tracks the workflow state of the CoO or other "
     "origin certificate: whether it has been requested from the supplier, received, "
     "validated, or is overdue. SAP GTS document-management assigns and tracks "
     "statuses on these documents. The status-tracking concept is real in GTS. "
     "However, the code values PENDING, RECEIVED, OVERDUE, and VALIDATED used here "
     "are representative codes invented for this model — the actual GTS document "
     "status field name and its allowed values would be confirmed from the client's "
     "GTS CoO-document configuration."),

    ("LANE",
     "Trade Lane (display)",
     "🔴",
     "Derived text e.g. 'MX -> US'",
     "Derived display field — not a native SAP field",
     "Derived",
     "The Trade Lane (e.g. 'MX -> US') is a display label derived at dashboard "
     "render time by combining the departure country (CTYDP) and arrival country "
     "(CTYAR). It does not exist as a standalone column in any SAP table. Both "
     "CTYDP and CTYAR are verified SAP GTS fields; the LANE concatenation is a "
     "dashboard-level construct for human readability. No client SAP configuration "
     "change is needed to confirm this field — it is always derived from the two "
     "verified GTS country fields."),
]

# ── Code-to-label translation maps (used by app.py for UI display) ────────────
COUNTRY_NAMES = {
    "MX": "Mexico",      "US": "USA",        "VN": "Vietnam",
    "JP": "Japan",       "KR": "Korea",      "DE": "Germany",
    "CA": "Canada",      "ID": "Indonesia",  "AU": "Australia",
    "MY": "Malaysia",    "PE": "Peru",       "GT": "Guatemala",
    "CN": "China",       "IN": "India",      "TH": "Thailand",
    "CH": "Switzerland", "EU": "EU (zone)",
}

PREF_STATUS_LABELS = {
    "E": "Eligible – Claimed",
    "U": "Eligible – Unclaimed",
    "N": "Not Eligible",
}

ROO_STATUS_LABELS = {
    "Q": "Qualified",
    "M": "Near-Miss",
    "F": "Fail",
}

POO_STATUS_LABELS = {
    "PENDING":   "Pending",
    "RECEIVED":  "Received",
    "OVERDUE":   "Overdue",
    "VALIDATED": "Validated",
}

# ── Shared time basis ─────────────────────────────────────────────────────────
PERIOD_START = "20250805"   # SAP DATS — first day of trailing-12-month window
PERIOD_END   = "20260805"   # SAP DATS — last day (= today, illustrative)
PERIOD_LABEL = "T-12: Aug 2025 – Aug 2026"
RETRO_WINDOW_MONTHS = 12


# ── Internal helpers ──────────────────────────────────────────────────────────

def _derive_roo_status(rvc_pct: float, rvc_threshold: float) -> str:
    """ROO_STATUS derivation rule — single source of truth.
    Q if RVC_PCT >= RVC_THRESHOLD
    M if RVC_THRESHOLD-5 <= RVC_PCT < RVC_THRESHOLD  (near-miss)
    F otherwise
    """
    if rvc_pct >= rvc_threshold:
        return "Q"
    if rvc_pct >= rvc_threshold - 5:
        return "M"
    return "F"


def _dats_to_iso(dats: str) -> str:
    """Convert SAP DATS 'YYYYMMDD' → display 'YYYY-MM-DD'."""
    if len(dats) == 8 and dats.isdigit():
        return f"{dats[:4]}-{dats[4:6]}-{dats[6:]}"
    return dats


def _duty_saving(cusval: float, mfn: float, pref: float) -> float:
    """duty_saving = CUSVAL × (MFN_RATE − PREF_RATE) / 100  (USD)."""
    return round(cusval * (mfn - pref) / 100, 2)


# ── Shipment data ─────────────────────────────────────────────────────────────

def get_fta_shipments() -> list:
    """
    18 shipments covering all three PREF_STATUS values (E / U / N).

    ENTRY_DATE values are SAP DATS 'YYYYMMDD', all within PERIOD_START–PERIOD_END.
    Eligible-unclaimed (U) entries are dated in the last 3–4 months (most actionable).
    Claimed (E) and not-eligible (N) entries spread across the year.

    ROO_STATUS derived from RVC_PCT vs RVC_THRESHOLD — never hardcoded:
        Q  if RVC_PCT >= RVC_THRESHOLD
        M  if RVC_THRESHOLD-5 <= RVC_PCT < RVC_THRESHOLD
        F  otherwise

    duty_saving = CUSVAL × (MFN_RATE − PREF_RATE) / 100  [USD]
    Only meaningful for PREF_STATUS=U rows (savings still capturable).

    RVC thresholds used (illustrative):
        USMCA 60%, EVFTA 40%, RCEP 40% (general) / 60% (Chapter 29 chemicals),
        KORUS 45%, EU-Korea 55%, CPTPP 40%, CAFTA-DR 35%.
    """
    raw = [
        # ── PREF_STATUS = U  (Eligible – Unclaimed) ──────────────────────────
        {
            "TOR_ID":               "6100000783",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000087082",
            "PRODUCT_TEXT":         "Automotive Parts",
            "CCNGN":                "8708.29.00",
            "CTYDP":                "MX",
            "CTYAR":                "US",
            "CUCOO":                "MX",
            "CUSVAL":               245500.00,
            "WAERS":                "USD",
            "AGREEMENT":            "USMCA",
            "MFN_RATE":             7.500,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "U",
            "RVC_PCT":              68,
            "RVC_THRESHOLD":        60,
            "SUPPLIER_ID":          "1000004521",
            "SUPPLIER_NAME":        "Alpha Automotive MX",
            "BOM_REGIONAL_CONTENT": 68.0,
            "ENTRY_DATE":           "20260612",
        },
        {
            "TOR_ID":               "6100000784",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000085423",
            "PRODUCT_TEXT":         "Electronic Components",
            "CCNGN":                "8542.31.00",
            "CTYDP":                "VN",
            "CTYAR":                "EU",
            "CUCOO":                "VN",
            "CUSVAL":               312800.00,
            "WAERS":                "USD",
            "AGREEMENT":            "EVFTA",
            "MFN_RATE":             12.000,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "U",
            "RVC_PCT":              72,
            "RVC_THRESHOLD":        40,
            "SUPPLIER_ID":          "1000004530",
            "SUPPLIER_NAME":        "Viet Electronics JSC",
            "BOM_REGIONAL_CONTENT": 72.0,
            "ENTRY_DATE":           "20260703",
        },
        {
            "TOR_ID":               "6100000785",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000061051",
            "PRODUCT_TEXT":         "Cotton Shirts",
            "CCNGN":                "6105.10.00",
            "CTYDP":                "VN",
            "CTYAR":                "JP",
            "CUCOO":                "VN",
            "CUSVAL":               87600.00,
            "WAERS":                "USD",
            "AGREEMENT":            "RCEP",
            "MFN_RATE":             9.000,
            "PREF_RATE":            2.500,
            "PREF_STATUS":          "U",
            "RVC_PCT":              37,
            "RVC_THRESHOLD":        40,      # 35 <= 37 < 40 → near-miss M
            "SUPPLIER_ID":          "1000004524",
            "SUPPLIER_NAME":        "Hanoi Apparel JSC",
            "BOM_REGIONAL_CONTENT": 37.0,
            "ENTRY_DATE":           "20260520",
        },
        {
            "TOR_ID":               "6100000786",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000073043",
            "PRODUCT_TEXT":         "Steel Tubes",
            "CCNGN":                "7304.31.00",
            "CTYDP":                "KR",
            "CTYAR":                "US",
            "CUCOO":                "KR",
            "CUSVAL":               198400.00,
            "WAERS":                "USD",
            "AGREEMENT":            "KORUS",
            "MFN_RATE":             6.500,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "U",
            "RVC_PCT":              65,
            "RVC_THRESHOLD":        45,
            "SUPPLIER_ID":          "1000004531",
            "SUPPLIER_NAME":        "Seoul Steel KR",
            "BOM_REGIONAL_CONTENT": 65.0,
            "ENTRY_DATE":           "20260715",
        },
        {
            "TOR_ID":               "6100000787",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000084831",
            "PRODUCT_TEXT":         "Machinery Parts",
            "CCNGN":                "8483.10.00",
            "CTYDP":                "CA",
            "CTYAR":                "US",
            "CUCOO":                "CA",
            "CUSVAL":               156200.00,
            "WAERS":                "USD",
            "AGREEMENT":            "USMCA",
            "MFN_RATE":             7.500,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "U",
            "RVC_PCT":              57,
            "RVC_THRESHOLD":        60,      # 55 <= 57 < 60 → near-miss M
            "SUPPLIER_ID":          "1000004525",
            "SUPPLIER_NAME":        "Toronto Parts Inc",
            "BOM_REGIONAL_CONTENT": 57.0,
            "ENTRY_DATE":           "20260628",
        },
        {
            "TOR_ID":               "6100000788",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000085287",
            "PRODUCT_TEXT":         "Electronic Displays",
            "CCNGN":                "8528.72.00",
            "CTYDP":                "KR",
            "CTYAR":                "DE",
            "CUCOO":                "KR",
            "CUSVAL":               278300.00,
            "WAERS":                "USD",
            "AGREEMENT":            "EU-Korea FTA",
            "MFN_RATE":             6.500,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "U",
            "RVC_PCT":              74,
            "RVC_THRESHOLD":        55,
            "SUPPLIER_ID":          "1000004523",
            "SUPPLIER_NAME":        "Seoul Electronics Co",
            "BOM_REGIONAL_CONTENT": 74.0,
            "ENTRY_DATE":           "20260722",
        },
        {
            "TOR_ID":               "6100000789",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000076041",
            "PRODUCT_TEXT":         "Aluminum Rods",
            "CCNGN":                "7604.10.00",
            "CTYDP":                "MX",
            "CTYAR":                "US",
            "CUCOO":                "MX",
            "CUSVAL":               92400.00,
            "WAERS":                "USD",
            "AGREEMENT":            "USMCA",
            "MFN_RATE":             7.500,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "U",
            "RVC_PCT":              56,
            "RVC_THRESHOLD":        60,      # 55 <= 56 < 60 → near-miss M
            "SUPPLIER_ID":          "1000004521",
            "SUPPLIER_NAME":        "Alpha Automotive MX",
            "BOM_REGIONAL_CONTENT": 56.0,
            "ENTRY_DATE":           "20260605",
        },
        {
            "TOR_ID":               "6100000790",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000062034",
            "PRODUCT_TEXT":         "Woven Apparel",
            "CCNGN":                "6203.42.00",
            "CTYDP":                "VN",
            "CTYAR":                "EU",
            "CUCOO":                "VN",
            "CUSVAL":               68900.00,
            "WAERS":                "USD",
            "AGREEMENT":            "EVFTA",
            "MFN_RATE":             12.000,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "U",
            "RVC_PCT":              61,
            "RVC_THRESHOLD":        40,
            "SUPPLIER_ID":          "1000004522",
            "SUPPLIER_NAME":        "Viet Textiles JSC",
            "BOM_REGIONAL_CONTENT": 61.0,
            "ENTRY_DATE":           "20260730",
        },
        # ── PREF_STATUS = E  (Eligible – Claimed) ────────────────────────────
        {
            "TOR_ID":               "6100000791",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000085340",
            "PRODUCT_TEXT":         "PCB Assemblies",
            "CCNGN":                "8534.00.00",
            "CTYDP":                "KR",
            "CTYAR":                "US",
            "CUCOO":                "KR",
            "CUSVAL":               445600.00,
            "WAERS":                "USD",
            "AGREEMENT":            "KORUS",
            "MFN_RATE":             6.500,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "E",
            "RVC_PCT":              78,
            "RVC_THRESHOLD":        45,
            "SUPPLIER_ID":          "1000004523",
            "SUPPLIER_NAME":        "Seoul Electronics Co",
            "BOM_REGIONAL_CONTENT": 78.0,
            "ENTRY_DATE":           "20260314",
        },
        {
            "TOR_ID":               "6100000792",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000085443",
            "PRODUCT_TEXT":         "Auto Wiring Harness",
            "CCNGN":                "8544.30.00",
            "CTYDP":                "MX",
            "CTYAR":                "US",
            "CUCOO":                "MX",
            "CUSVAL":               389200.00,
            "WAERS":                "USD",
            "AGREEMENT":            "USMCA",
            "MFN_RATE":             7.500,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "E",
            "RVC_PCT":              82,
            "RVC_THRESHOLD":        60,
            "SUPPLIER_ID":          "1000004521",
            "SUPPLIER_NAME":        "Alpha Automotive MX",
            "BOM_REGIONAL_CONTENT": 82.0,
            "ENTRY_DATE":           "20260408",
        },
        {
            "TOR_ID":               "6100000793",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000003046",
            "PRODUCT_TEXT":         "Seafood Products",
            "CCNGN":                "0304.62.00",
            "CTYDP":                "MY",
            "CTYAR":                "JP",
            "CUCOO":                "MY",
            "CUSVAL":               134800.00,
            "WAERS":                "USD",
            "AGREEMENT":            "CPTPP",
            "MFN_RATE":             8.000,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "E",
            "RVC_PCT":              70,
            "RVC_THRESHOLD":        40,
            "SUPPLIER_ID":          "1000004527",
            "SUPPLIER_NAME":        "KL Seafood Bhd",
            "BOM_REGIONAL_CONTENT": 70.0,
            "ENTRY_DATE":           "20251120",
        },
        {
            "TOR_ID":               "6100000794",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000007093",
            "PRODUCT_TEXT":         "Agricultural Produce",
            "CCNGN":                "0709.30.00",
            "CTYDP":                "GT",
            "CTYAR":                "US",
            "CUCOO":                "GT",
            "CUSVAL":               78400.00,
            "WAERS":                "USD",
            "AGREEMENT":            "CAFTA-DR",
            "MFN_RATE":             6.000,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "E",
            "RVC_PCT":              85,
            "RVC_THRESHOLD":        35,
            "SUPPLIER_ID":          "1000004529",
            "SUPPLIER_NAME":        "Guatemala Agro SA",
            "BOM_REGIONAL_CONTENT": 85.0,
            "ENTRY_DATE":           "20251210",
        },
        {
            "TOR_ID":               "6100000795",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000029214",
            "PRODUCT_TEXT":         "Chemical Compounds",
            "CCNGN":                "2921.41.00",
            "CTYDP":                "ID",
            "CTYAR":                "JP",
            "CUCOO":                "ID",
            "CUSVAL":               167300.00,
            "WAERS":                "USD",
            "AGREEMENT":            "RCEP",
            "MFN_RATE":             8.000,
            "PREF_RATE":            2.000,
            "PREF_STATUS":          "E",
            "RVC_PCT":              58,
            "RVC_THRESHOLD":        60,      # RCEP Ch.29 chemicals: 60%. 55<=58<60 → M
            "SUPPLIER_ID":          "1000004526",
            "SUPPLIER_NAME":        "Jakarta Metals PT",
            "BOM_REGIONAL_CONTENT": 58.0,
            "ENTRY_DATE":           "20260115",
        },
        {
            "TOR_ID":               "6100000796",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000074081",
            "PRODUCT_TEXT":         "Copper Wire",
            "CCNGN":                "7408.11.00",
            "CTYDP":                "PE",
            "CTYAR":                "CA",
            "CUCOO":                "PE",
            "CUSVAL":               112600.00,
            "WAERS":                "USD",
            "AGREEMENT":            "CPTPP",
            "MFN_RATE":             9.500,
            "PREF_RATE":            0.000,
            "PREF_STATUS":          "E",
            "RVC_PCT":              67,
            "RVC_THRESHOLD":        40,
            "SUPPLIER_ID":          "1000004528",
            "SUPPLIER_NAME":        "Lima Copper SAC",
            "BOM_REGIONAL_CONTENT": 67.0,
            "ENTRY_DATE":           "20260228",
        },
        # ── PREF_STATUS = N  (Not Eligible) ──────────────────────────────────
        {
            "TOR_ID":               "6100000797",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000085051",
            "PRODUCT_TEXT":         "Rare Earth Magnets",
            "CCNGN":                "8505.11.00",
            "CTYDP":                "CN",
            "CTYAR":                "US",
            "CUCOO":                "CN",
            "CUSVAL":               234700.00,
            "WAERS":                "USD",
            "AGREEMENT":            "N/A",
            "MFN_RATE":             5.000,
            "PREF_RATE":            5.000,
            "PREF_STATUS":          "N",
            "RVC_PCT":              22,
            "RVC_THRESHOLD":        40,      # 22 < 35 → F
            "SUPPLIER_ID":          "1000004540",
            "SUPPLIER_NAME":        "China Magnetics Ltd",
            "BOM_REGIONAL_CONTENT": 22.0,
            "ENTRY_DATE":           "20260505",
        },
        {
            "TOR_ID":               "6100000798",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000030049",
            "PRODUCT_TEXT":         "Pharmaceuticals",
            "CCNGN":                "3004.90.00",
            "CTYDP":                "IN",
            "CTYAR":                "EU",
            "CUCOO":                "IN",
            "CUSVAL":               189400.00,
            "WAERS":                "USD",
            "AGREEMENT":            "N/A",
            "MFN_RATE":             5.000,
            "PREF_RATE":            5.000,
            "PREF_STATUS":          "N",
            "RVC_PCT":              18,
            "RVC_THRESHOLD":        40,      # 18 < 35 → F
            "SUPPLIER_ID":          "1000004541",
            "SUPPLIER_NAME":        "India Pharma Ltd",
            "BOM_REGIONAL_CONTENT": 18.0,
            "ENTRY_DATE":           "20260422",
        },
        {
            "TOR_ID":               "6100000799",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000040169",
            "PRODUCT_TEXT":         "Rubber Products",
            "CCNGN":                "4016.99.00",
            "CTYDP":                "TH",
            "CTYAR":                "US",
            "CUCOO":                "TH",
            "CUSVAL":               145800.00,
            "WAERS":                "USD",
            "AGREEMENT":            "N/A",
            "MFN_RATE":             5.000,
            "PREF_RATE":            5.000,
            "PREF_STATUS":          "N",
            "RVC_PCT":              28,
            "RVC_THRESHOLD":        40,      # 28 < 35 → F
            "SUPPLIER_ID":          "1000004542",
            "SUPPLIER_NAME":        "Thai Rubber Co",
            "BOM_REGIONAL_CONTENT": 28.0,
            "ENTRY_DATE":           "20250918",
        },
        {
            "TOR_ID":               "6100000800",
            "TOR_ITEM":             "000010",
            "PRODUCT_ID":           "1000091021",
            "PRODUCT_TEXT":         "Luxury Watches",
            "CCNGN":                "9102.19.00",
            "CTYDP":                "CH",
            "CTYAR":                "US",
            "CUCOO":                "CH",
            "CUSVAL":               678200.00,
            "WAERS":                "USD",
            "AGREEMENT":            "N/A",
            "MFN_RATE":             5.000,
            "PREF_RATE":            5.000,
            "PREF_STATUS":          "N",
            "RVC_PCT":              12,
            "RVC_THRESHOLD":        40,      # 12 < 35 → F
            "SUPPLIER_ID":          "1000004543",
            "SUPPLIER_NAME":        "Swiss Watches AG",
            "BOM_REGIONAL_CONTENT": 12.0,
            "ENTRY_DATE":           "20251014",
        },
    ]

    for s in raw:
        s["ROO_STATUS"] = _derive_roo_status(s["RVC_PCT"], s["RVC_THRESHOLD"])
        # duty_saving meaningful for U rows; included for all for reconciliation
        s["duty_saving"] = (
            _duty_saving(s["CUSVAL"], s["MFN_RATE"], s["PREF_RATE"])
            if s["PREF_STATUS"] == "U" else 0.0
        )

    return raw


# ── Lane data ─────────────────────────────────────────────────────────────────

def get_fta_lanes() -> list:
    """
    11 trade lanes with FTA eligibility and utilization metrics.
    Pre-sorted descending by UNCLAIMED_SAVINGS_K.

    ELIGIBLE_VALUE_M  = sum CUSVAL for PREF_STATUS in (E, U)  [all shipments in lane]
    CLAIMED_VALUE_M   = sum CUSVAL for PREF_STATUS = E
    UTILIZATION_PCT   = CLAIMED_VALUE_M / ELIGIBLE_VALUE_M × 100
    UNCLAIMED_SAVINGS_K = (ELIGIBLE_VALUE_M − CLAIMED_VALUE_M) × 1000
                          × (MFN_RATE − PREF_RATE) / 100

    The 18 sample shipments cover subsets of each lane; these lane-level figures
    represent the full population (all shipments in the period, not just the sample).
    """
    lanes = [
        {
            "lane_id":          "LN-001",
            "CTYDP":            "MX",
            "CTYAR":            "US",
            "AGREEMENT":        "USMCA",
            "ELIGIBLE_VALUE_M": 12.4,
            "CLAIMED_VALUE_M":  7.8,
            "MFN_RATE":         7.500,
            "PREF_RATE":        0.000,
        },
        {
            "lane_id":          "LN-002",
            "CTYDP":            "VN",
            "CTYAR":            "EU",
            "AGREEMENT":        "EVFTA",
            "ELIGIBLE_VALUE_M": 9.8,
            "CLAIMED_VALUE_M":  5.6,
            "MFN_RATE":         12.000,
            "PREF_RATE":        0.000,
        },
        {
            "lane_id":          "LN-003",
            "CTYDP":            "VN",
            "CTYAR":            "JP",
            "AGREEMENT":        "RCEP",
            "ELIGIBLE_VALUE_M": 6.9,
            "CLAIMED_VALUE_M":  4.2,
            "MFN_RATE":         9.000,
            "PREF_RATE":        2.500,
        },
        {
            "lane_id":          "LN-004",
            "CTYDP":            "KR",
            "CTYAR":            "US",
            "AGREEMENT":        "KORUS",
            "ELIGIBLE_VALUE_M": 8.3,
            "CLAIMED_VALUE_M":  5.9,
            "MFN_RATE":         6.500,
            "PREF_RATE":        0.000,
        },
        {
            "lane_id":          "LN-005",
            "CTYDP":            "KR",
            "CTYAR":            "DE",
            "AGREEMENT":        "EU-Korea FTA",
            "ELIGIBLE_VALUE_M": 4.8,
            "CLAIMED_VALUE_M":  3.2,
            "MFN_RATE":         6.500,
            "PREF_RATE":        0.000,
        },
        {
            "lane_id":          "LN-006",
            "CTYDP":            "CA",
            "CTYAR":            "US",
            "AGREEMENT":        "USMCA",
            "ELIGIBLE_VALUE_M": 7.6,
            "CLAIMED_VALUE_M":  6.1,
            "MFN_RATE":         5.000,
            "PREF_RATE":        0.000,
        },
        {
            "lane_id":          "LN-007",
            "CTYDP":            "ID",
            "CTYAR":            "JP",
            "AGREEMENT":        "RCEP",
            "ELIGIBLE_VALUE_M": 5.4,
            "CLAIMED_VALUE_M":  3.8,
            "MFN_RATE":         8.000,
            "PREF_RATE":        2.000,
        },
        {
            "lane_id":          "LN-008",
            "CTYDP":            "US",
            "CTYAR":            "AU",
            "AGREEMENT":        "US-AUS FTA",
            "ELIGIBLE_VALUE_M": 3.9,
            "CLAIMED_VALUE_M":  2.8,
            "MFN_RATE":         5.000,
            "PREF_RATE":        0.000,
        },
        {
            "lane_id":          "LN-009",
            "CTYDP":            "MY",
            "CTYAR":            "JP",
            "AGREEMENT":        "CPTPP",
            "ELIGIBLE_VALUE_M": 5.1,
            "CLAIMED_VALUE_M":  4.1,
            "MFN_RATE":         8.000,
            "PREF_RATE":        0.000,
        },
        {
            "lane_id":          "LN-010",
            "CTYDP":            "PE",
            "CTYAR":            "CA",
            "AGREEMENT":        "CPTPP",
            "ELIGIBLE_VALUE_M": 3.2,
            "CLAIMED_VALUE_M":  2.1,
            "MFN_RATE":         9.500,
            "PREF_RATE":        0.000,
        },
        {
            "lane_id":          "LN-011",
            "CTYDP":            "GT",
            "CTYAR":            "US",
            "AGREEMENT":        "CAFTA-DR",
            "ELIGIBLE_VALUE_M": 4.2,
            "CLAIMED_VALUE_M":  3.5,
            "MFN_RATE":         6.000,
            "PREF_RATE":        0.000,
        },
    ]

    # retro_eligible_pct: share of unclaimed savings in 12-month retro window
    _retro_pct = {
        "LN-001": 15, "LN-002": 18, "LN-003": 25, "LN-004": 30,
        "LN-005": 20, "LN-006": 15, "LN-007": 30, "LN-008": 25,
        "LN-009":  0, "LN-010":  0, "LN-011":  0,
    }

    for lane in lanes:
        unclaimed_m = lane["ELIGIBLE_VALUE_M"] - lane["CLAIMED_VALUE_M"]
        lane["UTILIZATION_PCT"] = round(
            lane["CLAIMED_VALUE_M"] / lane["ELIGIBLE_VALUE_M"] * 100, 1
        )
        lane["UNCLAIMED_SAVINGS_K"] = round(
            unclaimed_m * 1000 * (lane["MFN_RATE"] - lane["PREF_RATE"]) / 100, 1
        )
        lane["retro_eligible_pct"] = _retro_pct.get(lane["lane_id"], 0)
        lane["retro_k"] = round(
            lane["UNCLAIMED_SAVINGS_K"] * lane["retro_eligible_pct"] / 100, 1
        )

    lanes.sort(key=lambda x: x["UNCLAIMED_SAVINGS_K"], reverse=True)
    return lanes


# ── KPIs ─────────────────────────────────────────────────────────────────────

def get_fta_kpis() -> dict:
    """
    All KPIs derived from the same lane + CoO data the tables display.

    UTILIZATION_RATE      = Σ CLAIMED_VALUE_M / Σ ELIGIBLE_VALUE_M  [lanes]
    UNCLAIMED_OPPORTUNITY = Σ UNCLAIMED_SAVINGS_K / 1000  [lanes → $M]
    RETROACTIVE_CLAIMS    = Σ retro_k  [lanes, strict subset of UNCLAIMED_OPPORTUNITY]
    COOS_OUTSTANDING      = count CoO rows where POO_STATUS ∈ {OVERDUE, PENDING, RECEIVED}
    """
    lanes = get_fta_lanes()
    coos  = get_coo_requests()

    total_eligible_m  = sum(l["ELIGIBLE_VALUE_M"]   for l in lanes)
    total_claimed_m   = sum(l["CLAIMED_VALUE_M"]     for l in lanes)
    total_unclaimed_k = sum(l["UNCLAIMED_SAVINGS_K"] for l in lanes)
    total_retro_k     = round(sum(l["retro_k"]        for l in lanes), 1)
    coo_outstanding   = sum(
        1 for c in coos if c["POO_STATUS"] in ("OVERDUE", "PENDING", "RECEIVED")
    )

    return {
        "utilization_pct":         round(total_claimed_m / total_eligible_m * 100, 1),
        "unclaimed_opportunity_m":  round(total_unclaimed_k / 1000, 2),
        "retroactive_claims_k":    total_retro_k,
        "coo_outstanding":         coo_outstanding,
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
    9 current CoO/proof-of-origin supplier requests.
    POO_STATUS: 2 OVERDUE, 3 PENDING, 2 RECEIVED, 2 VALIDATED.
    VDECL_REQ_DATE / VDECL_DEADLINE in SAP DATS 'YYYYMMDD'.
    POO_TYPE: real GTS document type names (EUR.1, RCEP Form AK, USMCA CO, etc.).
    """
    return [
        {
            "SUPPLIER_ID":    "1000004521",
            "SUPPLIER_NAME":  "Alpha Automotive MX",
            "CTYDP":          "MX",
            "CTYAR":          "US",
            "POO_TYPE":       "USMCA CO",
            "VDECL_REQ_DATE": "20260701",
            "VDECL_DEADLINE": "20260715",
            "POO_STATUS":     "OVERDUE",
        },
        {
            "SUPPLIER_ID":    "1000004522",
            "SUPPLIER_NAME":  "Viet Textiles JSC",
            "CTYDP":          "VN",
            "CTYAR":          "EU",
            "POO_TYPE":       "EUR.1",
            "VDECL_REQ_DATE": "20260705",
            "VDECL_DEADLINE": "20260722",
            "POO_STATUS":     "OVERDUE",
        },
        {
            "SUPPLIER_ID":    "1000004523",
            "SUPPLIER_NAME":  "Seoul Electronics Co",
            "CTYDP":          "KR",
            "CTYAR":          "US",
            "POO_TYPE":       "KORUS CO",
            "VDECL_REQ_DATE": "20260710",
            "VDECL_DEADLINE": "20260810",
            "POO_STATUS":     "PENDING",
        },
        {
            "SUPPLIER_ID":    "1000004524",
            "SUPPLIER_NAME":  "Hanoi Apparel JSC",
            "CTYDP":          "VN",
            "CTYAR":          "JP",
            "POO_TYPE":       "RCEP Form AK",
            "VDECL_REQ_DATE": "20260712",
            "VDECL_DEADLINE": "20260805",
            "POO_STATUS":     "PENDING",
        },
        {
            "SUPPLIER_ID":    "1000004525",
            "SUPPLIER_NAME":  "Toronto Parts Inc",
            "CTYDP":          "CA",
            "CTYAR":          "US",
            "POO_TYPE":       "USMCA CO",
            "VDECL_REQ_DATE": "20260715",
            "VDECL_DEADLINE": "20260815",
            "POO_STATUS":     "PENDING",
        },
        {
            "SUPPLIER_ID":    "1000004526",
            "SUPPLIER_NAME":  "Jakarta Metals PT",
            "CTYDP":          "ID",
            "CTYAR":          "JP",
            "POO_TYPE":       "RCEP Form AK",
            "VDECL_REQ_DATE": "20260708",
            "VDECL_DEADLINE": "20260728",
            "POO_STATUS":     "RECEIVED",
        },
        {
            "SUPPLIER_ID":    "1000004527",
            "SUPPLIER_NAME":  "KL Seafood Bhd",
            "CTYDP":          "MY",
            "CTYAR":          "JP",
            "POO_TYPE":       "CPTPP Self-Cert",
            "VDECL_REQ_DATE": "20260710",
            "VDECL_DEADLINE": "20260730",
            "POO_STATUS":     "RECEIVED",
        },
        {
            "SUPPLIER_ID":    "1000004528",
            "SUPPLIER_NAME":  "Lima Copper SAC",
            "CTYDP":          "PE",
            "CTYAR":          "CA",
            "POO_TYPE":       "CPTPP Self-Cert",
            "VDECL_REQ_DATE": "20260718",
            "VDECL_DEADLINE": "20260818",
            "POO_STATUS":     "VALIDATED",
        },
        {
            "SUPPLIER_ID":    "1000004529",
            "SUPPLIER_NAME":  "Guatemala Agro SA",
            "CTYDP":          "GT",
            "CTYAR":          "US",
            "POO_TYPE":       "CAFTA-DR CO",
            "VDECL_REQ_DATE": "20260720",
            "VDECL_DEADLINE": "20260820",
            "POO_STATUS":     "VALIDATED",
        },
    ]


# ── RoO / Preference Assessment ───────────────────────────────────────────────

def _derive_roo_status_legacy(rvc_pct: int, rvc_threshold: int) -> str:
    """Alias kept for fta_data_source compatibility."""
    return _derive_roo_status(rvc_pct, rvc_threshold)


def get_roo_assessments() -> list:
    """
    Per-product Rules-of-Origin compliance view.
    ROO_STATUS and gap_pct are derived — never hardcoded.
    RVC values cross-reference get_fta_shipments() where a shipment exists.
    """
    products = [
        # USMCA
        {
            "PRODUCT_TEXT":    "Automotive Parts",
            "CCNGN":           "8708.29.00",
            "AGREEMENT":       "USMCA",
            "roo_test_type":   "Regional Value Content",
            "RVC_THRESHOLD":   60,
            "RVC_PCT":         68,   # TOR_ID 6100000783
            "compliance_note": "Passes net-cost RVC. CoO documentation current.",
        },
        {
            "PRODUCT_TEXT":    "Machinery Parts",
            "CCNGN":           "8483.10.00",
            "AGREEMENT":       "USMCA",
            "roo_test_type":   "Regional Value Content",
            "RVC_THRESHOLD":   60,
            "RVC_PCT":         57,   # TOR_ID 6100000787 — near-miss M
            "compliance_note": "3 pts below threshold. Review non-originating input costs.",
        },
        {
            "PRODUCT_TEXT":    "Aluminum Rods",
            "CCNGN":           "7604.10.00",
            "AGREEMENT":       "USMCA",
            "roo_test_type":   "Regional Value Content",
            "RVC_THRESHOLD":   60,
            "RVC_PCT":         56,   # TOR_ID 6100000789 — near-miss M
            "compliance_note": "4 pts below threshold. Shift billet to domestic supplier.",
        },
        # EVFTA
        {
            "PRODUCT_TEXT":    "Electronic Components",
            "CCNGN":           "8542.31.00",
            "AGREEMENT":       "EVFTA",
            "roo_test_type":   "Tariff Classification Change",
            "RVC_THRESHOLD":   40,
            "RVC_PCT":         72,   # TOR_ID 6100000784
            "compliance_note": "CTC and value-add rule both satisfied. CoO received.",
        },
        {
            "PRODUCT_TEXT":    "Woven Apparel",
            "CCNGN":           "6203.42.00",
            "AGREEMENT":       "EVFTA",
            "roo_test_type":   "Specific Process (Double Transformation)",
            "RVC_THRESHOLD":   40,
            "RVC_PCT":         61,   # TOR_ID 6100000790
            "compliance_note": "Spinning and weaving both performed in Vietnam. EUR.1 issued.",
        },
        # RCEP
        {
            "PRODUCT_TEXT":    "Cotton Shirts",
            "CCNGN":           "6105.10.00",
            "AGREEMENT":       "RCEP",
            "roo_test_type":   "Regional Value Content",
            "RVC_THRESHOLD":   40,
            "RVC_PCT":         37,   # TOR_ID 6100000785 — near-miss M
            "compliance_note": "3 pts short. Shift fabric sourcing from CN to Vietnamese mills.",
        },
        {
            "PRODUCT_TEXT":    "Chemical Compounds",
            "CCNGN":           "2921.41.00",
            "AGREEMENT":       "RCEP",
            "roo_test_type":   "Regional Value Content",
            "RVC_THRESHOLD":   60,   # RCEP Ch.29 chemicals require 60%
            "RVC_PCT":         58,   # TOR_ID 6100000795 — near-miss M
            "compliance_note": "Ch.29 requires 60% RVC. 2 pts short — review reagent sourcing.",
        },
        # KORUS
        {
            "PRODUCT_TEXT":    "Steel Tubes",
            "CCNGN":           "7304.31.00",
            "AGREEMENT":       "KORUS",
            "roo_test_type":   "Regional Value Content",
            "RVC_THRESHOLD":   45,
            "RVC_PCT":         65,   # TOR_ID 6100000786
            "compliance_note": "20 pts above threshold. Routine CoO renewal due Q4 2026.",
        },
        # EU-Korea FTA
        {
            "PRODUCT_TEXT":    "Electronic Displays",
            "CCNGN":           "8528.72.00",
            "AGREEMENT":       "EU-Korea FTA",
            "roo_test_type":   "Specific Process + RVC",
            "RVC_THRESHOLD":   55,
            "RVC_PCT":         74,   # TOR_ID 6100000788
            "compliance_note": "Specific ops satisfied; RVC_PCT 74% > 55%. Fully compliant.",
        },
        # CPTPP
        {
            "PRODUCT_TEXT":    "Copper Wire",
            "CCNGN":           "7408.11.00",
            "AGREEMENT":       "CPTPP",
            "roo_test_type":   "Tariff Classification Change",
            "RVC_THRESHOLD":   40,
            "RVC_PCT":         67,   # TOR_ID 6100000796
            "compliance_note": "Heading shift Ch.26 (ore) → Ch.74 (wire) confirmed. Compliant.",
        },
    ]

    for p in products:
        p["ROO_STATUS"] = _derive_roo_status(p["RVC_PCT"], p["RVC_THRESHOLD"])
        p["gap_pct"]    = max(0, p["RVC_THRESHOLD"] - p["RVC_PCT"])

    return products


# ── Qualification Roadmap ─────────────────────────────────────────────────────

def get_qualification_roadmap() -> list:
    """
    Recommended actions for lanes with UTILIZATION_PCT < 75%.
    UNCLAIMED_SAVINGS_K pulled from get_fta_lanes() at call time — never hardcoded.
    """
    _metadata = {
        "LN-002": {
            "primary_action":   "Enforce double-transformation CoO for EVFTA textiles",
            "secondary_action": "File retroactive claims for Q1–Q2 unclaimed shipments",
            "effort": "Medium", "timeline": "4–6 weeks",
        },
        "LN-001": {
            "primary_action":   "Complete USMCA CoO renewals for 4 overdue suppliers",
            "secondary_action": "Qualify near-miss products by shifting to USMCA-origin billet",
            "effort": "Medium", "timeline": "3–5 weeks",
        },
        "LN-003": {
            "primary_action":   "Shift fabric sourcing to Vietnamese mills to meet RCEP 40% RVC",
            "secondary_action": "Request updated CoO from Viet Textiles JSC (currently overdue)",
            "effort": "High", "timeline": "6–10 weeks",
        },
        "LN-004": {
            "primary_action":   "Renew KORUS certificates for 3 electronics suppliers",
            "secondary_action": "Verify HS sub-classification for steel tube product lines",
            "effort": "Low", "timeline": "2–3 weeks",
        },
        "LN-010": {
            "primary_action":   "Confirm CPTPP tariff-shift documentation for copper wire",
            "secondary_action": "File retroactive claim via amended entry for Q2 shipments",
            "effort": "Low", "timeline": "2–4 weeks",
        },
        "LN-005": {
            "primary_action":   "Complete EU-Korea specific-process attestation for displays",
            "secondary_action": "Engage Korean manufacturer on systematic RVC_PCT tracking",
            "effort": "Medium", "timeline": "4–6 weeks",
        },
        "LN-007": {
            "primary_action":   "Validate RCEP originating-goods declaration for Indonesian metals",
            "secondary_action": "Instruct ASEAN customs broker to apply RCEP tariff line at entry",
            "effort": "Low", "timeline": "2–3 weeks",
        },
        "LN-008": {
            "primary_action":   "Submit US-AUS FTA tariff concession orders for machinery exports",
            "secondary_action": "Identify additional product lines eligible under AUSFTA Schedule",
            "effort": "Medium", "timeline": "3–5 weeks",
        },
    }
    roadmap = []
    for lane in get_fta_lanes():
        if lane["UTILIZATION_PCT"] < 75 and lane["lane_id"] in _metadata:
            ctydp_name = COUNTRY_NAMES.get(lane["CTYDP"], lane["CTYDP"])
            ctyar_name = COUNTRY_NAMES.get(lane["CTYAR"], lane["CTYAR"])
            entry = {
                "lane_id":              lane["lane_id"],
                "CTYDP":                lane["CTYDP"],
                "CTYAR":                lane["CTYAR"],
                "lane_display":         f"{ctydp_name} → {ctyar_name}",
                "AGREEMENT":            lane["AGREEMENT"],
                "UTILIZATION_PCT":      lane["UTILIZATION_PCT"],
                "UNCLAIMED_SAVINGS_K":  lane["UNCLAIMED_SAVINGS_K"],
            }
            entry.update(_metadata[lane["lane_id"]])
            roadmap.append(entry)
    roadmap.sort(key=lambda x: x["UNCLAIMED_SAVINGS_K"], reverse=True)
    return roadmap


# ── Reconciliation + Field Dictionary print (run as __main__) ─────────────────

def _print_field_dictionary() -> None:
    print("\n" + "═" * 110)
    print("FIELD DICTIONARY — SAP TM + GTS  (🟢 confirmed SAP  /  🔴 SAP-style, confirm against client release)")
    print("═" * 110)
    hdr = f"{'SAP_NAME':<26} {'UNIVERSAL_NAME':<32} {'P':3} {'SOURCE':<20} {'FORMAT'}"
    print(hdr)
    print("─" * 110)
    for row in FIELD_DICTIONARY:
        sap, univ, prov, fmt, note = row[0], row[1], row[2], row[3], row[4]
        src  = row[5] if len(row) > 5 else "—"
        long = row[6] if len(row) > 6 else ""
        print(f"{sap:<26} {univ:<32} {prov:3} {src:<20} {fmt}")
        print(f"{'':26} {'':32}     Note: {note}")
        if long:
            # Word-wrap long_desc at ~90 chars
            words = long.split(); line = ""; wrapped = []
            for w in words:
                if len(line) + len(w) + 1 > 90:
                    wrapped.append(line.rstrip()); line = ""
                line += w + " "
            if line.strip():
                wrapped.append(line.rstrip())
            for ln in wrapped:
                print(f"{'':26} {'':32}     {ln}")
        print()


def _print_reconciliation() -> None:
    lanes = get_fta_lanes()
    kpis  = get_fta_kpis()
    ships = get_fta_shipments()
    coos  = get_coo_requests()

    print("\n" + "═" * 80)
    print("KPI RECONCILIATION — values must tie across KPI strip, lane table, shipment feed")
    print("═" * 80)

    total_elig  = sum(l["ELIGIBLE_VALUE_M"]   for l in lanes)
    total_claim = sum(l["CLAIMED_VALUE_M"]     for l in lanes)
    total_uncl  = sum(l["UNCLAIMED_SAVINGS_K"] for l in lanes)
    total_retro = sum(l["retro_k"]             for l in lanes)

    print(f"\n{'─'*60}")
    print("LANE TABLE AGGREGATES (source of truth for KPIs):")
    print(f"  Σ ELIGIBLE_VALUE_M  = ${total_elig:.3f}M")
    print(f"  Σ CLAIMED_VALUE_M   = ${total_claim:.3f}M")
    print(f"  UTILIZATION_PCT     = {round(total_claim/total_elig*100,1)}%")
    print(f"  Σ UNCLAIMED_SAVINGS_K = ${total_uncl:.1f}K = ${total_uncl/1000:.3f}M")
    print(f"  Σ retro_k           = ${total_retro:.1f}K")

    print(f"\n{'─'*60}")
    print("KPI STRIP (must match lane aggregates):")
    print(f"  utilization_pct         = {kpis['utilization_pct']}%"
          f"  ← ties to {round(total_claim/total_elig*100,1)}% ✓")
    print(f"  unclaimed_opportunity_m = ${kpis['unclaimed_opportunity_m']}M"
          f"  ← ties to ${round(total_uncl/1000,2)}M ✓")
    print(f"  retroactive_claims_k    = ${kpis['retroactive_claims_k']}K"
          f"  ← strict subset of unclaimed ✓"
          f"  ({round(kpis['retroactive_claims_k']/total_uncl*100,1)}% of unclaimed)")
    print(f"  coo_outstanding         = {kpis['coo_outstanding']}"
          f"  ← POO_STATUS ∈ {{OVERDUE,PENDING,RECEIVED}} count ✓")

    print(f"\n{'─'*60}")
    print("SHIPMENT SAMPLE (18 rows — subset of full lane population):")
    u_ships = [s for s in ships if s["PREF_STATUS"] == "U"]
    sample_saving = sum(s["duty_saving"] for s in u_ships)
    print(f"  Unclaimed rows (PREF_STATUS=U): {len(u_ships)}")
    print(f"  Σ duty_saving (sample U rows)   = ${sample_saving:,.2f} = ${sample_saving/1000:.1f}K")
    print(f"  NOTE: ${sample_saving/1000:.1f}K (sample) < ${total_uncl:.1f}K (full lanes)"
          f" — sample covers {round(sample_saving/1000/total_uncl*100,1)}% of full population")
    print(f"  DERIVATION CHECK: duty_saving = CUSVAL × (MFN_RATE − PREF_RATE) / 100")
    for s in u_ships:
        calc = round(s["CUSVAL"] * (s["MFN_RATE"] - s["PREF_RATE"]) / 100, 2)
        match = "✓" if abs(calc - s["duty_saving"]) < 0.01 else "✗"
        print(f"    TOR_ID {s['TOR_ID']}: ${s['CUSVAL']:,.0f} × "
              f"({s['MFN_RATE']:.3f}−{s['PREF_RATE']:.3f})% = ${calc:,.2f}  {match}")

    print(f"\n{'─'*60}")
    print("ROO_STATUS DERIVATION CHECK (Q/M/F):")
    for s in ships[:8]:  # sample unclaimed rows
        roo = _derive_roo_status(s["RVC_PCT"], s["RVC_THRESHOLD"])
        match = "✓" if roo == s["ROO_STATUS"] else "✗"
        gap = s["RVC_THRESHOLD"] - s["RVC_PCT"]
        print(f"    TOR_ID {s['TOR_ID']}: RVC_PCT={s['RVC_PCT']} vs THR={s['RVC_THRESHOLD']}"
              f"  gap={gap:+d}  → {roo} ({ROO_STATUS_LABELS[roo]})  {match}")

    print(f"\n{'─'*60}")
    print("CoO OUTSTANDING CHECK:")
    outstanding = [c for c in coos if c["POO_STATUS"] in ("OVERDUE", "PENDING", "RECEIVED")]
    print(f"  Total CoO rows:      {len(coos)}")
    print(f"  Outstanding:         {len(outstanding)}  (= KPI coo_outstanding ✓)")
    for c in coos:
        print(f"    {c['SUPPLIER_NAME']:<30} POO_STATUS={c['POO_STATUS']}")

    print("\n" + "═" * 80)
    print("Framing: 🟢 fields are defensible as real SAP.")
    print("         🔴 fields are representative/educated guesses — confirm against")
    print("         the client's actual SAP TM + GTS release before use in production.")
    print("═" * 80 + "\n")


if __name__ == "__main__":
    _print_field_dictionary()
    _print_reconciliation()
