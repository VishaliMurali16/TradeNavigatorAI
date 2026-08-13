# SAP TM + GTS field dictionary used by the FTA agent's template-download feature.
# Moved here from fta_simulator.py so fta_data_source.py can reference it without
# importing the dead simulator module.
#
# Tuple format: (SAP_NAME, UNIVERSAL_NAME, PROVENANCE, FORMAT, NOTE, SOURCE_SYSTEM, LONG_DESC)
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
