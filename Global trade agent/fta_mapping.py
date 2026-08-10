"""
fta_mapping.py — intelligent column mapping for FTA file uploads.

Provides:
  1. Local keyword/synonym matching (zero-dependency, instant fallback)
  2. LLM-based mapping via a provided async _call_ai function (Qwen/Qwen3-32B)
  3. Sanity checks on sample values per SAP field

All required SAP fields are declared here; fta_simulator.FIELD_DICTIONARY is
the single source of truth for descriptions (imported lazily to avoid circulars).
"""

from __future__ import annotations

import json
import re
from typing import Any


# ── Required SAP fields and their human-readable descriptions for LLM prompts ──
REQUIRED_FIELDS: list[tuple[str, str]] = [
    ("TOR_ID",               "Freight order / shipment identifier"),
    ("PRODUCT_TEXT",         "Product or material description (free text)"),
    ("CCNGN",                "HS / tariff / commodity code (e.g. 8708.29.00)"),
    ("CTYDP",                "Country of departure / export — 2-letter ISO code"),
    ("CTYAR",                "Country of destination / import — 2-letter ISO code"),
    ("CUCOO",                "Country of origin — 2-letter ISO code"),
    ("CUSVAL",               "Customs / declared value (numeric, e.g. USD 42000)"),
    ("WAERS",                "Currency code (e.g. USD, EUR, SGD)"),
    ("AGREEMENT",            "FTA / trade agreement name (e.g. USMCA, RCEP, CPTPP)"),
    ("MFN_RATE",             "MFN / WTO duty rate as a percentage (e.g. 7.5)"),
    ("PREF_RATE",            "Preferential / FTA duty rate as a percentage (e.g. 0.0)"),
    ("PREF_STATUS",          "Preference status code: E=claimed, U=eligible-unclaimed, N=not-eligible"),
    ("RVC_PCT",              "Regional value content % (actual, e.g. 68.0) — optional"),
    ("RVC_THRESHOLD",        "Required RVC threshold % (e.g. 60.0) — optional"),
    ("ROO_STATUS",           "Rules-of-origin status: Q=qualified, M=near-miss, F=fail — optional"),
    ("SUPPLIER_NAME",        "Supplier or vendor name — optional"),
    ("BOM_REGIONAL_CONTENT", "Bill-of-materials regional content % — optional"),
    ("ENTRY_DATE",           "Customs entry date (YYYYMMDD or YYYY-MM-DD)"),
]

# Derived set for quick membership tests
_REQUIRED_SAP = {r[0] for r in REQUIRED_FIELDS}

# ── Optional fields — dashboard degrades gracefully if absent ─────────────────
OPTIONAL_FIELDS = {
    "RVC_PCT", "RVC_THRESHOLD", "ROO_STATUS",
    "SUPPLIER_NAME", "BOM_REGIONAL_CONTENT",
    "CUCOO", "WAERS",
}

# ── Keyword/synonym dictionary for local matching ─────────────────────────────
_SYNONYMS: dict[str, list[str]] = {
    "TOR_ID": [
        "tor_id", "tor id", "freight order", "shipment id", "shipment no",
        "order id", "order no", "order number", "freight id", "freight no",
        "reference no", "ref no", "reference", "bill no", "consignment",
        "transport order", "tor",
    ],
    "PRODUCT_TEXT": [
        "product", "description", "item", "material", "goods", "article",
        "commodity desc", "product description", "material description",
        "item description", "goods description", "product name", "item name",
        "product text",
    ],
    "CCNGN": [
        "hs", "hs code", "hscode", "tariff", "tariff code", "commodity code",
        "hts", "hs_code", "tariff_code", "customs code", "commodity",
        "schedule b", "classification", "ccngn", "tariff number", "hs number",
    ],
    "CTYDP": [
        "departure", "dep", "ctydp", "from", "ship from", "export country",
        "country from", "from country", "origin country", "exporting country",
        "country of export", "country of departure",
    ],
    "CTYAR": [
        "destination", "dest", "ctyar", "arrival", "import country", "ship to",
        "to country", "receiving country", "importing country", "country to",
        "country of import", "country of destination", "country of arrival",
    ],
    "CUCOO": [
        "country of origin", "coo", "made in", "cucoo", "mfg country",
        "manufactured in", "production country", "origin", "country origin",
        "manufacturer country",
    ],
    "CUSVAL": [
        "value", "customs value", "declared value", "invoice value", "amount",
        "transaction value", "customs amount", "cargo value", "cusval",
        "fob", "cif", "invoice amount", "shipment value", "goods value",
        "dutiable value", "customs val",
    ],
    "WAERS": [
        "currency", "curr", "ccy", "waers", "currency code", "cur",
        "currency type",
    ],
    "AGREEMENT": [
        "fta", "trade agreement", "agreement", "treaty", "preference",
        "free trade agreement", "agreement name", "fta name", "trade deal",
        "preferential agreement",
    ],
    "MFN_RATE": [
        "mfn", "mfn rate", "mfn duty", "wto rate", "standard rate",
        "tariff rate", "duty rate", "normal rate", "base rate",
        "most favoured", "most-favoured", "mfn tariff", "regular duty",
        "non-pref rate", "non pref rate", "standard duty",
    ],
    "PREF_RATE": [
        "pref rate", "preferential rate", "preference rate", "reduced rate",
        "fta rate", "fta duty", "pref duty", "preferential duty",
        "preferential tariff", "concessional rate", "concessional duty",
    ],
    "PREF_STATUS": [
        "pref status", "preference status", "eligibility status", "eligible",
        "preference", "pref", "preference code", "claim status",
        "preferential status", "fta status",
    ],
    "RVC_PCT": [
        "rvc", "rvc pct", "rvc percent", "rvc %", "regional content",
        "regional value", "regional value content", "content pct",
        "rvc_pct", "regional content %", "content percent",
    ],
    "RVC_THRESHOLD": [
        "threshold", "rvc threshold", "min content", "required content",
        "rvc_threshold", "content threshold", "required rvc",
        "minimum rvc", "rvc minimum", "content requirement",
    ],
    "ROO_STATUS": [
        "roo", "roo status", "rules of origin", "origin status",
        "qualification status", "roo_status", "ro status", "rules-of-origin",
    ],
    "SUPPLIER_NAME": [
        "supplier", "vendor", "manufacturer", "producer", "seller",
        "supplier name", "vendor name", "company", "manufacturer name",
        "supplier company",
    ],
    "BOM_REGIONAL_CONTENT": [
        "bom", "bom content", "bom regional", "bill of materials",
        "material content", "bom_regional_content", "bom regional content",
        "bill of materials content",
    ],
    "ENTRY_DATE": [
        "entry date", "date", "import date", "customs date",
        "declaration date", "shipment date", "entry_date",
        "transaction date", "clearance date", "filing date",
        "customs entry date", "import entry date",
    ],
}


def _normalize(s: str) -> str:
    """Lower-case, strip, collapse whitespace and underscores."""
    return re.sub(r"[\s_]+", " ", str(s).lower().strip())


def local_map(upload_cols: list[str]) -> dict[str, str | None]:
    """
    Map required SAP fields → best uploaded column via keyword/synonym matching.
    Returns {SAP_NAME: matched_col_or_None, ...} for every field in REQUIRED_FIELDS.
    Each column is used at most once (greedy, ordered by REQUIRED_FIELDS).
    """
    # normalised → original column name
    norm_to_orig = {_normalize(c): c for c in upload_cols}

    result: dict[str, str | None] = {}
    used: set[str] = set()  # original col names already claimed

    for sap_name, _ in REQUIRED_FIELDS:
        synonyms = _SYNONYMS.get(sap_name, [])
        best_orig: str | None = None
        best_score: int = 0

        for syn in synonyms:
            norm_syn = _normalize(syn)

            # Exact normalised match — immediate win
            if norm_syn in norm_to_orig:
                candidate = norm_to_orig[norm_syn]
                if candidate not in used:
                    best_orig = candidate
                    best_score = 1000
                    break

            # Containment match — score by overlap length
            for nc, oc in norm_to_orig.items():
                if oc in used:
                    continue
                if norm_syn in nc or nc in norm_syn:
                    score = min(len(norm_syn), len(nc))
                    if score > best_score:
                        best_score = score
                        best_orig = oc

        # Require a minimum overlap of 3 chars to avoid spurious matches
        if best_orig is not None and best_score >= 3:
            result[sap_name] = best_orig
            used.add(best_orig)
        else:
            result[sap_name] = None

    return result


async def llm_map(
    upload_cols: list[str],
    samples: dict[str, list],
    call_ai_fn,   # async (system: str, user: str) -> str
) -> dict[str, str | None]:
    """
    Use the LLM to suggest a column mapping.
    Returns {SAP_NAME: col_or_None, ...}.
    Falls back to local_map() on any error.
    """
    # Build column description block
    col_lines: list[str] = []
    for col in upload_cols:
        vals = [str(v) for v in (samples.get(col) or []) if str(v) not in ("nan", "", "None")][:4]
        sample_str = ", ".join(f'"{v}"' for v in vals) if vals else "(no samples)"
        col_lines.append(f'  "{col}": [{sample_str}]')

    field_lines: list[str] = []
    for sap_name, desc in REQUIRED_FIELDS:
        field_lines.append(f'  "{sap_name}": "{desc}"')

    system = (
        "You are a precise data-mapping assistant for trade compliance software. "
        "Return ONLY valid compact JSON with no explanation, markdown, or code fences. "
        "Do not include any text before or after the JSON object."
    )
    user = (
        "A user uploaded a CSV/Excel file with these columns and sample values:\n"
        "{\n" + "\n".join(col_lines) + "\n}\n\n"
        "Map each required SAP field to the most likely matching uploaded column.\n"
        "Required SAP fields and their meanings:\n"
        "{\n" + "\n".join(field_lines) + "\n}\n\n"
        'Return a single JSON object like: {"SAP_FIELD": "uploaded_col_name_or_null", ...}\n'
        "Include ALL SAP fields. Use null for fields with no confident match.\n"
        "Each uploaded column may only be used once. Prioritise exact or near-exact matches."
    )

    try:
        raw = await call_ai_fn(system, user)

        # Strip <think>...</think> blocks (Qwen3 extended thinking)
        raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
        # Strip markdown code fences
        raw = re.sub(r"```[a-z]*\n?", "", raw).strip().rstrip("`")
        # Extract the first JSON object
        m = re.search(r"\{[^{}]*\}", raw, flags=re.DOTALL)
        if not m:
            return local_map(upload_cols)

        proposed: dict[str, Any] = json.loads(m.group())

        valid_cols = set(upload_cols)
        cleaned: dict[str, str | None] = {}
        used: set[str] = set()

        for sap_name, _ in REQUIRED_FIELDS:
            col = proposed.get(sap_name)
            if col is None or str(col).lower() == "null":
                cleaned[sap_name] = None
            elif col in valid_cols and col not in used:
                cleaned[sap_name] = col
                used.add(col)
            else:
                cleaned[sap_name] = None   # LLM hallucinated a column name

        return cleaned

    except Exception:
        return local_map(upload_cols)


def extract_samples(df, max_rows: int = 5) -> dict[str, list[str]]:
    """Return up to max_rows non-null sample values per column as strings."""
    samples: dict[str, list[str]] = {}
    for col in df.columns:
        vals = (
            df[col]
            .dropna()
            .astype(str)
            .loc[lambda s: s.str.strip() != ""]
            .head(max_rows)
            .tolist()
        )
        samples[str(col)] = vals
    return samples


def sanity_warnings(
    mapping: dict[str, str | None],
    samples: dict[str, list[str]],
) -> dict[str, str]:
    """
    Return {SAP_NAME: warning_text} for mapped columns whose sample values
    look suspicious for the field they are mapped to.
    """
    warns: dict[str, str] = {}

    for sap_name, col in mapping.items():
        if not col:
            continue
        vals = [v for v in (samples.get(col) or []) if v and v.lower() not in ("nan", "", "none")]
        if not vals:
            continue

        if sap_name == "ENTRY_DATE":
            date_ok = sum(
                1 for v in vals
                if re.fullmatch(r"\d{8}", v.strip()) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", v.strip())
            )
            if date_ok < len(vals) * 0.5:
                warns[sap_name] = f"Expected YYYYMMDD or YYYY-MM-DD — samples: {', '.join(vals[:3])}"

        elif sap_name in ("CTYDP", "CTYAR", "CUCOO"):
            iso_ok = sum(1 for v in vals if re.fullmatch(r"[A-Za-z]{2}", v.strip()))
            if iso_ok < len(vals) * 0.5:
                warns[sap_name] = f"Expected 2-letter ISO country code — samples: {', '.join(vals[:3])}"

        elif sap_name in ("MFN_RATE", "PREF_RATE", "RVC_PCT",
                          "RVC_THRESHOLD", "BOM_REGIONAL_CONTENT"):
            def _try_float(v: str) -> bool:
                try:
                    float(v.replace("%", "").strip())
                    return True
                except ValueError:
                    return False

            num_ok = sum(1 for v in vals if _try_float(v))
            if num_ok < len(vals) * 0.5:
                warns[sap_name] = f"Expected numeric percentage — samples: {', '.join(vals[:3])}"

        elif sap_name == "CUSVAL":
            def _try_val(v: str) -> bool:
                try:
                    float(v.replace(",", "").replace("$", "").strip())
                    return True
                except ValueError:
                    return False

            num_ok = sum(1 for v in vals if _try_val(v))
            if num_ok < len(vals) * 0.5:
                warns[sap_name] = f"Expected numeric monetary value — samples: {', '.join(vals[:3])}"

        elif sap_name == "PREF_STATUS":
            valid = {"e", "u", "n"}
            bad = [v for v in vals if v.strip().lower() not in valid]
            if bad:
                warns[sap_name] = (
                    f"Values will be normalised — expected E/U/N, got: {', '.join(bad[:3])}"
                )

        elif sap_name == "ROO_STATUS":
            valid = {"q", "m", "f", "qualified", "near-miss", "fail"}
            bad = [v for v in vals if v.strip().lower() not in valid]
            if bad:
                warns[sap_name] = (
                    f"Expected Q/M/F (Qualified/Near-Miss/Fail) — samples: {', '.join(bad[:3])}"
                )

    return warns
