"""Canonical W-2 record model shared by the ingestion function, the HITL dashboard,
the warehouse loader and the local simulator.

Single source of truth for:
  * mapping raw Document AI keys -> canonical fields
  * parsing / validating values (money, EIN, tax year)
  * routing decisions (auto-verify vs. human review)
  * the JSON payload contract written to GCS
  * the BigQuery row contract

Pure standard library on purpose: it must import cleanly inside Cloud Functions,
the Streamlit container and unit tests without any GCP SDK installed.

Deployables receive a copy of this file via ``scripts/sync_shared.sh`` (run by CI
and by the Dockerfile build context), so there is exactly one implementation.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

SCHEMA_VERSION = "2.0"

REVIEW_SOURCE_AUTOMATED = "AUTOMATED"
REVIEW_SOURCE_HUMAN = "HUMAN"

# Object-name prefixes (kept here so every component agrees on them).
PREFIX_AUTOMATED = "automated/"
PREFIX_HUMAN_VERIFIED = "human_verified/"
PREFIX_REVIEW_PENDING = "review_pending/"
PREFIX_REVIEW_REJECTED = "review_rejected/"

# MIME types accepted by Document AI online processing.
SUPPORTED_MIME_TYPES: Dict[str, str] = {
    ".pdf": "application/pdf",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".webp": "image/webp",
}

# ---------------------------------------------------------------------------
# Canonical fields and the raw keys that map to them.
#
# Raw keys are normalised with ``normalize_key`` before lookup, so
# "EmployerEIN", "Employer EIN:" and "employer_ein" all become "employer_ein".
# IMPORTANT: verify these aliases against the entity schema of YOUR processor
# version (Document AI console -> Processor -> "Fields"). Extra aliases can be
# supplied at runtime through the FIELD_ALIASES_JSON environment variable.
# ---------------------------------------------------------------------------
DEFAULT_FIELD_ALIASES: Dict[str, List[str]] = {
    "employer_name": [
        "employer_name", "w2_employer_name", "employers_name",
        "employer_name_address_and_zip_code", "employers_name_address_and_zip_code",
        "c_employers_name_address_and_zip_code",
    ],
    "employer_ein": [
        "employer_ein", "w2_employer_ein", "ein", "employer_identification_number",
        "employers_identification_number", "b_employer_identification_number_ein",
        "employer_id_number",
    ],
    "wages": [
        "wages", "w2_wages", "wages_tips_other_compensation",
        "wages_tips_other_comp", "1_wages_tips_other_compensation", "box_1",
    ],
    "fed_income_tax_withheld": [
        "fed_income_tax_withheld", "w2_fed_income_tax_withheld",
        "federal_income_tax_withheld", "2_federal_income_tax_withheld", "box_2",
    ],
    "tax_year": ["tax_year", "w2_tax_year", "form_year", "year"],
}

REQUIRED_FIELDS: Tuple[str, ...] = (
    "employer_name", "employer_ein", "wages", "fed_income_tax_withheld", "tax_year",
)

FIELD_LABELS: Dict[str, str] = {
    "employer_name": "Employer name (Box c)",
    "employer_ein": "Employer EIN (Box b)",
    "wages": "Wages, tips, other comp. (Box 1)",
    "fed_income_tax_withheld": "Federal income tax withheld (Box 2)",
    "tax_year": "Tax year",
}

# Columns written to BigQuery, in DDL order. Kept in sync with
# warehouse/looker_schema.sql and warehouse/load_to_bigquery.py (unit-tested).
BQ_COLUMNS: Tuple[str, ...] = (
    "document_id", "employer_name", "employer_ein", "wages",
    "fed_income_tax_withheld", "tax_year", "review_source", "reviewed_by",
    "low_confidence_fields", "source_bucket", "source_file", "schema_version",
    "processed_at",
)

_MONEY_MAX = Decimal("1000000000")  # sanity cap: $1B on a single W-2
_CENT = Decimal("0.01")
_SSN_RE = re.compile(r"\b(\d{3})[- ]?(\d{2})[- ]?(\d{4})\b")


class ValidationError(ValueError):
    """Raised when a single field value cannot be parsed into its canonical type."""


# ---------------------------------------------------------------------------
# Key normalisation and alias mapping
# ---------------------------------------------------------------------------
def normalize_key(raw: str) -> str:
    """Normalise any raw label/entity type to snake_case.

    >>> normalize_key("EmployerEIN")
    'employer_ein'
    >>> normalize_key("b Employer identification number (EIN):")
    'b_employer_identification_number_ein'
    """
    if not raw:
        return ""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw.strip())
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()
    return re.sub(r"_+", "_", s)


def load_field_aliases(env_value: Optional[str] = None) -> Dict[str, List[str]]:
    """Default aliases merged with optional JSON overrides ({"wages": ["my_key"]})."""
    aliases = {k: list(v) for k, v in DEFAULT_FIELD_ALIASES.items()}
    raw = env_value if env_value is not None else os.environ.get("FIELD_ALIASES_JSON", "")
    if raw:
        extra = json.loads(raw)
        if not isinstance(extra, dict):
            raise ValueError("FIELD_ALIASES_JSON must be a JSON object")
        for canonical, keys in extra.items():
            if canonical not in aliases:
                raise ValueError(f"Unknown canonical field in FIELD_ALIASES_JSON: {canonical}")
            aliases[canonical].extend(normalize_key(k) for k in keys)
    return aliases


def _reverse_aliases(aliases: Mapping[str, List[str]]) -> Dict[str, str]:
    reverse: Dict[str, str] = {}
    for canonical, keys in aliases.items():
        for k in keys:
            reverse[normalize_key(k)] = canonical
    return reverse


# ---------------------------------------------------------------------------
# Value parsers
# ---------------------------------------------------------------------------
def parse_money(value: Any) -> Decimal:
    """Parse a currency amount exactly (Decimal, never float).

    Accepts "$12,345.6", "12345", "12 345.00". Rejects OCR artefacts such as
    "125OO.00", negatives and implausible magnitudes.
    """
    if value is None:
        raise ValidationError("value is empty")
    s = str(value).strip().replace("$", "").replace(",", "").replace(" ", "")
    if not s:
        raise ValidationError("value is empty")
    if not re.fullmatch(r"\d+(\.\d{1,2})?", s):
        raise ValidationError(f"'{value}' is not a valid dollar amount")
    try:
        amount = Decimal(s).quantize(_CENT, rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:  # pragma: no cover - regex already guards
        raise ValidationError(f"'{value}' is not a valid dollar amount") from exc
    if amount >= _MONEY_MAX:
        raise ValidationError(f"'{value}' exceeds the plausible maximum")
    return amount


def parse_ein(value: Any) -> str:
    """Return EIN in canonical NN-NNNNNNN form."""
    digits = re.sub(r"[\s-]", "", str(value or ""))
    if not re.fullmatch(r"\d{9}", digits):
        raise ValidationError(f"'{value}' is not a 9-digit EIN (NN-NNNNNNN)")
    return f"{digits[:2]}-{digits[2:]}"


def parse_tax_year(value: Any, now: Optional[datetime] = None) -> int:
    now = now or datetime.now(timezone.utc)
    s = str(value or "").strip()
    if not re.fullmatch(r"\d{4}", s):
        raise ValidationError(f"'{value}' is not a 4-digit year")
    year = int(s)
    if not (2000 <= year <= now.year):
        raise ValidationError(f"tax year {year} is outside 2000..{now.year}")
    return year


def parse_text(value: Any, max_len: int = 200) -> str:
    s = re.sub(r"\s+", " ", str(value or "")).strip()
    if not s:
        raise ValidationError("value is empty")
    if len(s) > max_len:
        raise ValidationError(f"value longer than {max_len} characters")
    return s


PARSERS: Dict[str, Callable[[Any], Any]] = {
    "employer_name": parse_text,
    "employer_ein": parse_ein,
    "wages": parse_money,
    "fed_income_tax_withheld": parse_money,
    "tax_year": parse_tax_year,
}


def validate_record(values: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Validate canonical field values.

    Returns (clean_values, errors). ``errors`` maps field -> human message; the
    special key ``_record`` holds cross-field errors.
    """
    clean: Dict[str, Any] = {}
    errors: Dict[str, str] = {}
    for fname, parser in PARSERS.items():
        raw = values.get(fname)
        if raw is None or str(raw).strip() == "":
            if fname in REQUIRED_FIELDS:
                errors[fname] = "required field is missing"
            continue
        try:
            clean[fname] = parser(raw)
        except ValidationError as exc:
            errors[fname] = str(exc)

    wages, withheld = clean.get("wages"), clean.get("fed_income_tax_withheld")
    if wages is not None and withheld is not None and withheld > wages:
        errors["_record"] = "federal tax withheld (Box 2) exceeds wages (Box 1)"
    return clean, errors


# ---------------------------------------------------------------------------
# Extraction -> canonical mapping and routing
# ---------------------------------------------------------------------------
def to_canonical(
    extracted: Mapping[str, Mapping[str, Any]],
    aliases: Optional[Mapping[str, List[str]]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """Map raw extracted keys onto canonical fields.

    Returns (canonical, conflicts). When several raw keys map to the same
    canonical field with *different* values, the field is reported as a
    conflict (and the highest-confidence candidate is kept for display).
    """
    reverse = _reverse_aliases(aliases or DEFAULT_FIELD_ALIASES)
    candidates: Dict[str, List[Dict[str, Any]]] = {}
    for raw_key, attrs in extracted.items():
        base_key = normalize_key(raw_key.split("__dup")[0])
        canonical = reverse.get(base_key)
        if not canonical:
            continue
        candidates.setdefault(canonical, []).append({
            "value": str(attrs.get("value", "") or "").strip(),
            "confidence": float(attrs.get("confidence") or 0.0),
            "source_key": raw_key,
        })

    canonical_out: Dict[str, Dict[str, Any]] = {}
    conflicts: List[str] = []
    for fname, cands in candidates.items():
        cands.sort(key=lambda c: c["confidence"], reverse=True)
        canonical_out[fname] = cands[0]
        distinct = {re.sub(r"\s+", " ", c["value"]).lower() for c in cands if c["value"]}
        if len(distinct) > 1:
            conflicts.append(fname)
    return canonical_out, sorted(conflicts)


@dataclass
class RoutingDecision:
    needs_review: bool
    reasons: List[str] = field(default_factory=list)
    low_confidence_fields: List[str] = field(default_factory=list)
    missing_fields: List[str] = field(default_factory=list)
    validation_errors: Dict[str, str] = field(default_factory=dict)
    conflicting_fields: List[str] = field(default_factory=list)
    clean_values: Dict[str, Any] = field(default_factory=dict)


def evaluate_extraction(
    extracted: Mapping[str, Mapping[str, Any]],
    threshold: float,
    aliases: Optional[Mapping[str, List[str]]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], RoutingDecision]:
    """Decide whether a document can be auto-verified.

    A document is auto-verified only if ALL of the following hold:
      * every required canonical field was extracted,
      * every required canonical field meets the confidence threshold,
      * every value passes type/format validation and cross-field checks,
      * no canonical field has conflicting candidate values.

    Anything else (including "nothing extracted at all") goes to human review.
    Low-confidence *non-canonical* fields (e.g. a stray checkbox picked up by
    the Form Parser) are not loaded to the warehouse and do not gate routing.
    """
    if not 0.0 < threshold <= 1.0:
        raise ValueError("confidence threshold must be in (0, 1]")

    canonical, conflicts = to_canonical(extracted, aliases)
    decision = RoutingDecision(needs_review=False, conflicting_fields=conflicts)

    if not extracted:
        decision.reasons.append("no_fields_extracted")

    decision.missing_fields = [f for f in REQUIRED_FIELDS if not canonical.get(f, {}).get("value")]
    if decision.missing_fields:
        decision.reasons.append("missing_required_fields")

    decision.low_confidence_fields = sorted(
        f for f, attrs in canonical.items() if attrs["confidence"] < threshold
    )
    if decision.low_confidence_fields:
        decision.reasons.append("low_confidence")

    clean, errors = validate_record({f: a["value"] for f, a in canonical.items()})
    # Missing fields are already reported; keep only real format/cross-field errors.
    errors = {k: v for k, v in errors.items() if k not in decision.missing_fields}
    if errors:
        decision.reasons.append("validation_failed")
    decision.validation_errors = errors
    decision.clean_values = clean

    if conflicts:
        decision.reasons.append("conflicting_values")

    decision.needs_review = bool(decision.reasons)
    return canonical, decision


# ---------------------------------------------------------------------------
# Identity, naming and payload contracts
# ---------------------------------------------------------------------------
def document_id(bucket: str, name: str, generation: Any) -> str:
    """Deterministic id for one *version* of one uploaded object.

    Including the GCS generation means a re-upload under the same name is a new
    document, while a duplicate event delivery for the same upload is not.
    """
    digest = hashlib.sha256(f"gs://{bucket}/{name}#{generation}".encode("utf-8"))
    return digest.hexdigest()[:32]


def output_object_name(source_name: str, doc_id: str) -> str:
    """Collision-free, path-safe JSON name (keeps the original extension in the stem)."""
    base = os.path.basename(source_name)
    safe = re.sub(r"[^0-9A-Za-z._-]+", "_", base)[:120] or "document"
    return f"{safe}__{doc_id[:16]}.json"


def mime_type_for(name: str, content_type: Optional[str] = None) -> Optional[str]:
    ext = os.path.splitext(name)[1].lower()
    if ext in SUPPORTED_MIME_TYPES:
        return SUPPORTED_MIME_TYPES[ext]
    if content_type and content_type.split(";")[0].strip() in SUPPORTED_MIME_TYPES.values():
        return content_type.split(";")[0].strip()
    return None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_payload(
    *,
    doc_id: str,
    source_bucket: str,
    source_file: str,
    source_generation: Any,
    mime_type: Optional[str],
    threshold: float,
    extracted: Mapping[str, Any],
    canonical: Mapping[str, Any],
    decision: RoutingDecision,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "metadata": {
            "document_id": doc_id,
            "source_bucket": source_bucket,
            "source_file": source_file,
            "source_generation": str(source_generation),
            "mime_type": mime_type,
            "processed_at": utc_now_iso(),
            "confidence_threshold": threshold,
            "needs_manual_review": decision.needs_review,
            "review_reasons": decision.reasons,
            "low_confidence_fields": decision.low_confidence_fields,
            "missing_fields": decision.missing_fields,
            "validation_errors": decision.validation_errors,
            "conflicting_fields": decision.conflicting_fields,
        },
        "extracted_data": dict(extracted),
        "canonical_data": dict(canonical),
    }
    if error:
        payload["metadata"]["processing_error"] = error
    if not decision.needs_review:
        payload["verified_record"] = serialize_clean(decision.clean_values)
    return payload


def serialize_clean(clean: Mapping[str, Any]) -> Dict[str, Any]:
    """JSON-safe representation. Money stays a string to preserve exact cents."""
    out: Dict[str, Any] = {}
    for k, v in clean.items():
        out[k] = format(v, "f") if isinstance(v, Decimal) else v
    return out


def to_bq_row(
    clean: Mapping[str, Any],
    *,
    doc_id: str,
    source_bucket: str,
    source_file: str,
    review_source: str,
    reviewed_by: Optional[str],
    low_confidence_fields: List[str],
    processed_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Row for ``insert_rows_json``. NUMERIC columns are sent as strings (exact)."""
    if review_source not in (REVIEW_SOURCE_AUTOMATED, REVIEW_SOURCE_HUMAN):
        raise ValueError(f"invalid review_source: {review_source}")
    s = serialize_clean(clean)
    row = {
        "document_id": doc_id,
        "employer_name": s.get("employer_name"),
        "employer_ein": s.get("employer_ein"),
        "wages": s.get("wages"),
        "fed_income_tax_withheld": s.get("fed_income_tax_withheld"),
        "tax_year": s.get("tax_year"),
        "review_source": review_source,
        "reviewed_by": reviewed_by,
        "low_confidence_fields": list(low_confidence_fields),
        "source_bucket": source_bucket,
        "source_file": source_file,
        "schema_version": SCHEMA_VERSION,
        "processed_at": processed_at or utc_now_iso(),
    }
    assert tuple(row) == BQ_COLUMNS
    return row


def mask_pii(text: Any) -> str:
    """Mask SSN-shaped values for display/logging (keeps last 4)."""
    return _SSN_RE.sub(lambda m: f"***-**-{m.group(3)}", str(text or ""))
