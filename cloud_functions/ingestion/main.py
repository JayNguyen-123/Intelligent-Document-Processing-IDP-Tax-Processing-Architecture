"""Ingestion Cloud Function (2nd gen, CloudEvent / Eventarc GCS 'finalized' trigger).

Flow per uploaded object:
  1. Skip folder placeholders, empty/oversized/unsupported files.
  2. Idempotency: skip if this exact object version (bucket/name/generation) was
     already routed. GCS events are delivered at-least-once.
  3. Extract with Document AI (regional endpoint).
  4. Map to canonical fields + validate + confidence gate (shared/tax_record.py).
  5a. Pass  -> BigQuery row (review_source=AUTOMATED) + verified JSON in GCS.
  5b. Fail  -> HITL queue JSON + Pub/Sub alert.
  Permanent extraction failures (bad file, page limit, ...) are routed to the
  HITL queue with the error so a human can key the values in; transient
  failures are re-raised so Eventarc retries (deploy with --retry).

Requires a copy of shared/tax_record.py next to this file (scripts/sync_shared.sh).
"""
from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict, Optional

import functions_framework
from google.api_core import exceptions as gexc
from google.api_core.client_options import ClientOptions
from google.cloud import bigquery, pubsub_v1, storage
from google.cloud import documentai_v1 as documentai

import tax_record as tr

# ---------------------------------------------------------------------------
# Configuration (fail fast on missing required settings)
# ---------------------------------------------------------------------------
def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


PROJECT_ID = _require("GCP_PROJECT_ID")
DOCAI_LOCATION = os.environ.get("DOCAI_LOCATION", "us")
PROCESSOR_ID = _require("DOCAI_PROCESSOR_ID")
PROCESSOR_VERSION = os.environ.get("DOCAI_PROCESSOR_VERSION", "").strip()  # pin in prod
HITL_BUCKET = _require("HITL_BUCKET_NAME")
VERIFIED_BUCKET = _require("VERIFIED_BUCKET_NAME")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC_NAME", "hitl-alerts")
BQ_DATASET = os.environ.get("BQ_DATASET", "tax_processing_ds")
BQ_TABLE = os.environ.get("BQ_TABLE", "verified_w2_records")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.85"))
MAX_FILE_BYTES = int(os.environ.get("MAX_FILE_BYTES", str(20 * 1024 * 1024)))
FIELD_ALIASES = tr.load_field_aliases()

if not 0.0 < CONFIDENCE_THRESHOLD <= 1.0:
    raise RuntimeError("CONFIDENCE_THRESHOLD must be in (0, 1]")
if HITL_BUCKET == VERIFIED_BUCKET:
    raise RuntimeError("HITL and verified buckets must differ")

# ---------------------------------------------------------------------------
# Structured logging (Cloud Logging parses JSON lines with 'severity').
# Never log extracted field VALUES - they are taxpayer PII.
# ---------------------------------------------------------------------------
_logger = logging.getLogger("ingestion")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_h)
    _logger.propagate = False


def log(severity: str, message: str, **fields: Any) -> None:
    _logger.info(json.dumps({"severity": severity, "message": message, "component": "ingestion", **fields}, default=str))


# ---------------------------------------------------------------------------
# Lazily-created, reused clients (module scope survives warm invocations)
# ---------------------------------------------------------------------------
_clients: Dict[str, Any] = {}


def _storage() -> storage.Client:
    if "gcs" not in _clients:
        _clients["gcs"] = storage.Client(project=PROJECT_ID)
    return _clients["gcs"]


def _docai() -> documentai.DocumentProcessorServiceClient:
    if "docai" not in _clients:
        opts = ClientOptions(api_endpoint=f"{DOCAI_LOCATION}-documentai.googleapis.com")
        _clients["docai"] = documentai.DocumentProcessorServiceClient(client_options=opts)
    return _clients["docai"]


def _bq() -> bigquery.Client:
    if "bq" not in _clients:
        _clients["bq"] = bigquery.Client(project=PROJECT_ID)
    return _clients["bq"]


def _publisher() -> pubsub_v1.PublisherClient:
    if "pub" not in _clients:
        _clients["pub"] = pubsub_v1.PublisherClient()
    return _clients["pub"]


# Errors that will not succeed on retry -> route to a human instead.
PERMANENT_DOCAI_ERRORS = (gexc.InvalidArgument, gexc.FailedPrecondition, gexc.NotFound, gexc.PermissionDenied)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
@functions_framework.cloud_event
def process_tax_upload(cloud_event) -> None:
    data = cloud_event.data or {}
    bucket_name = data.get("bucket")
    object_name = data.get("name")
    generation = data.get("generation")
    size = int(data.get("size") or 0)
    content_type = data.get("contentType")

    if not bucket_name or not object_name:
        log("ERROR", "Malformed storage event; ignoring", event_id=cloud_event["id"])
        return
    if bucket_name in (HITL_BUCKET, VERIFIED_BUCKET):
        log("ERROR", "Trigger misconfigured: event from an output bucket; ignoring", bucket=bucket_name)
        return
    if object_name.endswith("/"):
        return  # folder placeholder

    doc_id = tr.document_id(bucket_name, object_name, generation)
    ctx = {"document_id": doc_id, "object": f"gs://{bucket_name}/{object_name}", "generation": generation}
    out_name = tr.output_object_name(object_name, doc_id)

    # 1. Idempotency -------------------------------------------------------
    state = _existing_route(out_name)
    if state == "final":
        log("INFO", "Duplicate event: document already verified/decided; skipping", **ctx)
        return
    if state == "hitl":
        # Already queued; re-send the alert in case the previous attempt failed after
        # writing the queue item (at-least-once alerting is acceptable).
        log("INFO", "Duplicate event: document already queued; re-sending alert", **ctx)
        payload = json.loads(_storage().bucket(HITL_BUCKET).blob(tr.PREFIX_REVIEW_PENDING + out_name).download_as_bytes())
        _publish_alert(payload, tr.PREFIX_REVIEW_PENDING + out_name)
        return

    # 2. Pre-flight checks -------------------------------------------------
    mime_type = tr.mime_type_for(object_name, content_type)
    precheck_error: Optional[str] = None
    if mime_type is None:
        precheck_error = f"unsupported file type (contentType={content_type})"
    elif size == 0:
        precheck_error = "empty file"
    elif size > MAX_FILE_BYTES:
        precheck_error = f"file is {size} bytes; limit for online processing is {MAX_FILE_BYTES}"

    # 3. Extraction ---------------------------------------------------------
    extracted: Dict[str, Dict[str, Any]] = {}
    error = precheck_error
    if error is None:
        try:
            content = _storage().bucket(bucket_name).blob(object_name).download_as_bytes(if_generation_match=generation)
            extracted = extract_fields(_run_docai(content, mime_type))
        except gexc.PreconditionFailed:
            log("WARNING", "Object was overwritten before processing; newer event will handle it", **ctx)
            return
        except PERMANENT_DOCAI_ERRORS as exc:
            error = f"{type(exc).__name__}: {str(exc)[:500]}"
        # Anything else (ServiceUnavailable, DeadlineExceeded, 429, ...) propagates -> retry.

    # 4. Routing decision ---------------------------------------------------
    canonical, decision = tr.evaluate_extraction(extracted, CONFIDENCE_THRESHOLD, FIELD_ALIASES)
    if error:
        decision.needs_review = True
        decision.reasons.insert(0, "processing_error")
        log("WARNING", "Document could not be extracted automatically; routing to HITL", error=error, **ctx)

    payload = tr.build_payload(
        doc_id=doc_id, source_bucket=bucket_name, source_file=object_name,
        source_generation=generation, mime_type=mime_type, threshold=CONFIDENCE_THRESHOLD,
        extracted=extracted, canonical=canonical, decision=decision, error=error,
    )

    # 5. Route ---------------------------------------------------------------
    if decision.needs_review:
        queue_object = tr.PREFIX_REVIEW_PENDING + out_name
        if _write_json_once(HITL_BUCKET, queue_object, payload):
            log("INFO", "Routed to HITL queue", reasons=decision.reasons,
                low_confidence_fields=decision.low_confidence_fields, **ctx)
        _publish_alert(payload, queue_object)
    else:
        # Warehouse first, then the GCS marker that makes the event idempotent.
        # If BQ fails we raise and retry; insertId=document_id lets BigQuery
        # best-effort de-duplicate, and the Looker view de-dups by document_id.
        row = tr.to_bq_row(
            decision.clean_values, doc_id=doc_id, source_bucket=bucket_name, source_file=object_name,
            review_source=tr.REVIEW_SOURCE_AUTOMATED, reviewed_by=None, low_confidence_fields=[],
        )
        insert_bq_row(row)
        _write_json_once(VERIFIED_BUCKET, tr.PREFIX_AUTOMATED + out_name, payload)
        log("INFO", "Auto-verified and loaded to BigQuery", **ctx)


# ---------------------------------------------------------------------------
# Document AI
# ---------------------------------------------------------------------------
def _run_docai(content: bytes, mime_type: str):
    client = _docai()
    if PROCESSOR_VERSION:
        name = client.processor_version_path(PROJECT_ID, DOCAI_LOCATION, PROCESSOR_ID, PROCESSOR_VERSION)
    else:
        name = client.processor_path(PROJECT_ID, DOCAI_LOCATION, PROCESSOR_ID)
    request = documentai.ProcessRequest(
        name=name, raw_document=documentai.RawDocument(content=content, mime_type=mime_type)
    )
    return client.process_document(request=request, timeout=120).document


def text_from_anchor(anchor, full_text: str) -> str:
    """Concatenate ALL text segments (multi-line values span several segments)."""
    if anchor is None or not getattr(anchor, "text_segments", None):
        return ""
    parts = [full_text[int(seg.start_index or 0):int(seg.end_index)] for seg in anchor.text_segments]
    return "".join(parts)


def _put(fields: Dict[str, Dict[str, Any]], key: str, value: str, confidence: float) -> None:
    """Insert without silently overwriting duplicates (e.g. two Box 1 candidates)."""
    if not key:
        return
    final, n = key, 2
    while final in fields:
        final, n = f"{key}__dup{n}", n + 1
    fields[final] = {"value": value, "confidence": round(float(confidence or 0.0), 4)}


def extract_fields(document) -> Dict[str, Dict[str, Any]]:
    """Flatten a Document AI response into {raw_key: {value, confidence}}.

    Handles specialised processors (entities, incl. nested properties, preferring
    normalized_value) and the generic Form Parser (page.form_fields). The field
    confidence is the MIN of label and value confidence so a misread label can't
    hide behind a confident value.
    """
    fields: Dict[str, Dict[str, Any]] = {}
    full_text = document.text or ""

    def walk(entity, prefix: str = "") -> None:
        key = tr.normalize_key(prefix + entity.type_)
        norm = getattr(entity, "normalized_value", None)
        value = (getattr(norm, "text", "") if norm else "") or entity.mention_text or ""
        if entity.properties:
            for child in entity.properties:
                walk(child, prefix=f"{entity.type_}_")
            if value.strip():
                _put(fields, key, value.strip(), entity.confidence)
        else:
            _put(fields, key, value.strip(), entity.confidence)

    if document.entities:
        for entity in document.entities:
            walk(entity)
        return fields

    for page in document.pages:
        for ff in page.form_fields:
            label = text_from_anchor(ff.field_name.text_anchor, full_text)
            value = text_from_anchor(ff.field_value.text_anchor, full_text).strip()
            conf = min(ff.field_name.confidence or 0.0, ff.field_value.confidence or 0.0)
            _put(fields, tr.normalize_key(label), value, conf)
    return fields


# ---------------------------------------------------------------------------
# Storage / BigQuery / Pub/Sub helpers
# ---------------------------------------------------------------------------
def _existing_route(out_name: str) -> Optional[str]:
    gcs = _storage()
    verified = gcs.bucket(VERIFIED_BUCKET)
    if verified.blob(tr.PREFIX_AUTOMATED + out_name).exists() or verified.blob(tr.PREFIX_HUMAN_VERIFIED + out_name).exists():
        return "final"
    hitl = gcs.bucket(HITL_BUCKET)
    if hitl.blob(tr.PREFIX_REVIEW_PENDING + out_name).exists():
        return "hitl"
    if hitl.blob(tr.PREFIX_REVIEW_REJECTED + out_name).exists():
        return "final"  # a human already rejected it
    return None


def _write_json_once(bucket: str, name: str, payload: Dict[str, Any]) -> bool:
    """Create-only write. Returns False if the object already exists (duplicate run)."""
    blob = _storage().bucket(bucket).blob(name)
    try:
        blob.upload_from_string(json.dumps(payload, indent=2), content_type="application/json", if_generation_match=0)
        return True
    except gexc.PreconditionFailed:
        return False


def insert_bq_row(row: Dict[str, Any]) -> None:
    table = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    errors = _bq().insert_rows_json(table, [row], row_ids=[row["document_id"]])
    if errors:
        # Schema/type errors are not transient; surface loudly and let retry/alerting catch it.
        raise RuntimeError(f"BigQuery insert failed for {row['document_id']}: {errors}")


def _publish_alert(payload: Dict[str, Any], queue_object: str) -> None:
    md = payload["metadata"]
    message = {
        "alert_type": "MANUAL_REVIEW_REQUIRED",
        "document_id": md["document_id"],
        "file_name": md["source_file"],
        "queue_object": f"gs://{HITL_BUCKET}/{queue_object}",
        "review_reasons": md.get("review_reasons", []),
        "flagged_fields": sorted(set(md.get("low_confidence_fields", []) + md.get("missing_fields", [])
                                     + md.get("conflicting_fields", [])
                                     + list(md.get("validation_errors", {}).keys()))),
    }
    publisher = _publisher()
    topic = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    # .result() raises on failure -> function errors -> Eventarc retries -> alert re-sent.
    message_id = publisher.publish(topic, json.dumps(message).encode("utf-8"),
                                   document_id=md["document_id"]).result(timeout=30)
    log("INFO", "HITL alert published", message_id=message_id, document_id=md["document_id"])
