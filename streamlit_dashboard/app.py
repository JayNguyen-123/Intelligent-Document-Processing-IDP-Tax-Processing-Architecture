"""Human-in-the-loop (HITL) review console for low-confidence tax extractions.

Production behaviours:
  * Authentication: must sit behind Identity-Aware Proxy. The IAP JWT is
    cryptographically verified; the reviewer's email is recorded on every decision.
  * Side-by-side review: original document (from the raw bucket) next to the
    editable canonical fields.
  * Server-side validation (shared/tax_record.py) before anything is written -
    money is parsed as Decimal, EIN/tax-year formats enforced, Box 2 <= Box 1.
  * Concurrency-safe: a reviewer "claims" a task with a GCS generation-match
    write, so two reviewers can never both approve/reject the same document.
  * Ordering: claim -> BigQuery (insertId=document_id) -> verified JSON -> delete
    queue item. A crash mid-way leaves a claimed task that can be safely re-run.
  * PII: SSN-shaped values are masked in the raw-extraction view; values are
    never logged.
"""
from __future__ import annotations

import base64
import json
import os
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from google.api_core import exceptions as gexc
from google.cloud import bigquery, storage

import tax_record as tr

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
HITL_BUCKET_NAME = os.environ.get("HITL_BUCKET_NAME", "")
VERIFIED_BUCKET_NAME = os.environ.get("VERIFIED_BUCKET_NAME", "")
BQ_DATASET = os.environ.get("BQ_DATASET", "tax_processing_ds")
BQ_TABLE = os.environ.get("BQ_TABLE", "verified_w2_records")
IAP_AUDIENCE = os.environ.get("IAP_AUDIENCE", "").strip()
DEV_MODE = os.environ.get("DEV_MODE", "false").lower() == "true"  # local only - never in Cloud Run
MAX_PREVIEW_BYTES = int(os.environ.get("MAX_PREVIEW_BYTES", str(15 * 1024 * 1024)))

st.set_page_config(page_title="IDP Tax HITL Console", layout="wide", page_icon="🔍")

_missing = [n for n, v in {"GCP_PROJECT_ID": PROJECT_ID, "HITL_BUCKET_NAME": HITL_BUCKET_NAME,
                           "VERIFIED_BUCKET_NAME": VERIFIED_BUCKET_NAME}.items() if not v]
if _missing:
    st.error(f"Server misconfigured - missing environment variables: {', '.join(_missing)}")
    st.stop()


# ---------------------------------------------------------------------------
# Authentication (IAP)
# ---------------------------------------------------------------------------
@st.cache_resource
def _google_request():
    from google.auth.transport import requests as ga_requests
    return ga_requests.Request()


def current_reviewer() -> Optional[str]:
    """Return the verified reviewer email, or None if the request is unauthenticated."""
    if IAP_AUDIENCE:
        token = st.context.headers.get("X-Goog-IAP-JWT-Assertion")
        if not token:
            return None
        try:
            from google.oauth2 import id_token
            claims = id_token.verify_token(
                token, _google_request(), audience=IAP_AUDIENCE,
                certs_url="https://www.gstatic.com/iap/verify/public_key",
            )
            return claims.get("email")
        except Exception:  # noqa: BLE001 - any verification failure = unauthenticated
            return None
    if DEV_MODE:
        return os.environ.get("DEV_REVIEWER_EMAIL", "local-dev@example.com")
    return None


REVIEWER = current_reviewer()
if not REVIEWER:
    st.error("Access denied: this console must be accessed through Identity-Aware Proxy.")
    st.stop()


# ---------------------------------------------------------------------------
# Clients and data access
# ---------------------------------------------------------------------------
@st.cache_resource
def get_clients() -> Tuple[storage.Client, bigquery.Client]:
    return storage.Client(project=PROJECT_ID), bigquery.Client(project=PROJECT_ID)


storage_client, bq_client = get_clients()


@st.cache_data(ttl=20, show_spinner=False)
def list_pending() -> List[str]:
    blobs = storage_client.list_blobs(HITL_BUCKET_NAME, prefix=tr.PREFIX_REVIEW_PENDING)
    return sorted(b.name for b in blobs if b.name.endswith(".json"))


def load_task(name: str) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    blob = storage_client.bucket(HITL_BUCKET_NAME).get_blob(name)
    if blob is None:
        return None, None
    return json.loads(blob.download_as_bytes(if_generation_match=blob.generation)), blob.generation


def load_source_document(md: Dict[str, Any]) -> Tuple[Optional[bytes], Optional[str]]:
    try:
        bucket = storage_client.bucket(md["source_bucket"])
        blob = bucket.get_blob(md["source_file"], generation=int(md["source_generation"]))
        if blob is None:
            return None, "original document not found (deleted or lifecycle-expired)"
        if (blob.size or 0) > MAX_PREVIEW_BYTES:
            return None, "original document too large to preview"
        return blob.download_as_bytes(), None
    except (gexc.GoogleAPICallError, KeyError, ValueError) as exc:
        return None, f"cannot load original document: {exc}"


def claim_task(name: str, payload: Dict[str, Any], generation: int, action: str) -> int:
    """Compare-and-swap the queue object to record who is acting on it.

    Raises gexc.PreconditionFailed if anyone else changed it since we loaded it.
    """
    payload = json.loads(json.dumps(payload))
    payload["metadata"]["review_state"] = {"action": action, "by": REVIEWER, "at": tr.utc_now_iso()}
    blob = storage_client.bucket(HITL_BUCKET_NAME).blob(name)
    blob.upload_from_string(json.dumps(payload, indent=2), content_type="application/json",
                            if_generation_match=generation)
    return blob.generation


def finish_task(name: str, generation: int) -> None:
    try:
        storage_client.bucket(HITL_BUCKET_NAME).blob(name).delete(if_generation_match=generation)
    except (gexc.NotFound, gexc.PreconditionFailed):
        pass  # already finished by a re-run


def flash(kind: str, message: str) -> None:
    st.session_state["_flash"] = (kind, message)


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------
def approve(name: str, payload: Dict[str, Any], generation: int, clean: Dict[str, Any],
            submitted: Dict[str, str]) -> None:
    md = payload["metadata"]
    doc_id = md["document_id"]
    new_gen = claim_task(name, payload, generation, "approve")

    row = tr.to_bq_row(
        clean, doc_id=doc_id, source_bucket=md["source_bucket"], source_file=md["source_file"],
        review_source=tr.REVIEW_SOURCE_HUMAN, reviewed_by=REVIEWER,
        low_confidence_fields=md.get("low_confidence_fields", []),
    )
    errors = bq_client.insert_rows_json(f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}", [row], row_ids=[doc_id])
    if errors:
        raise RuntimeError(f"BigQuery rejected the row: {errors}")

    original = {f: (payload.get("canonical_data", {}).get(f) or {}).get("value", "") for f in tr.PARSERS}
    verified = json.loads(json.dumps(payload))
    verified["metadata"].pop("review_state", None)
    verified["metadata"].update({"needs_manual_review": False})
    verified["review"] = {
        "decision": "approved",
        "reviewed_by": REVIEWER,
        "reviewed_at": tr.utc_now_iso(),
        "corrections": {f: {"before": original.get(f, ""), "after": submitted.get(f, "")}
                        for f in tr.PARSERS if (original.get(f) or "") != (submitted.get(f) or "")},
    }
    verified["verified_record"] = tr.serialize_clean(clean)
    out_name = name.replace(tr.PREFIX_REVIEW_PENDING, tr.PREFIX_HUMAN_VERIFIED, 1)
    storage_client.bucket(VERIFIED_BUCKET_NAME).blob(out_name).upload_from_string(
        json.dumps(verified, indent=2), content_type="application/json")
    finish_task(name, new_gen)


def reject(name: str, payload: Dict[str, Any], generation: int, reason: str) -> None:
    new_gen = claim_task(name, payload, generation, "reject")
    rejected = json.loads(json.dumps(payload))
    rejected["metadata"].pop("review_state", None)
    rejected["review"] = {"decision": "rejected", "reason": reason,
                          "reviewed_by": REVIEWER, "reviewed_at": tr.utc_now_iso()}
    out_name = name.replace(tr.PREFIX_REVIEW_PENDING, tr.PREFIX_REVIEW_REJECTED, 1)
    storage_client.bucket(HITL_BUCKET_NAME).blob(out_name).upload_from_string(
        json.dumps(rejected, indent=2), content_type="application/json")
    finish_task(name, new_gen)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.title("🔍 IDP Tax Document Review")
st.caption(f"Signed in as **{REVIEWER}**")

if "_flash" in st.session_state:
    kind, msg = st.session_state.pop("_flash")
    getattr(st, kind)(msg)

try:
    pending = list_pending()
except gexc.GoogleAPICallError as exc:
    st.error(f"Cannot read the review queue: {exc.message}")
    st.stop()

if not pending:
    st.success("Queue is empty - no documents are waiting for review.")
    if st.button("Refresh"):
        list_pending.clear()
        st.rerun()
    st.stop()

st.sidebar.header(f"Pending ({len(pending)})")
selected = st.sidebar.selectbox("Document", pending, format_func=lambda n: n.removeprefix(tr.PREFIX_REVIEW_PENDING))
if st.sidebar.button("Refresh queue"):
    list_pending.clear()
    st.rerun()

payload, generation = load_task(selected)
if payload is None:
    list_pending.clear()
    flash("info", "That document was just completed by another reviewer.")
    st.rerun()

md = payload.get("metadata", {})
canonical = payload.get("canonical_data", {})
doc_id = md.get("document_id", selected)

if md.get("review_state"):
    rs = md["review_state"]
    st.warning(f"This task was claimed by {rs.get('by')} at {rs.get('at')} ({rs.get('action')}) but not "
               "completed - probably an interrupted run. Re-submitting is safe.")

c1, c2, c3 = st.columns(3)
c1.info(f"**Source:** `{md.get('source_file')}`")
c2.warning(f"**Reasons:** {', '.join(md.get('review_reasons', [])) or 'n/a'}")
c3.write(f"**Document ID:** `{doc_id}`")
if md.get("processing_error"):
    st.error(f"Automatic extraction failed: {md['processing_error']} - key values in from the document.")
for fname, msg in (md.get("validation_errors") or {}).items():
    st.caption(f"⚠️ Pipeline validation: **{fname}** - {msg}")

left, right = st.columns([1, 1])

with left:
    st.subheader("Original document")
    content, err = load_source_document(md)
    if err:
        st.warning(err)
    elif md.get("mime_type") == "application/pdf":
        b64 = base64.b64encode(content).decode()
        st.markdown(f'<iframe src="data:application/pdf;base64,{b64}" width="100%" height="820" '
                    'style="border:none"></iframe>', unsafe_allow_html=True)
        st.download_button("Download PDF", content, file_name=os.path.basename(md["source_file"]),
                           mime="application/pdf", key=f"dl:{doc_id}")
    else:
        st.image(content, use_container_width=True)

with right:
    st.subheader("Verify fields")
    low = set(md.get("low_confidence_fields", []))
    missing = set(md.get("missing_fields", []))
    with st.form(key=f"form:{doc_id}"):
        submitted: Dict[str, str] = {}
        for fname in tr.PARSERS:
            attrs = canonical.get(fname) or {}
            conf = attrs.get("confidence")
            tag = " 🚨 low confidence" if fname in low else (" ❗ missing" if fname in missing else "")
            conf_txt = f" · {conf:.0%}" if isinstance(conf, (int, float)) else ""
            submitted[fname] = st.text_input(
                f"{tr.FIELD_LABELS.get(fname, fname)}{tag}{conf_txt}",
                value=str(attrs.get("value", "")), key=f"{doc_id}:{fname}",
            )
        reject_reason = st.text_input("Rejection reason (required to reject)", key=f"{doc_id}:reject_reason")
        b1, b2 = st.columns(2)
        do_approve = b1.form_submit_button("✅ Approve & load to warehouse", type="primary", use_container_width=True)
        do_reject = b2.form_submit_button("⛔ Reject (not a valid W-2)", use_container_width=True)

    with st.expander("All raw extracted fields (SSNs masked)"):
        raw = payload.get("extracted_data", {})
        st.dataframe(
            [{"key": k, "value": tr.mask_pii(v.get("value")), "confidence": v.get("confidence")} for k, v in raw.items()],
            use_container_width=True, hide_index=True,
        )

if do_approve:
    clean, errors = tr.validate_record(submitted)
    if errors:
        for fname, msg in errors.items():
            st.error(f"**{tr.FIELD_LABELS.get(fname, 'Record')}**: {msg}")
    else:
        try:
            with st.spinner("Saving..."):
                approve(selected, payload, generation, clean, submitted)
            list_pending.clear()
            flash("success", f"Approved `{md.get('source_file')}` and loaded it to BigQuery.")
            st.rerun()
        except gexc.PreconditionFailed:
            list_pending.clear()
            flash("warning", "Another reviewer changed this document while you were editing. Reloaded - please re-check.")
            st.rerun()
        except Exception as exc:  # noqa: BLE001 - show, task stays in queue for retry
            st.error(f"Approval failed; the task remains in the queue and can be retried. Details: {exc}")

if do_reject:
    if not reject_reason.strip():
        st.error("Enter a rejection reason.")
    else:
        try:
            reject(selected, payload, generation, reject_reason.strip()[:500])
            list_pending.clear()
            flash("info", f"Rejected `{md.get('source_file')}`.")
            st.rerun()
        except gexc.PreconditionFailed:
            list_pending.clear()
            flash("warning", "Another reviewer changed this document. Reloaded.")
            st.rerun()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Rejection failed; the task remains in the queue. Details: {exc}")
