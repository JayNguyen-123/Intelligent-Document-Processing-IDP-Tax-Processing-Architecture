import os
import json
import streamlit as st
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

# Fetch system storage workspace environment routing variables
HITL_BUCKET_NAME = os.environ.get("HITL_BUCKET_NAME", "your-hitl-review-queue")
VERIFIED_BUCKET_NAME = os.environ.get("VERIFIED_BUCKET_NAME", "your-verified-tax-data")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = "tax_processing_ds"
TABLE_ID = "verified_w2_records"

# Set up page configurations
st.set_page_config(page_title="IDP Tax Forms HITL Console", layout="wide", page_icon="🔍")

# Initialize infrastructure clients
@st.cache_resource
def get_gcp_clients():
    """Caches cloud service initializations to optimize dashboard load performance."""
    return storage.Client(), bigquery.Client(project=PROJECT_ID)

storage_client, bq_client = get_gcp_clients()

st.title("🔍 Intelligent Document Processing (IDP) Operational Audit Workspace")
st.markdown("---")

# 1. Fetch pending reviews list from the GCS exceptions bucket queue
try:
    hitl_bucket = storage_client.bucket(HITL_BUCKET_NAME)
    # List files residing strictly within the review_pending/ subfolder directory
    blobs = storage_client.list_blobs(HITL_BUCKET_NAME, prefix="review_pending/")
    pending_files = [blob.name for blob in blobs if blob.name.endswith(".json")]
except Exception as bucket_err:
    st.error(f"❌ Failed to reach GCS Queue Bucket: {str(bucket_err)}")
    pending_files = []

if not pending_files:
    st.success("🎉 Outstanding queues cleared! There are no low-confidence tax forms waiting for review.")
    st.stop()

# 2. Build Sidebar Navigation Panel for outstanding tasks
st.sidebar.header(f"📥 Pending Tasks Queue ({len(pending_files)})")
selected_blob_name = st.sidebar.selectbox("Select document to audit:", pending_files)

# Fetch target payload data string
target_blob = hitl_bucket.blob(selected_blob_name)
payload_raw = target_blob.download_as_string()
payload = json.loads(payload_raw)

metadata = payload.get("metadata", {})
extracted_data = payload.get("extracted_data", {})
low_confidence_fields = metadata.get("low_confidence_fields", [])

# Display Context Metadata Status Header
col_a, col_b = st.columns(2)
with col_a:
    st.info(f"📄 **Source Image File Target:** `{metadata.get('source_file')}`")
with col_b:
    st.warning(f"⚠️ **Total Flagged Fields Below Threshold:** {len(low_confidence_fields)}")

st.markdown("### 📋 Verification Matrix Form")
st.markdown("Review data extractions below. Modify fields containing low-confidence indicator labels.")

# 3. Dynamic Generation of the Forms Engine View Interface
corrected_submission = {}

with st.form(key="hitl_validation_form"):
    col1, col2 = st.columns(2)
    
    # Divide the extracted document fields array structure evenly across twin layout grids
    fields_list = list(extracted_data.keys())
    split_index = (len(fields_list) + 1) // 2
    left_fields = fields_list[:split_index]
    right_fields = fields_list[split_index:]
    
    # Render Left Grid Block Column
    with col1:
        for field in left_fields:
            field_attr = extracted_data[field]
            val = str(field_attr.get("value", ""))
            conf = field_attr.get("confidence", 1.0)
            
            # Format UI visual flag context alerts for low confidence
            label_suffix = " 🚨 [LOW CONFIDENCE]" if field in low_confidence_fields else ""
            field_label = f"**{field.replace('_', ' ').upper()}**{label_suffix} (Confidence: {conf:.2%})"
            
            corrected_submission[field] = st.text_input(label=field_label, value=val)
            
    # Render Right Grid Block Column
    with col2:
        for field in right_fields:
            field_attr = extracted_data[field]
            val = str(field_attr.get("value", ""))
            conf = field_attr.get("confidence", 1.0)
            
            label_suffix = " 🚨 [LOW CONFIDENCE]" if field in low_confidence_fields else ""
            field_label = f"**{field.replace('_', ' ').upper()}**{label_suffix} (Confidence: {conf:.2%})"
            
            corrected_submission[field] = st.text_input(label=field_label, value=val)

    st.markdown("---")
    submit_button = st.form_submit_button(label="🚀 Validate, Promote & Stream to Warehouse", use_container_width=True)

# 4. Processing Validated Actions Form Logic On-Click Events
if submit_button:
    with st.spinner("Executing secure audit promotion workflow..."):
        try:
            # Recompile payload envelope setting verification markers to true
            verified_payload = {
                "metadata": {
                    "source_file": metadata.get("source_file"),
                    "source_bucket": metadata.get("source_bucket"),
                    "needs_manual_review": False,
                    "audited_by_human": True,
                    "original_low_confidence_fields": low_confidence_fields
                },
                "extracted_data": corrected_submission
            }
            
            # Save verified production file into destination clean master storage bucket location
            verified_bucket = storage_client.bucket(VERIFIED_BUCKET_NAME)
            clean_filename = selected_blob_name.replace("review_pending/", "human_verified/")
            clean_blob = verified_bucket.blob(clean_filename)
            
            clean_blob.upload_from_string(
                json.dumps(verified_payload, indent=4),
                content_type="application/json"
            )
            st.toast("💾 Clean payload backed up to Verified Storage Bucket successfully.", icon="📁")

            # Format record dataset layout payload block array for direct streaming to BigQuery database
            bq_row_record = {
                "employer_name": corrected_submission.get("w2_employer_name", ""),
                "employer_ein": corrected_submission.get("w2_employer_ein", ""),
                "wages": float(corrected_submission.get("w2_wages", 0.0)) if corrected_submission.get("w2_wages") else None,
                "fed_income_tax_withheld": float(corrected_submission.get("w2_fed_income_tax_withheld", 0.0)) if corrected_submission.get("w2_fed_income_tax_withheld") else None,
                "tax_year": int(corrected_submission.get("tax_year", 0)) if corrected_submission.get("tax_year") else None,
                "processed_at": pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            }
            
            table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
            errors = bq_client.insert_rows_json(table_ref, [bq_row_record])
            
            if not errors:
                st.toast("📈 Structured record row streamed successfully to BigQuery!", icon="📊")
                
                # Delete target item from review exception folder queue block
                target_blob.delete()
                st.toast("🗑️ File purged from exception queue.", icon="✨")
                
                st.success(f"🎉 Validation success! File `{metadata.get('source_file')}` processed and archived.")
                st.button("Load Next Task Queue Entry")
            else:
                st.error(f"❌ BigQuery Streaming Insertion Errors Detected: {errors}")
                
        except Exception as streaming_error:
            st.error(f"❌ Execution failure during workspace pipeline resolution steps: {str(streaming_error)}")
