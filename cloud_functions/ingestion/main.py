import os
import json
from google.cloud import documentai_v1 as documentai
from google.cloud import storage
from google.cloud import pubsub_v1

# Fetch systems environment routing parameters
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
LOCATION = os.environ.get("GCP_LOCATION", "us")
PROCESSOR_ID = os.environ.get("DOCAI_PROCESSOR_ID", "your-processor-id")
HITL_BUCKET = os.environ.get("HITL_BUCKET_NAME", "your-hitl-review-queue")
VERIFIED_BUCKET = os.environ.get("VERIFIED_BUCKET_NAME", "your-verified-tax-data")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC_NAME", "hitl-alerts")
CONFIDENCE_THRESHOLD = float(os.environ.get("CONFIDENCE_THRESHOLD", "0.85"))

def process_tax_upload(event, context):
    """Background Cloud Function triggered by Google Cloud Storage file arrivals."""
    storage_client = storage.Client()
    
    # Extract structural metadata from file upload metadata envelope
    source_bucket_name = event['bucket']
    file_name = event['name']
    
    print(f"📥 Processing incoming file: {file_name} from bucket: {source_bucket_name}")
    
    # Read the raw binary stream data directly into function memory
    bucket = storage_client.bucket(source_bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_bytes()
    
    # Determine proper standard MIME classification
    mime_type = "application/pdf"
    if file_name.lower().endswith((".jpg", ".jpeg")):
        mime_type = "image/jpeg"
    elif file_name.lower().endswith(".png"):
        mime_type = "image/png"

    # Authenticate and execute Document AI Form/Tax Processor Engine 
    docai_client = documentai.DocumentProcessorServiceClient()
    resource_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    
    raw_document = documentai.RawDocument(content=file_content, mime_type=mime_type)
    request = documentai.ProcessRequest(name=resource_name, raw_document=raw_document)
    
    print(f"🤖 Dispatching {file_name} to Google Document AI Processor...")
    result = docai_client.process_document(request=request)
    document_payload = result.document

    # Parse and structure extraction outputs
    extracted_fields = {}
    low_confidence_fields = []
    global_passed_threshold = True

    # Scan extracted entity forms (Handles specialized Tax Processors or Form Parsers)
    if document_payload.entities:
        for entity in document_payload.entities:
            field_name = entity.type_
            field_value = entity.mention_text
            confidence = entity.confidence if entity.confidence else 0.0
            
            extracted_fields[field_name] = {
                "value": field_value,
                "confidence": confidence
            }
            
            # Identify individual data quality drops
            if confidence < CONFIDENCE_THRESHOLD:
                low_confidence_fields.append(field_name)
                global_passed_threshold = False
    else:
        # Fallback tracking interface loop for standard layout Form Parsers
        for page in document_payload.pages:
            for form_field in page.form_fields:
                name_text = get_text_segment(form_field.field_name, document_payload).strip().replace(":", "")
                value_text = get_text_segment(form_field.field_value, document_payload).strip()
                confidence = form_field.field_value.confidence if form_field.field_value.confidence else 0.0
                
                # Standardize tax field identifiers to fit snake_case database definitions
                clean_key = name_text.lower().replace(" ", "_").replace(",", "")
                
                extracted_fields[clean_key] = {
                    "value": value_text,
                    "confidence": confidence
                }
                
                if confidence < CONFIDENCE_THRESHOLD:
                    low_confidence_fields.append(clean_key)
                    global_passed_threshold = False

    # Structure clean payload wrapper metadata
    output_payload = {
        "metadata": {
            "source_file": file_name,
            "source_bucket": source_bucket_name,
            "needs_manual_review": not global_passed_threshold,
            "low_confidence_fields": low_confidence_fields
        },
        "extracted_data": extracted_fields
    }

    # Route document output payload based on validation compliance scoring
    output_json_name = f"{os.path.splitext(file_name)[0]}_output.json"
    
    if global_passed_threshold:
        print(f"✅ Document passed automated QA checks. Ingesting to Verified Storage...")
        destination_bucket = storage_client.bucket(VERIFIED_BUCKET)
        dest_blob = destination_bucket.blob(f"automated_clean/{output_json_name}")
        dest_blob.upload_from_string(json.dumps(output_payload, indent=4), content_type="application/json")
    else:
        print(f"⚠️ Document low-confidence detected. Routing to HITL Operations Queue...")
        # Save to manual validation staging queue area
        destination_bucket = storage_client.bucket(HITL_BUCKET)
        dest_blob = destination_bucket.blob(f"review_pending/{output_json_name}")
        dest_blob.upload_from_string(json.dumps(output_payload, indent=4), content_type="application/json")
        
        # Dispatch system warning alert broadcast via Pub/Sub Infrastructure
        dispatch_pubsub_alert(file_name, low_confidence_fields)

def get_text_segment(el, doc):
    """Helper method translating character layout coordinate frames into clean string tokens."""
    if not el.text_anchor.text_segments:
        return ""
    start_index = int(el.text_anchor.text_segments[0].start_index)
    end_index = int(el.text_anchor.text_segments[0].end_index)
    return doc.text[start_index:end_index]

def dispatch_pubsub_alert(filename, fields):
    """Publishes systematic queue warning metrics over to operational Pub/Sub alerting channels."""
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
        
        message_data = {
            "alert_type": "MANUAL_REVIEW_REQUIRED",
            "file_name": filename,
            "flagged_fields": fields
        }
        
        data_bytes = json.dumps(message_data).encode("utf-8")
        future = publisher.publish(topic_path, data_bytes)
        print(f"📢 Pub/Sub Review Alert dispatched successfully: {future.result()}")
    except Exception as error:
        print(f"❌ Failed to dispatch Pub/Sub operational alert event: {str(error)}")
