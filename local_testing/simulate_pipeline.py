import os
import json
import time

# Define local mock configuration parameters mimicking system environment variables
LOCAL_HITL_DIR = "hitl_queue"
LOCAL_VERIFIED_DIR = "verified_data"
CONFIDENCE_THRESHOLD = 0.85

def init_local_workspace():
    """Ensures matching local directory frameworks exist before simulation runs."""
    os.makedirs(LOCAL_HITL_DIR, exist_ok=True)
    os.makedirs(LOCAL_VERIFIED_DIR, exist_ok=True)
    print("📁 Initialized local mock storage workspace folders successfully.\n")

def simulate_document_ai_processing(test_case_id):
    """Mocks Google Document AI processing outputs based on execution targets."""
    if test_case_id == "CLEAN_W2":
        return {
            "metadata": {
                "source_file": "clean_tax_form_2026.pdf",
                "source_bucket": "mock-raw-tax-inputs"
            },
            "extracted_data": {
                "w2_employer_name": {"value": "Innovate Tech Corp", "confidence": 0.98},
                "w2_employer_ein": {"value": "12-3456789", "confidence": 0.95},
                "w2_wages": {"value": "98500.00", "confidence": 0.97},
                "w2_fed_income_tax_withheld": {"value": "14200.00", "confidence": 0.94},
                "tax_year": {"value": "2026", "confidence": 0.99}
            }
        }
    elif test_case_id == "SMUDGED_W2":
        return {
            "metadata": {
                "source_file": "smudged_tax_form_2026.pdf",
                "source_bucket": "mock-raw-tax-inputs"
            },
            "extracted_data": {
                "w2_employer_name": {"value": "Legacy Manufacturing LLC", "confidence": 0.91},
                "w2_employer_ein": {"value": "98-7654321", "confidence": 0.89},
                "w2_wages": {"value": "125OO.00", "confidence": 0.64},  # Contains scanning typos (O instead of 0)
                "w2_fed_income_tax_withheld": {"value": "1100.00", "confidence": 0.87},
                "tax_year": {"value": "2026", "confidence": 0.95}
            }
        }
    return {}

def run_local_pipeline_processor(test_case_name):
    """Executes the core threshold inspection and file-routing workflow rules."""
    print(f"🎬 [RUNNING TEST CASE]: Processing {test_case_name} payload...")
    time.sleep(0.5) # Add small realistic execution footprint delay
    
    # 1. Fetch the simulated parsing response
    raw_payload = simulate_document_ai_processing(test_case_name)
    source_file = raw_payload["metadata"]["source_file"]
    extracted_fields = raw_payload["extracted_data"]
    
    # 2. Inspect data elements against confidence threshold benchmarks
    low_confidence_fields = []
    global_passed_threshold = True
    
    for field_name, attributes in extracted_fields.items():
        confidence = attributes["confidence"]
        if confidence < CONFIDENCE_THRESHOLD:
            low_confidence_fields.append(field_name)
            global_passed_threshold = False
            
    # 3. Compile output dictionary with routing metadata tracking tags
    output_data = {
        "metadata": {
            "source_file": source_file,
            "source_bucket": raw_payload["metadata"]["source_bucket"],
            "needs_manual_review": not global_passed_threshold,
            "low_confidence_fields": low_confidence_fields
        },
        "extracted_data": extracted_fields
    }
    
    output_filename = f"{os.path.splitext(source_file)[0]}_output.json"
    
    # 4. Route document payload along the appropriate infrastructure channel path
    if global_passed_threshold:
        dest_path = os.path.join(LOCAL_VERIFIED_DIR, output_filename)
        with open(dest_path, "w") as verified_file:
            json.dump(output_data, verified_file, indent=4)
        print(f"✅ SUCCESS: All fields passed threshold logic. File saved to: '{dest_path}'\n")
    else:
        dest_path = os.path.join(LOCAL_HITL_DIR, output_filename)
        with open(dest_path, "w") as hitl_file:
            json.dump(output_data, hitl_file, indent=4)
        print(f"⚠️ WARNING: Low confidence detected in fields: {low_confidence_fields}")
        print(f"📥 Document routed to Human-in-the-Loop review. File saved to: '{dest_path}'")
        
        # Trigger mock alert dispatch
        mock_pubsub_broadcast(source_file, low_confidence_fields)

def mock_pubsub_broadcast(filename, fields):
    """Simulates sending a Pub/Sub message alert packet payload."""
    alert_payload = {
        "alert_type": "MANUAL_REVIEW_REQUIRED",
        "file_name": filename,
        "flagged_fields": fields,
        "timestamp": "2026-08-17T18:26:00Z"
    }
    print("📢 [MOCK PUB/SUB ALERT DISPATCHED TO SLACK/SENDGRID]:")
    print(json.dumps(alert_payload, indent=2))
    print("-" * 60 + "\n")

if __name__ == "__main__":
    print("🚀 Starting Intelligent Document Processing Local Simulation Suite...")
    print("=" * 60)
    
    # Instantiate the system directories
    init_local_workspace()
    
    # Run Pass 1: Crisp Clean Record Document Ingestion Flow
    run_local_pipeline_processor("CLEAN_W2")
    
    # Run Pass 2: Blurry Smudged Exception Record Document Flow
    run_local_pipeline_processor("SMUDGED_W2")
    
    print("🎉 All localized test pipeline simulation passes completed successfully!")
