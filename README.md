# 🔍 Intelligent Document Processing (IDP) Tax Processing Architecture

An enterprise-grade, event-driven, serverless pipeline designed to process tax forms (W-2, 1099, 1040) using Google Cloud Document AI, Cloud Storage, Cloud Functions, BigQuery, and Looker Studio, featuring an isolated Human-in-the-Loop (HITL) Streamlit review console running on Google Cloud Run.

---

## 🏗️ Architecture Design Flow

[Raw Document Upload] ──> [GCS Ingestion Bucket]│▼[Cloud Run Dashboard] ◄── [Cloud Function (Ingestion)] ──► [Document AI Parser Engine]│                         ││                         ├─► [Confidence ≥ 85%] ──► [BigQuery Core Warehouse Row]│                         └─► [Confidence < 85%] ──► [GCS HITL Queue] + [Pub/Sub Alert]│                                                                             │└─► [Manual Validation & Overrides] ──────────────────────────────────────────┴─► [Slack & Email]

---

## 📂 Project Repository Tree

```text
idp-tax-processing/
├── .github/workflows/deploy.yml   # Automated GitHub Actions CI/CD pipeline
├── cloud_functions/
│   ├── ingestion/                 # Document Ingestion Engine
│   │   ├── main.py
│   │   └── requirements.txt
│   └── alerts/                    # Notification Alert Driver
│       ├── alert_function.py
│       └── requirements.txt
├── streamlit_dashboard/           # Human-in-the-Loop Web Portal
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
├── warehouse/                     # Analytics Layer
│   ├── load_to_bigquery.py
│   └── looker_schema.sql
└── local_testing/                 # Disconnected Local Mock Simulator
    └── simulate_pipeline.py
```

---

## 🚀 Local Invalidation Testing (Zero Infrastructure Cost)

Verify routing thresholds, metadata schema compilers, and mock alerting functions locally on your machine:

```bash
# 1. Clone your workspace and navigate to the project directory
cd idp-tax-processing/

# 2. Run the end-to-end processing pipeline simulation script
python local_testing/simulate_pipeline.py
```

---

## 🛠️ Step-by-Step Production Cloud Deployment

Follow these sequential terminal commands to provision your architecture infrastructure inside Google Cloud Platform (GCP).

### Step 1: Initial System Configurations & API Scoping
Set your operational context parameters and activate the necessary serverless API microservices:

```bash
export PROJECT_ID="YOUR_PROJECT_ID"
gcloud config set project \$PROJECT_ID

# Enable requisite Google Cloud APIs
gcloud services enable \
    ://googleapis.com \
    ://googleapis.com \
    ://googleapis.com \
    ://googleapis.com \
    ://googleapis.com \
    ://googleapis.com
```

### Step 2: Provision Storage Buckets & Messaging Topics
Construct isolated object-storage environments alongside your communication pub/sub topic channel layer:

```bash
# Generate the discrete intake and processing buckets
gsutil mb -l us-central1 gs://\$PROJECT_ID-raw-tax-inputs
gsutil mb -l us-central1 gs://\$PROJECT_ID-hitl-review-queue
gsutil mb -l us-central1 gs://\$PROJECT_ID-verified-tax-data

# Build the pipeline alerting messaging system topic
gcloud pubsub topics create hitl-alerts
```

### Step 3: Deploy the Serverless Cloud Functions
Deploy your main ingestion and alerting backend code modules straight into Google Cloud Functions:

```bash
# Deploy Ingestion Cloud Function linked directly to GCS Upload Actions
gcloud functions deploy process_tax_upload \
    --runtime python310 \
    --entry-point process_tax_upload \
    --source ./cloud_functions/ingestion \
    --trigger-bucket \$PROJECT_ID-raw-tax-inputs \
    --region us-central1 \
    --set-env-vars GCP_PROJECT_ID=\$PROJECT_ID,DOCAI_PROCESSOR_ID="YOUR_PROCESSOR_ID",HITL_BUCKET_NAME="\(PROJECT_ID-hitl-review-queue",VERIFIED_BUCKET_NAME="\)PROJECT_ID-verified-tax-data",PUBSUB_TOPIC_NAME="hitl-alerts",CONFIDENCE_THRESHOLD="0.85"

# Deploy Communications Cloud Function linked to Pub/Sub events
gcloud functions deploy handle_hitl_alert \
    --runtime python310 \
    --entry-point handle_hitl_alert \
    --source ./cloud_functions/alerts \
    --trigger-topic hitl-alerts \
    --region us-central1 \
    --set-env-vars SLACK_WEBHOOK_URL="https://slack.com...",SENDGRID_API_KEY="SG.xxx",FROM_EMAIL="ops@yourcompany.com",TO_EMAIL="review-team@yourcompany.com"
```

### Step 4: Provision the Relational BigQuery Analytics Engine
Initialize your analytics data warehouse and semantic formatting view structures:

```bash
# Execute the automated schema provision check script
export GCP_PROJECT_ID=\$PROJECT_ID
python warehouse/load_to_bigquery.py
```

### Step 5: Containerize & Deploy the Streamlit HITL Portal
Compile your interface into a secure Docker image, register it with Artifact Registry, and launch it serverless using Cloud Run:

```bash
# Create the secure container repository block space inside GCP
gcloud artifacts repositories create idp-tax-processing-repo \
    --repository-format=docker \
    --location=us-central1 \
    --description="Docker storage for IDP review app"

# Build and register the image directly into Artifact Registry via Cloud Builds
gcloud builds submit --tag us-central1-docker.pkg.dev/\$PROJECT_ID/idp-tax-processing-repo/hitl-dashboard:latest ./streamlit_dashboard

# Deploy the live, workforce-accessible dashboard instance to Cloud Run
gcloud run deploy tax-hitl-dashboard \
    --image us-central1-docker.pkg.dev/\$PROJECT_ID/idp-tax-processing-repo/hitl-dashboard:latest \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --port=8501 \
    --set-env-vars GCP_PROJECT_ID=\$PROJECT_ID,HITL_BUCKET_NAME="\(PROJECT_ID-hitl-review-queue",VERIFIED_BUCKET_NAME="\)PROJECT_ID-verified-tax-data"
```

---

## 🤖 Establishing CI/CD Automations

To wire up automated deployment routines using **GitHub Actions**:
1. Open your GitHub Repository and go to **Settings > Secrets and Variables > Actions**.
2. Add `GCP_PROJECT_ID` containing your unique Google Cloud project ID string.
3. Add `GCP_SA_KEY` containing the plain-text JSON credentials key string assigned to your deployment service account.

Any future push to your repository's `main` branch affecting components within the `streamlit_dashboard/` directory path automatically invokes `.github/workflows/deploy.yml` to refresh your application panel with zero operational down-time.


