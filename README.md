# IDP Tax Processing: Document AI + Human-in-the-Loop on GCP

An event-driven, serverless pipeline that extracts W-2 data with Google Document AI, auto-verifies high-quality extractions straight into BigQuery, and routes everything else to a private Streamlit review console on Cloud Run.

## Architecture

```text
 upload ─► gs://<proj>-raw-tax-inputs
                 │  (Eventarc: object finalized, at-least-once, --retry)
                 ▼
     Cloud Function: process-tax-upload ──► Document AI (regional endpoint)
                 │
                 ├─ all required fields present, ≥ threshold, valid ──► BigQuery row (AUTOMATED)
                 │                                                     + gs://…-verified-tax-data/automated/
                 │
                 └─ otherwise / extraction error ──► gs://…-hitl-review-queue/review_pending/
                                                     + Pub/Sub "hitl-alerts" ──► Cloud Function: handle-hitl-alert
                                                                                  ├─► Slack
                                                                                  └─► SendGrid email
 Reviewer (IAP-authenticated) ─► Cloud Run: tax-hitl-dashboard
                 ├─ Approve ─► BigQuery row (HUMAN, reviewed_by) + verified-tax-data/human_verified/
                 └─ Reject  ─► hitl-review-queue/review_rejected/
 Looker Studio ─► vw_looker_tax_analytics (de-duplicated by document_id)
```

### Routing rules (`shared/tax_record.py`)

A document is auto-verified only when **all** of these hold. Otherwise it goes to review, and the reasons are recorded.

| Check | Review reason |
|---|---|
| At least one field extracted | `no_fields_extracted` |
| Every required canonical field present (`employer_name`, `employer_ein`, `wages`, `fed_income_tax_withheld`, `tax_year`) | `missing_required_fields` |
| Every canonical field's confidence ≥ `CONFIDENCE_THRESHOLD` | `low_confidence` |
| Valid formats: dollar amounts, 9-digit EIN, year 2000–current, Box 2 ≤ Box 1 | `validation_failed` |
| No two different values for one field | `conflicting_values` |
| Document AI accepted the file | `processing_error` |

Raw keys are mapped to canonical fields through an alias table. **Check it against your processor's entity names** and extend it without code changes:

```bash
FIELD_ALIASES_JSON='{"wages": ["MyCustomWagesEntity"]}'
```

### Delivery guarantees

* `document_id = sha256(bucket/object#generation)`. A duplicate event for the same upload is a no-op. A re-upload is a new document.
* GCS outputs are create-only (`if_generation_match=0`). BigQuery inserts use `insertId=document_id`, and the Looker view keeps the latest row per `document_id`.
* Reviewers claim a task with a compare-and-swap write, so two people cannot approve the same document.
* Transient failures raise, and Eventarc or Pub/Sub retries them. Permanent failures (unsupported or corrupt file, page limits) land in the HITL queue so a person can key in the values.

## Repository layout

```text
idp-tax-processing/
├── .github/workflows/deploy.yml     # test → deploy (functions + dashboard), keyless WIF auth
├── shared/tax_record.py             # canonical model, validation, routing, payload + BQ contracts
├── cloud_functions/
│   ├── ingestion/main.py            # GCS-triggered extraction + routing (gen2)
│   ├── ingestion/requirements.txt
│   ├── alerts/main.py               # Pub/Sub-triggered Slack/SendGrid fan-out (gen2)
│   └── alerts/requirements.txt
├── streamlit_dashboard/
│   ├── app.py                       # IAP-protected HITL console
│   ├── requirements.txt
│   └── Dockerfile                   # build from repo root
├── warehouse/
│   ├── load_to_bigquery.py          # provision + additive schema migration + view
│   └── looker_schema.sql            # DDL + de-duplicated semantic view
├── local_testing/simulate_pipeline.py
├── tests/                           # unit tests with in-memory SDK fakes
├── scripts/sync_shared.sh           # copies shared module into function sources
├── requirements-dev.txt
└── REVIEW.md                        # production review findings
```

## Local verification (no GCP needed)

```bash
pip install -r requirements-dev.txt
python -m pytest -q tests                    # 30 unit tests
python local_testing/simulate_pipeline.py    # 6 routing scenarios, exits non-zero on regression
```

Run the dashboard locally against a dev project (never set `DEV_MODE` in Cloud Run):

```bash
gcloud auth application-default login
cp shared/tax_record.py streamlit_dashboard/
DEV_MODE=true GCP_PROJECT_ID=my-dev HITL_BUCKET_NAME=… VERIFIED_BUCKET_NAME=… \
  streamlit run streamlit_dashboard/app.py
```

## Production deployment

### 1. Project and APIs

```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
gcloud config set project "$PROJECT_ID"
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')

gcloud services enable \
  documentai.googleapis.com cloudfunctions.googleapis.com run.googleapis.com \
  eventarc.googleapis.com pubsub.googleapis.com storage.googleapis.com \
  bigquery.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com \
  secretmanager.googleapis.com iap.googleapis.com logging.googleapis.com
```

Create a W-2 Parser processor in Document AI (console). Note its **processor ID**, its location (`us` or `eu`) and, ideally, a pinned **processor version**.

### 2. Buckets and topic (private by default)

```bash
for b in raw-tax-inputs hitl-review-queue verified-tax-data; do
  gcloud storage buckets create "gs://${PROJECT_ID}-${b}" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention
done
gcloud pubsub topics create hitl-alerts
gcloud pubsub topics create hitl-alerts-dlq   # dead-letter for undeliverable alerts
```

Add lifecycle and retention rules that match your record-retention policy. Add CMEK if your policy requires it.

### 3. Least-privilege service accounts

```bash
for sa in idp-ingestion idp-alerts idp-dashboard idp-deployer; do
  gcloud iam service-accounts create "$sa"
done
SA() { echo "$1@${PROJECT_ID}.iam.gserviceaccount.com"; }
bind_bucket() { gcloud storage buckets add-iam-policy-binding "gs://${PROJECT_ID}-$1" --member="serviceAccount:$(SA $2)" --role="$3"; }

# Ingestion
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$(SA idp-ingestion)" --role=roles/documentai.apiUser
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$(SA idp-ingestion)" --role=roles/eventarc.eventReceiver
bind_bucket raw-tax-inputs     idp-ingestion roles/storage.objectViewer
bind_bucket hitl-review-queue  idp-ingestion roles/storage.objectUser
bind_bucket verified-tax-data  idp-ingestion roles/storage.objectUser
gcloud pubsub topics add-iam-policy-binding hitl-alerts --member="serviceAccount:$(SA idp-ingestion)" --role=roles/pubsub.publisher

# Dashboard
bind_bucket raw-tax-inputs     idp-dashboard roles/storage.objectViewer
bind_bucket hitl-review-queue  idp-dashboard roles/storage.objectUser
bind_bucket verified-tax-data  idp-dashboard roles/storage.objectUser

# Alerts
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$(SA idp-alerts)" --role=roles/eventarc.eventReceiver

# GCS service agent must publish events for gen2 storage triggers
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com" --role=roles/pubsub.publisher
```

After step 4 creates the dataset, grant BigQuery access at dataset level: `roles/bigquery.dataEditor` on `tax_processing_ds` for `idp-ingestion` and `idp-dashboard`.

The deployer SA needs `roles/run.admin`, `roles/cloudfunctions.developer`, `roles/artifactregistry.writer`, `roles/cloudbuild.builds.editor`, and `roles/iam.serviceAccountUser` on the three runtime SAs.

If a gen2 deploy reports that the trigger cannot invoke the function, grant `roles/run.invoker` to the trigger's service account.

### 4. BigQuery

```bash
GCP_PROJECT_ID=$PROJECT_ID BQ_LOCATION=US python warehouse/load_to_bigquery.py
```

This is idempotent. It creates the dataset, the partitioned and clustered table and the view, and adds any new columns to an existing table. Point Looker Studio at `tax_processing_ds.vw_looker_tax_analytics`.

### 5. Secrets

```bash
printf '%s' 'https://hooks.slack.com/services/…' | gcloud secrets create slack-webhook-url --data-file=-
printf '%s' 'SG.…'                                | gcloud secrets create sendgrid-api-key  --data-file=-
for s in slack-webhook-url sendgrid-api-key; do
  gcloud secrets add-iam-policy-binding "$s" --member="serviceAccount:$(SA idp-alerts)" --role=roles/secretmanager.secretAccessor
done
```

### 6. Artifact Registry

```bash
gcloud artifacts repositories create idp-tax-processing-repo --repository-format=docker --location="$REGION"
```

### 7. CI/CD with GitHub Actions (keyless)

1. Create a Workload Identity Pool and Provider for GitHub, restricted to your repository (`attribute.repository == "org/repo"`). Allow it to impersonate `idp-deployer` (`roles/iam.workloadIdentityUser`).
2. In GitHub → Settings → Secrets and variables → Actions → **Variables**, add:
   * `GCP_PROJECT_ID`, `GCP_REGION`
   * `GCP_WIF_PROVIDER`, `GCP_DEPLOY_SA`
   * `INGESTION_SA`, `ALERTS_SA`, `DASHBOARD_SA`
   * `DOCAI_PROCESSOR_ID`, `DOCAI_LOCATION`, `DOCAI_PROCESSOR_VERSION`, `CONFIDENCE_THRESHOLD`
   * `ALERT_FROM_EMAIL`, `ALERT_TO_EMAIL` (comma-separated), `DASHBOARD_URL`, `IAP_AUDIENCE`
3. Create a `production` environment with required reviewers.

Every PR runs lint, the unit tests and the simulator. A push to `main` runs the tests and then deploys both functions and the dashboard.

After the first deploy, attach the dead-letter topic to the alert function's Eventarc subscription:

```bash
SUB=$(gcloud pubsub topics list-subscriptions hitl-alerts --format='value(.)' | head -1)
gcloud pubsub subscriptions update "$SUB" --dead-letter-topic=hitl-alerts-dlq --max-delivery-attempts=10
```

### 8. Protect the dashboard with IAP

The service is deployed with `--no-allow-unauthenticated`. Enable Identity-Aware Proxy for it in one of two ways:

* Cloud Run → service → **Security** → enable IAP. With a recent gcloud you can also run `gcloud beta run services update tax-hitl-dashboard --region=$REGION --iap`.
* Use an external HTTPS load balancer with a serverless NEG and IAP on the backend service.

Grant reviewers `roles/iap.httpsResourceAccessor`. Set the `IAP_AUDIENCE` variable to the signed-header audience for your setup. See Google's "Securing your app with signed headers" for the exact format. The app verifies `X-Goog-IAP-JWT-Assertion` against that audience and refuses every other request.

## Operations

* **Logs:** JSON-structured, keyed by `document_id`. Field values are never logged. Example filter: `jsonPayload.component="ingestion" severity>=WARNING`.
* **Alert on:** function error rate, messages in `hitl-alerts-dlq`, the oldest object age in `review_pending/` (an SLA), and BigQuery insert errors.
* **Threshold tuning:** compare `review_source` and `low_confidence_fields` in BigQuery with the correction diffs stored in `human_verified/*.json` (`review.corrections`).
* **Limits:** online processing caps file size (default `MAX_FILE_BYTES` = 20 MB) and page count. Oversized files go to HITL. For large multi-page batches, switch to `batch_process_documents`.
