#!/bin/bash
# =============================================================================
# JSONPlaceholder → Gemini Enterprise Connector
# GCP Setup Script
# =============================================================================
# Run each section manually — do NOT run this entire script at once.
# Each section corresponds to a numbered step in the README.
# =============================================================================

set -euo pipefail

# ── FILL THESE IN BEFORE RUNNING ─────────────────────────────────────────────
export PROJECT_ID="search-ahmed"
export REGION="us-central1"
export GCS_BUCKET="jsonplaceholder-ge-connector-staging"
export DATA_STORE_ID="jsonplaceholder-test-store"
export IMAGE_NAME="gcr.io/${PROJECT_ID}/jsonplaceholder-ge-connector"

# ── STEP 1: Authenticate and set project ─────────────────────────────────────
gcloud auth login
gcloud config set project "${PROJECT_ID}"
gcloud auth application-default login

# ── STEP 2: Enable required APIs ─────────────────────────────────────────────
gcloud services enable \
  discoveryengine.googleapis.com \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com

echo "✓ APIs enabled"

# ── STEP 3: Grant IAM roles ───────────────────────────────────────────────────
# Replace USER_EMAIL with your Google account email
USER_EMAIL="your-email@example.com"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="user:${USER_EMAIL}" \
  --role="roles/discoveryengine.editor"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="user:${USER_EMAIL}" \
  --role="roles/storage.admin"

echo "✓ IAM roles granted"

# ── STEP 4: Create GCS bucket ────────────────────────────────────────────────
gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "gs://${GCS_BUCKET}"
echo "✓ GCS bucket created: gs://${GCS_BUCKET}"

# ── STEP 5: Run connector locally to provision and sync ────────────────────
echo "Running connector locally..."
export GCP_PROJECT_ID="${PROJECT_ID}"
export GCS_BUCKET="${GCS_BUCKET}"
export DATA_STORE_ID="${DATA_STORE_ID}"
export SYNC_MODE="full"

python connector.py

echo "✓ Connector run complete — bucket and data store should now be provisioned if they did not already exist."

echo "✓ Local run complete — check GCP Console for import status"

# ── STEP 7: (MANUAL) Connect data store to GE App ────────────────────────────
echo ""
echo "⚠ MANUAL STEP REQUIRED:"
echo "  1. Go to GCP Console → Gemini Enterprise → Apps"
echo "  2. Click your app (or Create app → Search type)"
echo "  3. Go to 'Connected data stores'"
echo "  4. Click '+ New data store'"
echo "  5. Select: ${DATA_STORE_ID}"
echo "  6. Click Save"
echo "  7. Wait for status to show 'Active'"
echo ""
read -p "Press Enter after completing the manual step above..."

# ── STEP 8: Deploy as Cloud Run Job (for production automation) ───────────────
gcloud builds submit \
  --tag "${IMAGE_NAME}" \
  --project "${PROJECT_ID}"

gcloud run jobs create jsonplaceholder-sync-job \
  --image "${IMAGE_NAME}" \
  --region "${REGION}" \
  --max-retries 2 \
  --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID},GCS_BUCKET=${GCS_BUCKET},DATA_STORE_ID=${DATA_STORE_ID},SYNC_MODE=full"

echo "✓ Cloud Run Job created"

# ── STEP 9: Test the Cloud Run Job ───────────────────────────────────────────
gcloud run jobs execute jsonplaceholder-sync-job \
  --region "${REGION}" \
  --wait

echo "✓ Test execution complete"

# ── STEP 10: Create Cloud Scheduler for ongoing sync ─────────────────────────
JOB_URI=$(gcloud run jobs describe jsonplaceholder-sync-job \
  --region "${REGION}" \
  --format "value(metadata.selfLink)")

gcloud scheduler jobs create http jsonplaceholder-sync-schedule \
  --schedule "0 */4 * * *" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/jsonplaceholder-sync-job:run" \
  --message-body "{}" \
  --oauth-service-account-email "${USER_EMAIL}" \
  --location "${REGION}"

echo "✓ Cloud Scheduler created — syncs every 4 hours"
echo ""
echo "✅ Full setup complete!"
