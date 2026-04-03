#!/bin/bash
# =============================================================================
# JSONPlaceholder → Gemini Enterprise Connector
# GCP Setup Script (Optimized)
# =============================================================================

set -euo pipefail

# ── CONFIGURATION ────────────────────────────────────────────────────────────
# These will be detected automatically or you can set them here.
PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
REPO_NAME="connector-repo"
IMAGE_NAME="jsonplaceholder-connector"
SERVICE_ACCOUNT_NAME="ge-connector-sa"
GCS_BUCKET="${PROJECT_ID}-connector-staging"
DATA_STORE_ID="jsonplaceholder-store"

# Full path for the Artifact Registry image
IMAGE_PATH="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:latest"

echo "--------------------------------------------------------"
echo "Deploying to Project: ${PROJECT_ID}"
echo "Region:               ${REGION}"
echo "Image Path:           ${IMAGE_PATH}"
echo "--------------------------------------------------------"

# ── STEP 1: Enable APIs ──────────────────────────────────────────────────────
echo "Enabling GCP APIs..."
gcloud services enable \
  artifactregistry.googleapis.com \
  discoveryengine.googleapis.com \
  storage.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com

# ── STEP 2: Create Artifact Registry ─────────────────────────────────────────
echo "Setting up Artifact Registry..."
gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Docker repository for connectors" || true

# ── STEP 3: Setup Service Account & Permissions ──────────────────────────────
echo "Configuring Service Account..."
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

if ! gcloud iam service-accounts describe "${SERVICE_ACCOUNT_EMAIL}" >/dev/null 2>&1; then
    gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
      --display-name="Gemini Enterprise Connector Service Account"
fi

# Grant necessary roles
for ROLE in "roles/discoveryengine.editor" "roles/storage.admin" "roles/secretmanager.secretAccessor"; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
      --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
      --role="${ROLE}" --quiet > /dev/null
done

# ── STEP 4: Build and Push Image ─────────────────────────────────────────────
echo "Building and pushing container image..."
gcloud builds submit --tag "${IMAGE_PATH}" .

# ── STEP 5: Create Cloud Run Job ─────────────────────────────────────────────
echo "Creating Cloud Run Job..."
# Note: We use --set-env-vars to pass configuration to the script
gcloud run jobs deploy jsonplaceholder-sync-job \
  --image "${IMAGE_PATH}" \
  --region "${REGION}" \
  --service-account "${SERVICE_ACCOUNT_EMAIL}" \
  --max-retries 3 \
  --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID},GCS_BUCKET=${GCS_BUCKET},DATA_STORE_ID=${DATA_STORE_ID},SYNC_MODE=incremental"

# ── STEP 6: Schedule the Job (Every 60 minutes) ──────────────────────────────
echo "Scheduling job..."
# Cron "0 * * * *" means "at minute 0 of every hour"
gcloud scheduler jobs create http jsonplaceholder-hourly-sync \
  --location "${REGION}" \
  --schedule "0 * * * *" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/jsonplaceholder-sync-job:run" \
  --http-method POST \
  --oauth-service-account-email "${SERVICE_ACCOUNT_EMAIL}" \
  --description "Triggers the JSONPlaceholder sync every 60 minutes" || \
gcloud scheduler jobs update http jsonplaceholder-hourly-sync \
  --location "${REGION}" \
  --schedule "0 * * * *"

echo "--------------------------------------------------------"
echo "✅ DEPLOYMENT COMPLETE"
echo "Job:      jsonplaceholder-sync-job"
echo "Schedule: Every 60 minutes (0 * * * *)"
echo "--------------------------------------------------------"
