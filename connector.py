"""
=============================================================================
Gemini Enterprise Custom Connector (Standard Template)
=============================================================================
Architecture : GCS-based Bulk Import (JSONL)
Sync Mode    : FULL or INCREMENTAL
Source       : Configurable via SOURCE_BASE_URL
Target       : Discovery Engine Document Store
=============================================================================
"""

import os
import sys
import json
import base64
import logging
import time
import random
import concurrent.futures
from typing import List, Optional
from dataclasses import dataclass, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import storage, secretmanager
from google.cloud import discoveryengine_v1alpha as discoveryengine
from google.api_core.exceptions import GoogleAPIError, NotFound
from dotenv import load_dotenv

# Load configuration from environment
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("ge-connector")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION MAPPING
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Config:
    # GCP
    project_id:    str = field(default_factory=lambda: os.environ["GCP_PROJECT_ID"])
    location:      str = field(default_factory=lambda: os.environ.get("GCP_LOCATION", "global"))

    # GCS staging bucket
    gcs_bucket:    str = field(default_factory=lambda: os.environ["GCS_BUCKET"])
    gcs_blob:      str = field(default_factory=lambda: os.environ.get(
                                   "GCS_BLOB", "documents/jsonplaceholder_posts.jsonl"))

    # Discovery Engine data store
    data_store_id: str = field(default_factory=lambda: os.environ["DATA_STORE_ID"])
    collection_id: str = field(default_factory=lambda: os.environ.get(
                                   "COLLECTION_ID", "default_collection"))
    branch_id:     str = field(default_factory=lambda: os.environ.get(
                                   "BRANCH_ID", "0"))

    # Source API
    source_base_url: str = field(default_factory=lambda: os.environ.get(
                                   "SOURCE_BASE_URL", "https://jsonplaceholder.typicode.com"))

    # Secret Manager configuration
    secret_api_credentials: str = field(default_factory=lambda: os.environ.get(
                                   "SECRET_API_CREDENTIALS", ""))
    secret_acl_mapping: str = field(default_factory=lambda: os.environ.get(
                                   "SECRET_ACL_MAPPING", ""))

    # Sync mode: "full" replaces everything; "incremental" adds/updates only
    sync_mode:     str = field(default_factory=lambda: os.environ.get("SYNC_MODE", "incremental"))

# ─────────────────────────────────────────────────────────────────────────────
# NETWORKING UTILS
# ─────────────────────────────────────────────────────────────────────────────

def get_session(retries=3, backoff_factor=0.3):
    """Creates a robust requests session with retries and backoff."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504)
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# ─────────────────────────────────────────────────────────────────────────────
# CORE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_secret(project_id: str, secret_name: str) -> Optional[str]:
    if not secret_name:
        return None
    log.info(f"Fetching secret: {secret_name}")
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        log.warning(f"Failed to fetch secret '{secret_name}': {e}")
        return None

def fetch_data(base_url: str, headers: dict, since: Optional[str] = None) -> List[dict]:
    """Fetches records from the source API and shuffles/limits to 25 for testing."""
    log.info(f"Connecting to source: {base_url}...")
    url = f"{base_url}/posts"
    params = {"updated_at_gt": since} if since else {}
    
    resp = get_session().get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    all_data = resp.json()
    
    # Randomization logic for incremental testing
    import random
    random.shuffle(all_data)
    subset = all_data[:25]
    
    # Log the IDs being synced for tracking
    ids = [d['id'] for d in subset]
    log.info(f"✓ Sampled 25 random records from source (Total available: {len(all_data)}).")
    log.info(f"  IDs being synced: {ids}")
    return subset

def fetch_users(base_url: str, headers: dict) -> dict:
    """Fetch all users for author enrichment."""
    url = f"{base_url}/users"
    resp = get_session().get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return {u["id"]: u for u in resp.json()}

def build_discovery_engine_doc(record: dict, users: dict, acl_mapping: dict = None) -> dict:
    """Transforms raw record into Discovery Engine Document format."""
    doc_id = f"doc-{record['id']}"
    user_id = record.get("userId", 0)
    author = users.get(user_id, {})
    
    import random
    # Adding a random salt to body to ensure 'content' change detection works during testing
    salt = f" [SyncID: {random.randint(1000, 9999)}]"
    content_text = f"Title: {record.get('title')}\n\nBody: {record.get('body')}{salt}"
    
    doc = {
        "id": doc_id,
        "structData": {
            "title":        record.get("title"),
            "source_id":    str(record.get("id")),
            "author":       author.get("username", "unknown"),
            "company":      author.get("company", {}).get("name", "MockCorp"),
            "updated_at":   record.get("updated_at", "2024-01-01T00:00:00Z"),
            "url":          f"https://jsonplaceholder.typicode.com/posts/{record['id']}"
        },
        "content": {
            "mimeType": "text/plain",
            "rawBytes": base64.b64encode(content_text.encode("utf-8")).decode("utf-8"),
        },
    }
    if acl_mapping:
        # Map dynamic ACL configuration to Discovery Engine AclInfo format
        principals = []
        for reader in acl_mapping.get("readers", []):
            if "user" in reader:
                principals.append({"userId": reader["user"]})
            elif "group" in reader:
                principals.append({"groupId": reader["group"]})
        
        if principals:
            doc["aclInfo"] = {
                "readers": [
                    {
                        "principals": principals
                    }
                ]
            }

    return doc

def ensure_infrastructure(cfg: Config, auto_create: bool = True) -> int:
    """Ensures that the target GCS bucket and Data Store exist."""
    log.info("Checking infrastructure prerequisites...")
    
    # 1. Ensure GCS Bucket
    storage_client = storage.Client(project=cfg.project_id)
    bucket = storage_client.bucket(cfg.gcs_bucket)
    if not bucket.exists():
        if auto_create:
            log.info(f"  Bucket gs://{cfg.gcs_bucket} not found. Creating...")
            location = cfg.location if cfg.location != "global" else "us-central1"
            storage_client.create_bucket(bucket, location=location)
            log.info(f"  ✓ Bucket gs://{cfg.gcs_bucket} created.")
        else:
            raise ValueError(f"Bucket gs://{cfg.gcs_bucket} does not exist. Please create it first.")
    else:
        log.info(f"  ✓ Bucket gs://{cfg.gcs_bucket} exists.")

    # 2. Ensure Data Store
    ds_client = discoveryengine.DataStoreServiceClient()
    parent = f"projects/{cfg.project_id}/locations/{cfg.location}/collections/{cfg.collection_id}"
    name = f"{parent}/dataStores/{cfg.data_store_id}"
    
    try:
        data_store = ds_client.get_data_store(name=name)
        log.info(f"  ✓ Data Store '{cfg.data_store_id}' exists.")
        return int(data_store.content_config)
    except NotFound:
        if auto_create:
            log.info(f"  Data Store '{cfg.data_store_id}' not found. Creating...")
            data_store = discoveryengine.DataStore(
                display_name=cfg.data_store_id,
                industry_vertical=discoveryengine.IndustryVertical.GENERIC,
                solution_types=[discoveryengine.SolutionType.SOLUTION_TYPE_SEARCH],
                content_config=discoveryengine.DataStore.ContentConfig.CONTENT_REQUIRED,
                acl_enabled=True,
            )
            request = discoveryengine.CreateDataStoreRequest(
                parent=parent,
                data_store=data_store,
                data_store_id=cfg.data_store_id,
            )
            operation = ds_client.create_data_store(request=request)
            operation.result()  # Wait for creation
            log.info(f"  ✓ Data Store '{cfg.data_store_id}' created successfully.")
            return int(discoveryengine.DataStore.ContentConfig.CONTENT_REQUIRED)
        else:
            raise ValueError(f"Data Store '{cfg.data_store_id}' not found. Please create it first.")

def sync_to_discovery_engine(cfg: Config, documents: List[dict]) -> None:
    """Uploads documents to GCS and triggers the Import API."""
    if not documents:
        log.info("No data to sync.")
        return

    # 1. Upload to GCS
    log.info(f"Staging {len(documents)} documents to GCS (gs://{cfg.gcs_bucket}/{cfg.gcs_blob})...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(cfg.gcs_bucket)
    blob = bucket.blob(cfg.gcs_blob)
    
    jsonl_content = "\n".join(json.dumps(doc) for doc in documents)
    blob.upload_from_string(jsonl_content, content_type="application/json")

    # 2. Trigger Import
    log.info("Triggering Discovery Engine Import...")
    client = discoveryengine.DocumentServiceClient()
    parent = f"projects/{cfg.project_id}/locations/{cfg.location}/collections/{cfg.collection_id}/dataStores/{cfg.data_store_id}/branches/{cfg.branch_id}"
    
    reconciliation_mode = (
        discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
        if cfg.sync_mode == "incremental" else discoveryengine.ImportDocumentsRequest.ReconciliationMode.FULL
    )

    request = discoveryengine.ImportDocumentsRequest(
        parent=parent,
        gcs_source=discoveryengine.GcsSource(
            input_uris=[f"gs://{cfg.gcs_bucket}/{cfg.gcs_blob}"], 
            data_schema="document"
        ),
        reconciliation_mode=reconciliation_mode,
    )

    operation = client.import_documents(request=request)
    log.info(f"Import started: {operation.operation.name}")
    try:
        operation.result(timeout=1800)
        log.info("✓ Discovery Engine Import complete.")
    except Exception as e:
        log.warning(f"Import did not complete within timeout or failed: {e}")

def run_sync(since: Optional[str] = None, preview: bool = False, auto_create: bool = True):
    """Main execution flow."""
    cfg = Config()
    start_time = time.time()
    
    log.info("="*60)
    log.info(f"GEMINI CONNECTOR RUN | MODE: {cfg.sync_mode.upper()}")
    log.info("="*60)

    # 1. Infrastructure Check
    content_config = ensure_infrastructure(cfg, auto_create=auto_create)

    # Fetch ACL mapping if configured
    acl_mapping = None
    if cfg.secret_acl_mapping:
        acl_payload = fetch_secret(cfg.project_id, cfg.secret_acl_mapping)
        if acl_payload:
            try:
                acl_mapping = json.loads(acl_payload)
            except Exception as e:
                log.warning(f"Failed to parse ACL mapping as JSON: {e}")

    # 2. Fetch API Credentials if configured
    api_headers = {}
    if cfg.secret_api_credentials:
        api_creds = fetch_secret(cfg.project_id, cfg.secret_api_credentials)
        if api_creds:
            try:
                creds_data = json.loads(api_creds)
                api_key = creds_data.get("api_key", api_creds.strip())
                api_headers = {"Authorization": f"Bearer {api_key}"}
            except json.JSONDecodeError:
                api_headers = {"Authorization": f"Bearer {api_creds.strip()}"}

    # 3. Fetch & Transform
    raw_data = fetch_data(cfg.source_base_url, headers=api_headers, since=since)
    users = fetch_users(cfg.source_base_url, headers=api_headers)
    documents = [build_discovery_engine_doc(r, users, acl_mapping=acl_mapping) for r in raw_data]
    
    # Handle NO_CONTENT stores
    if content_config == int(discoveryengine.DataStore.ContentConfig.NO_CONTENT):
        log.warning("Stripping 'content' field for NO_CONTENT compatibility...")
        for doc in documents: doc.pop("content", None)

    if preview:
        log.info("PREVIEW MODE: Printing first 2 documents and exiting.")
        for doc in documents[:2]:
            print(json.dumps(doc, indent=2))
        return

    # 3. Sync
    sync_to_discovery_engine(cfg, documents)
    log.info(f"Total Execution Time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="ISO timestamp for incremental sync")
    parser.add_argument("--preview", action="store_true", help="Preview documents without syncing to GCP")
    parser.add_argument("--no-auto-create", action="store_true", help="Disable automatic GCP infrastructure provisioning")
    args = parser.parse_args()

    try:
        run_sync(since=args.since, preview=args.preview, auto_create=not args.no_auto_create)
    except Exception as e:
        log.error(f"FATAL ERROR: {e}")
        sys.exit(1)
