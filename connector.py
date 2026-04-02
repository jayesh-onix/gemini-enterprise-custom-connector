"""
=============================================================================
Gemini Enterprise Custom Connector (jsonplaceholder)
=============================================================================
Source  : https://jsonplaceholder.typicode.com  (test/prototype API)
Target  : Google Discovery Engine Data Store → Gemini Enterprise App
Pattern : GCS-based comprehensive sync (Google's recommended architecture)
Docs    : https://docs.cloud.google.com/gemini/enterprise/docs/connectors/create-custom-connector

This connector serves as a working template. To adapt it for Highspot:
  1. Replace fetch_*() functions with Highspot API calls
  2. Update build_document() field mapping for Highspot schema
  3. Keep ALL GCS + Discovery Engine sync logic unchanged
=============================================================================
"""

import concurrent.futures
import os
import sys
import json
import base64
import logging
import time
from typing import List, Optional
from dataclasses import dataclass, field

import requests
from google.cloud import storage, secretmanager
from google.cloud import discoveryengine_v1 as discoveryengine
from google.api_core.exceptions import GoogleAPIError, NotFound
from dotenv import load_dotenv

# Load .env file automatically
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("ge-connector")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG  —  all values read from environment variables for security
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

    # Source API (JSONPlaceholder — no auth needed)
    source_base_url: str = field(default_factory=lambda: os.environ.get(
                                   "SOURCE_BASE_URL", "https://jsonplaceholder.typicode.com"))

    # Secret Manager configuration
    secret_api_credentials: str = field(default_factory=lambda: os.environ.get(
                                   "SECRET_API_CREDENTIALS", ""))
    secret_acl_mapping: str = field(default_factory=lambda: os.environ.get(
                                   "SECRET_ACL_MAPPING", ""))

    # Sync mode: "full" replaces everything; "incremental" adds/updates only
    sync_mode:     str = field(default_factory=lambda: os.environ.get("SYNC_MODE", "full"))


# ─────────────────────────────────────────────────────────────────────────────
# STEP 0: SECRETS  —  Fetch dynamic configuration from Secret Manager
# ─────────────────────────────────────────────────────────────────────────────

def fetch_secret(project_id: str, secret_name: str) -> Optional[str]:
    """
    Fetch a payload from Google Cloud Secret Manager.
    Requires roles/secretmanager.secretAccessor
    """
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


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: FETCH  —  Pull data from source API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_posts(base_url: str) -> List[dict]:
    """
    Fetch all posts from JSONPlaceholder.
    Pattern mirrors what the official GE doc shows for paginated fetch.
    Replace this function body with Highspot API calls when adapting.
    """
    log.info("Fetching posts from JSONPlaceholder...")
    url = f"{base_url}/posts"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    posts = resp.json()
    log.info(f"  ✓ Fetched {len(posts)} posts")
    return posts


def fetch_comments_for_post(base_url: str, post_id: int) -> List[dict]:
    """
    Fetch comments belonging to a specific post.
    Demonstrates nested resource fetch — mirrors Highspot Spot→Items pattern.
    """
    url = f"{base_url}/posts/{post_id}/comments"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_users(base_url: str) -> dict:
    """
    Fetch all users and return as a lookup dict {user_id: user_object}.
    Used to enrich documents with author metadata.
    """
    log.info("Fetching users (for author enrichment)...")
    url = f"{base_url}/users"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    users = {u["id"]: u for u in resp.json()}
    log.info(f"  ✓ Fetched {len(users)} users")
    return users


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: TRANSFORM  —  Convert to Discovery Engine Document format
# ─────────────────────────────────────────────────────────────────────────────

def encode_text(text: str) -> str:
    """
    Base64-encode plain text for the Discovery Engine 'rawBytes' field.
    Required by the API — content must be base64 encoded.
    """
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def build_document(post: dict, users: dict, comments: List[dict], acl_mapping: dict = None) -> dict:
    """
    Convert a JSONPlaceholder post into a Discovery Engine Document.

    Official document format reference:
    https://docs.cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1/projects.locations.collections.dataStores.branches.documents

    Document has three top-level sections:
      - id          : globally unique stable ID (never change this)
      - structData  : structured metadata fields (filterable, facetable)
      - content     : the actual text content for semantic search

    When adapting for Highspot:
      - id         → f"highspot-item-{item['id']}"
      - structData → map Highspot item fields
      - content    → item title + description + extracted text
    """
    post_id   = post["id"]
    user_id   = post.get("userId", 0)
    author    = users.get(user_id, {})
    title     = post.get("title", "")
    body      = post.get("body", "")
    username  = author.get("username", "unknown")
    email     = author.get("email", "")
    company   = author.get("company", {}).get("name", "")

    # Build rich searchable content from all available text
    comment_text = "\n".join(
        f"Comment by {c.get('name','')}: {c.get('body','')}"
        for c in comments
    )
    full_content = f"Title: {title}\n\nBody: {body}"
    if comment_text:
        full_content += f"\n\nComments:\n{comment_text}"

    doc = {
        # Stable unique ID — format must never change after first sync
        "id": f"jsonplaceholder-post-{post_id}",

        # Structured metadata — used for filtering, facets, display
        "structData": {
            "title":        title,
            "post_id":      str(post_id),
            "user_id":      str(user_id),
            "author":       username,
            "author_email": email,
            "company":      company,
            "comment_count": str(len(comments)),
            "source":       "JSONPlaceholder",
            "url":          f"https://jsonplaceholder.typicode.com/posts/{post_id}",
        },

        # Unstructured content — indexed for semantic search by GE
        "content": {
            "mimeType": "text/plain",
            "rawBytes": encode_text(full_content),
        },
    }

    if acl_mapping:
        doc["aclInfo"] = acl_mapping

    return doc


def build_all_documents(
    posts: List[dict],
    users: dict,
    base_url: str,
    include_comments: bool = True,
    acl_mapping: dict = None,
) -> List[dict]:
    """
    Build Discovery Engine documents for all posts.
    Optionally enriches each post with its comments.
    """
    documents = []
    total = len(posts)
    for i, post in enumerate(posts, 1):
        post_id  = post["id"]
        comments = []
        if include_comments:
            try:
                comments = fetch_comments_for_post(base_url, post_id)
            except requests.HTTPError as e:
                log.warning(f"  Could not fetch comments for post {post_id}: {e}")

        doc = build_document(post, users, comments, acl_mapping)
        documents.append(doc)

        if i % 10 == 0 or i == total:
            log.info(f"  Transformed {i}/{total} documents")

    return documents


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3a: SYNC TO GCS  —  Stage JSONL file in Cloud Storage
# ─────────────────────────────────────────────────────────────────────────────

def upload_to_gcs(documents: List[dict], bucket_name: str, blob_path: str) -> str:
    """
    Write all documents as JSONL to GCS.
    Returns the GCS URI for use in Discovery Engine import.

    JSONL format: one complete JSON document per line.
    This is what the Discovery Engine import API expects.
    """
    if not documents:
        raise ValueError("No documents to upload — aborting GCS write")

    log.info(f"Uploading {len(documents)} documents to GCS...")

    client  = storage.Client()
    bucket  = client.bucket(bucket_name)
    blob    = bucket.blob(blob_path)

    jsonl_content = "\n".join(json.dumps(doc, ensure_ascii=False) for doc in documents)
    size_kb       = len(jsonl_content.encode("utf-8")) / 1024

    blob.upload_from_string(jsonl_content, content_type="application/json")

    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    log.info(f"  ✓ Uploaded {size_kb:.1f} KB → {gcs_uri}")
    return gcs_uri


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3b: SYNC TO DISCOVERY ENGINE  —  Trigger import from GCS
# ─────────────────────────────────────────────────────────────────────────────

def import_from_gcs(cfg: Config, gcs_uri: str) -> None:
    """
    Call the Discovery Engine import API to pull documents from GCS
    into the data store.

    This uses the official Python client library for Discovery Engine:
    https://cloud.google.com/python/docs/reference/discoveryengine/latest

    reconciliation_mode:
      - INCREMENTAL : adds/updates, does NOT delete removed docs
      - FULL        : replaces entire dataset, handles deletions
    """
    log.info("Triggering Discovery Engine import from GCS...")

    client = discoveryengine.DocumentServiceClient()

    parent = (
        f"projects/{cfg.project_id}"
        f"/locations/{cfg.location}"
        f"/collections/{cfg.collection_id}"
        f"/dataStores/{cfg.data_store_id}"
        f"/branches/{cfg.branch_id}"
    )

    reconciliation_mode = (
        discoveryengine.ImportDocumentsRequest.ReconciliationMode.FULL
        if cfg.sync_mode == "full"
        else discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
    )

    gcs_source = discoveryengine.GcsSource(
        input_uris=[gcs_uri],
        data_schema="document",   # tells DE the file contains Document-format JSONL
    )

    request = discoveryengine.ImportDocumentsRequest(
        parent=parent,
        gcs_source=gcs_source,
        reconciliation_mode=reconciliation_mode,
    )

    try:
        # This is a Long Running Operation (LRO) — we poll until complete
        operation = client.import_documents(request=request)
        log.info(f"  Import operation started: {operation.operation.name}")
        log.info("  Waiting for import to complete (this may take a few minutes; large imports can take longer than 10 minutes)")

        # Poll the LRO
        try:
            result = operation.result(timeout=1800)   # 30-minute timeout
        except concurrent.futures.TimeoutError:
            log.warning("Import did not complete within 30 minutes. The operation is still running in Google Cloud.")
            log.warning(f"Operation name: {operation.operation.name}")
            log.warning("Check the GCP Console or use the operation name to inspect progress.")
            return

        # Log any partial errors
        if result.error_samples:
            log.warning(f"  Import completed with {len(result.error_samples)} error samples:")
            for err in result.error_samples[:3]:
                log.warning(f"    {err}")
        else:
            log.info("  ✓ Import completed with no errors")

        log.info(f"  Stats → {result.error_config}")

    except GoogleAPIError as e:
        if e.code == 400 and "Conflicting document import" in str(e):
            log.warning("An import operation is already running on this data store branch.")
            log.warning("Please wait a few minutes for the current operation to complete before triggering a new sync.")
        else:
            log.error(f"Failed to import documents: {e}")
            raise


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3c: INFRASTRUCTURE PROVISIONING  —  Ensure Bucket & Data Store exist
# ─────────────────────────────────────────────────────────────────────────────

def ensure_infrastructure(cfg: Config) -> int:
    """
    Ensures that the target Google Cloud Storage bucket and Discovery Engine 
    Data Store pipeline exist before attempting to push data.
    
    Returns:
        The content_config (int) of the Data Store.
    """
    log.info("Checking infrastructure prerequisites...")
    
    # 1. Ensure GCS Bucket
    storage_client = storage.Client(project=cfg.project_id)
    bucket = storage_client.bucket(cfg.gcs_bucket)
    if not bucket.exists():
        log.info(f"  Bucket gs://{cfg.gcs_bucket} not found. Creating...")
        location = cfg.location if cfg.location != "global" else "us-central1"
        storage_client.create_bucket(bucket, location=location)
        log.info(f"  ✓ Bucket gs://{cfg.gcs_bucket} created.")
    else:
        log.info(f"  ✓ Bucket gs://{cfg.gcs_bucket} exists.")

    # 2. Ensure Discovery Engine Data Store
    ds_client = discoveryengine.DataStoreServiceClient()
    parent = (
        f"projects/{cfg.project_id}/locations/{cfg.location}"
        f"/collections/{cfg.collection_id}"
    )
    name = f"{parent}/dataStores/{cfg.data_store_id}"
    
    try:
        data_store = ds_client.get_data_store(name=name)
        actual_content_config = int(data_store.content_config)
        
        # We prefer CONTENT_REQUIRED, but we'll allow NO_CONTENT if we adapt the documents
        expected_content_config = int(discoveryengine.DataStore.ContentConfig.CONTENT_REQUIRED)
        
        if actual_content_config != expected_content_config:
            content_config_name = "unknown"
            try:
                # In proto-plus, we can get the name from the enum class
                content_config_name = discoveryengine.DataStore.ContentConfig(actual_content_config).name
            except Exception:
                content_config_name = str(actual_content_config)
            
            if actual_content_config == int(discoveryengine.DataStore.ContentConfig.NO_CONTENT):
                log.warning(f"  [!] Data Store '{cfg.data_store_id}' exists but is configured with {content_config_name}.")
                log.warning("  [!] This connector will proceed by stripping the 'content' field to remain compatible.")
            else:
                log.error(f"  [!] Data Store '{cfg.data_store_id}' has incompatible content_config: {content_config_name}")
                raise ValueError(
                    f"Data Store '{cfg.data_store_id}' exists with content_config={content_config_name}. "
                    "It must be CONTENT_REQUIRED or NO_CONTENT. "
                    "Please recreate the data store or use a compatible one."
                )
        else:
            log.info(f"  ✓ Data Store '{cfg.data_store_id}' exists with correct configuration.")
        
        return actual_content_config

    except NotFound:
        log.info(f"  Data Store '{cfg.data_store_id}' not found. Creating (this takes a few minutes)...")
        data_store = discoveryengine.DataStore(
            display_name=cfg.data_store_id,
            industry_vertical=discoveryengine.IndustryVertical.GENERIC,
            solution_types=[discoveryengine.SolutionType.SOLUTION_TYPE_SEARCH],
            content_config=discoveryengine.DataStore.ContentConfig.CONTENT_REQUIRED,
            # Enable ACL support directly
            acl_enabled=True,
        )
        request = discoveryengine.CreateDataStoreRequest(
            parent=parent,
            data_store=data_store,
            data_store_id=cfg.data_store_id,
        )
        operation = ds_client.create_data_store(request=request)
        operation.result()  # Wait for creation
        log.info(f"  ✓ Data Store '{cfg.data_store_id}' created successfully with ACL support enabled.")
        return int(discoveryengine.DataStore.ContentConfig.CONTENT_REQUIRED)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION  —  Local pre-flight checks before touching GCP
# ─────────────────────────────────────────────────────────────────────────────

def validate_document(doc: dict, index: int) -> bool:
    """Validate a single document conforms to required Discovery Engine schema."""
    errors = []
    if not doc.get("id"):
        errors.append("missing 'id'")
    if not isinstance(doc.get("structData"), dict):
        errors.append("'structData' must be a dict")
    content = doc.get("content", {})
    if content and "rawBytes" not in content:
        errors.append("'content.rawBytes' missing when content is provided")
    
    acl_info = doc.get("aclInfo")
    if acl_info is not None and not isinstance(acl_info, dict):
        errors.append("'aclInfo' must be a dict")

    if errors:
        log.error(f"Document [{index}] id={doc.get('id','?')} validation failed: {errors}")
        return False
    return True


def validate_all_documents(documents: List[dict]) -> List[dict]:
    """Filter out invalid documents and log a summary."""
    valid   = [d for i, d in enumerate(documents) if validate_document(d, i)]
    invalid = len(documents) - len(valid)
    if invalid:
        log.warning(f"Dropped {invalid} invalid documents (see errors above)")
    log.info(f"✓ {len(valid)}/{len(documents)} documents passed validation")
    return valid


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL PREVIEW  —  Dump sample documents for inspection without GCP
# ─────────────────────────────────────────────────────────────────────────────

def preview_documents(documents: List[dict], count: int = 2) -> None:
    """Print a sample of documents to stdout for local inspection."""
    log.info(f"\n{'='*60}")
    log.info(f"DOCUMENT PREVIEW ({min(count, len(documents))} of {len(documents)})")
    log.info(f"{'='*60}")
    import copy
    for doc in documents[:count]:
        # Decode content for readability using a deepcopy to avoid corrupting the real payload
        preview = copy.deepcopy(doc)
        if "content" in preview and "rawBytes" in preview["content"]:
            try:
                decoded = base64.b64decode(preview["content"]["rawBytes"]).decode("utf-8")
                preview["content"]["rawBytes"] = f"<decoded> {decoded[:200]}..."
            except Exception:
                pass
        print(json.dumps(preview, indent=2))
    log.info(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

def run(cfg: Config, local_preview_only: bool = False) -> None:
    """
    Main connector pipeline:
      1. FETCH   — pull data from JSONPlaceholder
      2. TRANSFORM — convert to Discovery Engine Document format
      3. VALIDATE  — local schema checks
      4. PREVIEW   — optional local dump
      5. GCS UPLOAD — stage JSONL in Cloud Storage
      6. DE IMPORT  — trigger Discovery Engine import from GCS
    """
    start = time.time()
    log.info("=" * 60)
    log.info("JSONPlaceholder → Gemini Enterprise Connector")
    log.info(f"Sync mode : {cfg.sync_mode.upper()}")
    log.info(f"Project   : {cfg.project_id}")
    log.info(f"DataStore : {cfg.data_store_id}")
    log.info("=" * 60)

    # ── STEP 0: SECRETS ──────────────────────────────────────────
    acl_mapping = None
    if cfg.secret_api_credentials:
        api_creds = fetch_secret(cfg.project_id, cfg.secret_api_credentials)
        if api_creds:
            log.info("  ✓ Successfully fetched API credentials from Secret Manager")
    
    if cfg.secret_acl_mapping:
        acl_payload = fetch_secret(cfg.project_id, cfg.secret_acl_mapping)
        if acl_payload:
            try:
                acl_mapping = json.loads(acl_payload)
                log.info("  ✓ Successfully fetched and parsed ACL mapping from Secret Manager")
            except json.JSONDecodeError as e:
                log.warning(f"  Failed to parse ACL mapping as JSON: {e}")

    # ── STEP 1: FETCH ────────────────────────────────────────────
    posts = fetch_posts(cfg.source_base_url)
    # LIMIT to 10 documents for testing as requested
    posts = posts[:10]
    log.info(f"  → Limited to {len(posts)} posts for testing")
    
    users = fetch_users(cfg.source_base_url)

    # ── STEP 2: TRANSFORM ────────────────────────────────────────
    log.info("Transforming posts to Discovery Engine document format...")
    documents = build_all_documents(posts, users, cfg.source_base_url, acl_mapping=acl_mapping)

    # ── STEP 3: VALIDATE ─────────────────────────────────────────
    documents = validate_all_documents(documents)

    # ── STEP 4: PREVIEW (always shown for observability) ─────────
    preview_documents(documents, count=2)

    if local_preview_only:
        log.info("LOCAL_PREVIEW_ONLY=true — skipping GCS upload and DE import")
        log.info(f"Total documents ready: {len(documents)}")
        return

    # ── STEP 5: PRE-REQUISITE CHECKS ─────────────────────────────
    actual_content_config = ensure_infrastructure(cfg)

    # If the data store is NO_CONTENT, we must strip the content field from documents
    # to avoid import errors, as Discovery Engine will reject them.
    if actual_content_config == int(discoveryengine.DataStore.ContentConfig.NO_CONTENT):
        log.warning("Stripping 'content' field from all documents for NO_CONTENT compatibility...")
        for doc in documents:
            doc.pop("content", None)

    # ── STEP 6: UPLOAD TO GCS ────────────────────────────────────
    gcs_uri = upload_to_gcs(documents, cfg.gcs_bucket, cfg.gcs_blob)

    # ── STEP 7: IMPORT INTO DISCOVERY ENGINE ─────────────────────
    import_from_gcs(cfg, gcs_uri)

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info(f"✅ Connector run complete in {elapsed:.1f}s")
    log.info(f"   {len(documents)} documents synced to data store: {cfg.data_store_id}")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="JSONPlaceholder → Gemini Enterprise custom connector"
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Run fetch + transform + validate locally without touching GCP",
    )
    args = parser.parse_args()

    try:
        cfg = Config()
    except KeyError as e:
        log.error(f"Missing required environment variable: {e}")
        log.error("See .env.example for the full list of required variables.")
        sys.exit(1)

    try:
        run(cfg, local_preview_only=args.preview)
    except Exception as e:
        log.exception(f"Connector failed: {e}")
        sys.exit(1)
