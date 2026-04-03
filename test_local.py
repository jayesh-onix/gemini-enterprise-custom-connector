"""
=============================================================================
Local Validation Test — Run BEFORE connecting to GCP
=============================================================================
Tests the FETCH + TRANSFORM + VALIDATE pipeline entirely locally.
No GCP credentials required for this script if not fetching from Secret Manager.
(If testing ACL mappings, set a dummy environment variable or it will skip secrets).

Run with:
    python test_local.py
=============================================================================
"""

import json
import base64
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

# ── Import connector functions ────────────────────────────────────────────────
from connector import (
    fetch_data,
    fetch_users,
    build_discovery_engine_doc,
)

BASE_URL = "https://jsonplaceholder.typicode.com"

def test_fetch():
    """Test 1: Verify JSONPlaceholder API is reachable and returns data."""
    print("\n" + "="*60)
    print("TEST 1: Fetch from JSONPlaceholder")
    print("="*60)

    # fetch_data shuffles and returns 25 samples
    posts = fetch_data(BASE_URL, headers={})
    assert len(posts) == 25, f"Expected 25 posts due to sampling, got {len(posts)}"
    assert "id" in posts[0], "Post missing 'id' field"
    assert "title" in posts[0], "Post missing 'title' field"
    assert "body" in posts[0], "Post missing 'body' field"
    assert "userId" in posts[0], "Post missing 'userId' field"

    users = fetch_users(BASE_URL, headers={})
    assert len(users) == 10, f"Expected 10 users, got {len(users)}"

    print(f"  ✓ Fetched {len(posts)} random posts")
    print(f"  ✓ Fetched {len(users)} users")
    return posts, users

def test_transform(posts, users):
    """Test 2: Verify document transformation produces valid DE format."""
    print("\n" + "="*60)
    print("TEST 2: Transform to Discovery Engine Document Format with ACLs")
    print("="*60)

    sample_post = posts[0]
    
    # Test a dummy ACL mapping
    dummy_acl = {
        "readers": [
            {"group": "engineering@example.com"},
            {"user": "alice@example.com"}
        ]
    }
    
    doc = build_discovery_engine_doc(sample_post, users, acl_mapping=dummy_acl)

    # Validate required fields
    assert "id" in doc,         "Document missing 'id'"
    assert "structData" in doc, "Document missing 'structData'"
    assert "content" in doc,    "Document missing 'content'"
    assert doc["id"].startswith("doc-"), f"ID format wrong: {doc['id']}"
    assert "rawBytes" in doc["content"], "Content missing 'rawBytes'"
    assert "mimeType" in doc["content"], "Content missing 'mimeType'"

    # Verify rawBytes is valid base64
    decoded = base64.b64decode(doc["content"]["rawBytes"]).decode("utf-8")
    assert "Title:" in decoded, "Content does not contain title"
    assert "Body:"  in decoded, "Content does not contain body"

    # Verify ACL was mapped properly
    assert "aclInfo" in doc, "Document missing 'aclInfo'"
    readers = doc["aclInfo"].get("readers", [])
    assert len(readers) == 1
    principals = readers[0].get("principals", [])
    assert len(principals) == 2
    
    principal_groups = [p.get("groupId") for p in principals]
    principal_users = [p.get("userId") for p in principals]
    assert "engineering@example.com" in principal_groups
    assert "alice@example.com" in principal_users

    print(f"  ✓ Document ID format correct: {doc['id']}")
    print(f"  ✓ content rawBytes decoded cleanly ({len(decoded)} chars)")
    print(f"  ✓ aclInfo mapped correctly")
    return doc

def test_full_pipeline(posts, users):
    """Test 3: Build all sampled documents."""
    print("\n" + "="*60)
    print("TEST 3: Full Pipeline for 25 Sampled Posts")
    print("="*60)

    documents = []
    for post in posts:
        doc = build_discovery_engine_doc(post, users, acl_mapping=None)
        documents.append(doc)

    assert len(documents) == 25, f"Expected 25 documents, got {len(documents)}"
    print(f"  ✓ Built {len(documents)} documents successfully")

    output_file = "output_sample.jsonl"
    with open(output_file, "w") as f:
        for doc in documents:
            f.write(json.dumps(doc) + "\n")
    print(f"  ✓ Saved output to {output_file}")

if __name__ == "__main__":
    print("Starting Local Tests...")
    try:
        p, u = test_fetch()
        test_transform(p, u)
        test_full_pipeline(p, u)
        print("\n✅ ALL TESTS PASSED — Ready to run against GCP")
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
