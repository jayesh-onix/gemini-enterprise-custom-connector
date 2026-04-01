"""
=============================================================================
Local Validation Test — Run BEFORE connecting to GCP
=============================================================================
Tests the FETCH + TRANSFORM + VALIDATE pipeline entirely locally.
No GCP credentials required.

Run with:
    python test_local.py

Expected output:
  - All 100 posts fetched
  - All 100 documents pass validation
  - 2 sample documents printed to console
  - Output JSONL saved to ./output_sample.jsonl for inspection
=============================================================================
"""

import json
import base64
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

import requests


BASE_URL = "https://jsonplaceholder.typicode.com"

# ── Import connector functions ────────────────────────────────────────────────
from connector import (
    fetch_posts,
    fetch_users,
    fetch_comments_for_post,
    build_document,
    build_all_documents,
    validate_all_documents,
    preview_documents,
    encode_text,
)


def test_fetch():
    """Test 1: Verify JSONPlaceholder API is reachable and returns data."""
    print("\n" + "="*60)
    print("TEST 1: Fetch from JSONPlaceholder")
    print("="*60)

    posts = fetch_posts(BASE_URL)
    assert len(posts) == 100, f"Expected 100 posts, got {len(posts)}"
    assert "id" in posts[0], "Post missing 'id' field"
    assert "title" in posts[0], "Post missing 'title' field"
    assert "body" in posts[0], "Post missing 'body' field"
    assert "userId" in posts[0], "Post missing 'userId' field"

    users = fetch_users(BASE_URL)
    assert len(users) == 10, f"Expected 10 users, got {len(users)}"

    comments = fetch_comments_for_post(BASE_URL, post_id=1)
    assert len(comments) == 5, f"Expected 5 comments for post 1, got {len(comments)}"

    print(f"  ✓ Fetched {len(posts)} posts")
    print(f"  ✓ Fetched {len(users)} users")
    print(f"  ✓ Fetched {len(comments)} comments for post 1")
    return posts, users


def test_transform(posts, users):
    """Test 2: Verify document transformation produces valid DE format."""
    print("\n" + "="*60)
    print("TEST 2: Transform to Discovery Engine Document Format")
    print("="*60)

    # Test single document
    sample_post = posts[0]
    sample_comments = fetch_comments_for_post(BASE_URL, sample_post["id"])
    doc = build_document(sample_post, users, sample_comments)

    # Validate required fields
    assert "id" in doc,         "Document missing 'id'"
    assert "structData" in doc, "Document missing 'structData'"
    assert "content" in doc,    "Document missing 'content'"
    assert doc["id"].startswith("jsonplaceholder-post-"), \
        f"ID format wrong: {doc['id']}"
    assert "rawBytes" in doc["content"], "Content missing 'rawBytes'"
    assert "mimeType" in doc["content"], "Content missing 'mimeType'"

    # Verify rawBytes is valid base64
    decoded = base64.b64decode(doc["content"]["rawBytes"]).decode("utf-8")
    assert "Title:" in decoded, "Content does not contain title"
    assert "Body:"  in decoded, "Content does not contain body"

    # Verify structData fields
    struct = doc["structData"]
    assert struct["source"] == "JSONPlaceholder", "Source field wrong"
    assert struct["url"].startswith("https://"), "URL field wrong"

    print(f"  ✓ Document ID format correct: {doc['id']}")
    print(f"  ✓ structData has {len(struct)} fields")
    print(f"  ✓ content rawBytes decoded cleanly ({len(decoded)} chars)")
    return doc


def test_full_pipeline(posts, users):
    """Test 3: Build all 100 documents and validate them."""
    print("\n" + "="*60)
    print("TEST 3: Full Pipeline (100 posts, no comments for speed)")
    print("="*60)

    # Build without comments for speed in local test
    documents = []
    for post in posts:
        doc = build_document(post, users, [])
        documents.append(doc)

    validated = validate_all_documents(documents)

    assert len(validated) == 100, f"Expected 100 valid docs, got {len(validated)}"
    print(f"  ✓ All {len(validated)} documents valid")

    # Check ID uniqueness
    ids = [d["id"] for d in validated]
    assert len(ids) == len(set(ids)), "Duplicate document IDs detected!"
    print(f"  ✓ All document IDs are unique")

    return validated


def test_jsonl_output(documents):
    """Test 4: Write JSONL file and verify it can be re-parsed correctly."""
    print("\n" + "="*60)
    print("TEST 4: JSONL Output File")
    print("="*60)

    output_path = os.path.join(os.path.dirname(__file__), "output_sample.jsonl")
    jsonl = "\n".join(json.dumps(d, ensure_ascii=False) for d in documents)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(jsonl)

    # Verify the file can be re-read
    with open(output_path, "r", encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]

    assert len(lines) == len(documents), "JSONL line count mismatch"
    print(f"  ✓ Wrote {len(lines)} documents to {output_path}")
    print(f"  ✓ File size: {os.path.getsize(output_path)/1024:.1f} KB")
    print(f"  ✓ All lines parseable as JSON")
    print(f"\n  → Inspect this file before uploading to GCS:")
    print(f"    cat {output_path} | python -m json.tool | head -60")


def run_all_tests():
    print("\n" + "█"*60)
    print("  JSONPlaceholder Connector — Local Validation Suite")
    print("█"*60)

    try:
        posts, users = test_fetch()
        test_transform(posts, users)
        documents = test_full_pipeline(posts, users)
        test_jsonl_output(documents)

        # Show sample documents
        print("\n" + "="*60)
        print("SAMPLE DOCUMENT OUTPUT (2 of 100)")
        print("="*60)
        preview_documents(documents, count=2)

        print("█"*60)
        print("  ✅ ALL TESTS PASSED — Ready to run against GCP")
        print("█"*60)
        print()
        print("Next steps:")
        print("  1. Review output_sample.jsonl to confirm document format")
        print("  2. Create GCS bucket and data store in GCP Console")
        print("  3. Fill in .env from .env.example")
        print("  4. Run: python connector.py")
        print()

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except requests.HTTPError as e:
        print(f"\n❌ API ERROR: {e}")
        print("Check your internet connection and that jsonplaceholder.typicode.com is reachable")
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()
