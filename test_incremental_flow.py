import subprocess
import time
import requests
import json
import os
import signal
import sys
import re

def run_command(cmd):
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
    return result.stdout

def main():
    # 1. Start mock service in background
    print("Step 1: Starting mock service...")
    mock_proc = subprocess.Popen([sys.executable, "mock_service.py"])
    time.sleep(2)  # Give it time to start

    try:
        # 2. Initial Sync (Full)
        print("\nStep 2: Initial Sync (Fetching random sample)...")
        out1 = run_command([sys.executable, "connector.py", "--preview"])
        print(out1)
        
        # Extract sampled count from new log format: "✓ Sampled 25 random records..."
        match1 = re.search(r"Sampled (\d+) random records", out1)
        sampled_count = int(match1.group(1)) if match1 else 0
        print(f">>> Initial sync sampled {sampled_count} posts.")

        # 3. Simulate an update in the mock service
        print("\nStep 3: Simulating randomized data update...")
        resp = requests.post("http://localhost:8000/simulate-update")
        update_data = resp.json()
        expected_min = update_data["new_posts_added"] + update_data["posts_updated"]
        print(f"Mock Service updated: {update_data}")
        print(f"Expect some posts in next incremental sync (new + updated).")
        
        # 4. Incremental Sync
        print("\nStep 4: Incremental Sync (Should fetch randomized updates)...")
        # Use a timestamp that excludes initial posts (all 2024-01-01)
        out2 = run_command([sys.executable, "connector.py", "--preview", "--since", "2024-01-01T00:00:01Z"])
        print(out2)

        # Extract incremental count
        match2 = re.search(r"Sampled (\d+) random records", out2)
        incremental_count = int(match2.group(1)) if match2 else 0
        print(f">>> Incremental sync fetched {incremental_count} records.")

        if incremental_count > 0:
            print(f"\n✅ SUCCESS: Incremental sync correctly fetched {incremental_count} records!")
        else:
            print(f"\n❌ FAILURE: Incremental sync returned 0 records.")

    finally:
        print("\nStep 5: Cleaning up...")
        mock_proc.terminate()
        mock_proc.wait()
        if os.path.exists("mock_db.json"):
            os.remove("mock_db.json")
        print("Done.")

if __name__ == "__main__":
    # Ensure SOURCE_BASE_URL is set to local mock
    os.environ["SOURCE_BASE_URL"] = "http://localhost:8000"
    # Set dummy GCP vars so Config() doesn't fail even in preview mode
    os.environ.setdefault("GCP_PROJECT_ID", "mock-project")
    os.environ.setdefault("GCS_BUCKET", "mock-bucket")
    os.environ.setdefault("DATA_STORE_ID", "mock-ds")
    
    main()
