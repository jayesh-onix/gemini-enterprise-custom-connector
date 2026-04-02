import json
import time
import os
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

DB_FILE = "mock_db.json"

def get_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def seed_db():
    """Initialize the database with dummy data if it doesn't exist."""
    if os.path.exists(DB_FILE):
        return
    
    # Generate 30 to 40 initial posts as requested for better analysis
    count = random.randint(30, 40)
    data = {
        "posts": [
            {
                "id": i,
                "userId": (i % 10) + 1,
                "title": f"Initial Post {i}",
                "body": f"This is the body of post {i}. It was created at the start.",
                "updated_at": "2024-01-01T00:00:00Z"
            } for i in range(1, count + 1)
        ],
        "users": [
            {"id": i, "username": f"user_{i}", "email": f"user_{i}@example.com", "company": {"name": "MockCorp"}}
            for i in range(1, 11)
        ],
        "comments": []
    }
    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Database seeded with {count} initial posts.")

class MockHandler(BaseHTTPRequestHandler):
    def _send_response(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def do_GET(self):
        seed_db()
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query = parse_qs(parsed_path.query)

        with open(DB_FILE, "r") as f:
            db = json.load(f)

        if path == "/posts":
            posts = db["posts"]
            # Handle incremental sync filter: updated_at_gt
            if "updated_at_gt" in query:
                since = query["updated_at_gt"][0]
                posts = [p for p in posts if p["updated_at"] > since]
            return self._send_response(posts)

        elif path == "/users":
            return self._send_response(db["users"])

        elif path.startswith("/posts/") and path.endswith("/comments"):
            # Return some random comments for any post
            comments = [
                {"id": i, "postId": 1, "name": f"Commenter {i}", "email": f"c{i}@ex.com", "body": "Great post!"}
                for i in range(1, random.randint(2, 5))
            ]
            return self._send_response(comments)

        self._send_response({"error": "Not Found"}, 404)

    def do_POST(self):
        """Special endpoint to simulate data changes for testing."""
        seed_db()
        if self.path == "/simulate-update":
            with open(DB_FILE, "r") as f:
                db = json.load(f)
            
            # Add a random number of new posts (2 to 8)
            new_count = random.randint(2, 8)
            last_id = max([p["id"] for p in db["posts"]]) if db["posts"] else 0
            
            new_posts = []
            for i in range(1, new_count + 1):
                new_id = last_id + i
                new_post = {
                    "id": new_id,
                    "userId": random.randint(1, 10),
                    "title": f"New Random Post {new_id}",
                    "body": "This post was added dynamically to test sync randomization.",
                    "updated_at": get_now_iso()
                }
                db["posts"].append(new_post)
                new_posts.append(new_id)
            
            # Also update a few existing posts randomly (3 to 6)
            updated_count = random.randint(3, 6)
            indices_to_update = random.sample(range(len(db["posts"]) - new_count), min(updated_count, len(db["posts"]) - new_count))
            for idx in indices_to_update:
                db["posts"][idx]["title"] += f" (Updated at {get_now_iso()})"
                db["posts"][idx]["updated_at"] = get_now_iso()

            with open(DB_FILE, "w") as f:
                json.dump(db, f, indent=2)
            
            return self._send_response({
                "message": "Database updated randomly",
                "new_posts_added": new_count,
                "posts_updated": updated_count,
                "total_posts": len(db["posts"])
            })

        self._send_response({"error": "Method Not Allowed"}, 405)

def run_server():
    # Remove old DB to ensure fresh randomization if it exists
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        
    seed_db()
    server_address = ("", 8000)
    httpd = HTTPServer(server_address, MockHandler)
    print("🚀 Mock Service running on http://localhost:8000")
    print("Endpoints:")
    print("  GET  /posts?updated_at_gt=2024-01-01T00:00:00Z")
    print("  POST /simulate-update (Randomly adds/updates posts)")
    httpd.serve_forever()

if __name__ == "__main__":
    run_server()
