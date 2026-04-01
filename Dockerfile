# ─────────────────────────────────────────────────────────────────────────────
# JSONPlaceholder → Gemini Enterprise Connector
# Docker image for Cloud Run Job deployment
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# Security: run as non-root
RUN useradd --create-home --shell /bin/bash connector
WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy connector code
COPY connector.py .

# Switch to non-root user
USER connector

# Cloud Run Jobs execute CMD and exit
CMD ["python", "connector.py"]
