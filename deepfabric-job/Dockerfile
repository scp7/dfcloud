FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including Node.js (for npx/MCP servers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install deepfabric and dependencies
RUN pip install --no-cache-dir \
    deepfabric \
    google-cloud-storage \
    requests \
    pyyaml

# Copy entrypoint script
COPY entrypoint.py /app/entrypoint.py

# Make entrypoint executable
RUN chmod +x /app/entrypoint.py

ENTRYPOINT ["python", "/app/entrypoint.py"]
