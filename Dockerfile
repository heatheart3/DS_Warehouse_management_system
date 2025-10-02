# Base image with Python runtime
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# System dependencies (none required currently, but keep placeholder for future packages)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definitions first for cache efficiency
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY *.py ./
COPY warehouse_pb2.py warehouse_pb2_grpc.py ./

# Default command runs the inventory server; docker-compose overrides for client
CMD ["python", "inventory_server.py", "--host", "0.0.0.0", "--port", "50051"]
