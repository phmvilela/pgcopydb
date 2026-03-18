# Multi-stage build for security and size optimization
FROM python:3.10-slim as base

# Set working directory
WORKDIR /app

# Install system dependencies (minimal surface area)
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY pg_cyan/ ./pg_cyan/

# Create non-root user for security
RUN useradd -m -u 1000 migration && chown -R migration:migration /app

USER migration

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default entrypoint (can be overridden)
ENTRYPOINT ["python", "-m", "pg_cyan.cli"]
CMD ["--help"]
