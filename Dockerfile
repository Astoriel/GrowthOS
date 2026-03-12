FROM python:3.12-slim

WORKDIR /app

# Install build dependencies (DuckDB compiles some extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency resolution
RUN pip install --no-cache-dir uv

# Copy project files
COPY pyproject.toml .
COPY src/ src/

# Install the package (including sqlglot)
RUN uv pip install --system -e .

# Runtime defaults
ENV PYTHONUNBUFFERED=1
ENV GROWTH_QUERY_CACHE_TTL=300

# Map a volume for CSV data at /app/data
# Override with -e GROWTH_DATA_DIR=/your/path
ENV GROWTH_DATA_DIR=/app/data
VOLUME ["/app/data"]

# Stdio transport for MCP clients (Claude Desktop, Cursor, etc.)
CMD ["python", "-m", "growth_os.server"]

# -------------------------------------------------------
# Example usage:
#
# Build:
#   docker build -t growth-os .
#
# Run with CSV data:
#   docker run -e GROWTH_DATA_DIR=/data -v /local/csv:/data growth-os
#
# Run with PostgreSQL:
#   docker run -e POSTGRES_URL=postgresql://user:pass@host:5432/db growth-os
#
# Run with Stripe:
#   docker run -e STRIPE_API_KEY=sk_live_... growth-os
# -------------------------------------------------------
