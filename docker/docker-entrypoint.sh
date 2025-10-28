#!/bin/bash
set -e

# Install packages into venv if not already installed
if [ -f /app/.venv/bin/pip ]; then
    /app/.venv/bin/pip install --no-cache-dir \
        psycopg2-binary==2.9.9 \
        pg8000==1.30.3 2>/dev/null || true
fi

# Run original Superset entrypoint
exec /usr/bin/run-server.sh "$@"