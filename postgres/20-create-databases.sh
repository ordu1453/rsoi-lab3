#!/usr/bin/env bash
set -e

echo "=== Running database init script for variant v4 ==="


# TODO для создания баз прописать свой вариант
export VARIANT="4"
export SCRIPT_PATH=/docker-entrypoint-initdb.d/
export PGPASSWORD=postgres
psql -f "$SCRIPT_PATH/scripts/db-v$VARIANT.sql"
