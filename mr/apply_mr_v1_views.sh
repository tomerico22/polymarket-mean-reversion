#!/bin/bash
set -euo pipefail

: "${DB_URL:?DB_URL must be set}"

PSQL="${PSQL_BIN:-/opt/homebrew/opt/postgresql@16/bin/psql}"

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$THIS_DIR/sql/mr_v1_analytics_views.sql"

echo "[MR_V1_VIEWS] Applying views from $SQL_FILE"
"$PSQL" "$DB_URL" -f "$SQL_FILE"
echo "[MR_V1_VIEWS] Done."
