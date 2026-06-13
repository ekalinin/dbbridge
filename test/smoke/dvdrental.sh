#!/usr/bin/env bash
#
# Smoke test for a running docker-compose stack (see deploy/docker-compose.yaml).
# Exercises the full path: dbbridge REST API -> PostgreSQL (dvdrental) -> MinIO.
#
# Usage:
#   make up                       # start the stack first
#   test/smoke/dvdrental.sh       # run against dbbridge-blue (default :8081)
#   BASE_URL=http://localhost:8082 test/smoke/dvdrental.sh   # dbbridge-green
#
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8081}"
DB_ID="${DB_ID:-dvdrental}"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

echo "==> 1. List databases ($BASE_URL/v1/databases)"
dbs=$(curl -sf "$BASE_URL/v1/databases")
echo "$dbs" | jq .
echo "$dbs" | jq -e --arg id "$DB_ID" 'any(.[]; .id == $id and .healthy == true)' >/dev/null \
  || { echo "FAIL: database '$DB_ID' not found or unhealthy" >&2; exit 1; }

echo "==> 2. Run a synchronous query"
record=$(curl -sf -X POST "$BASE_URL/v1/queries" \
  -H 'Content-Type: application/json' \
  -d "{
    \"database_id\": \"$DB_ID\",
    \"sql\": \"SELECT category_id, name FROM category ORDER BY category_id LIMIT 5\",
    \"options\": {\"mode\": \"sync\"}
  }")
echo "$record" | jq .

state=$(echo "$record" | jq -r '.state')
[ "$state" = "SUCCEEDED" ] || { echo "FAIL: expected SUCCEEDED, got $state" >&2; exit 1; }

query_id=$(echo "$record" | jq -r '.id')
rows=$(echo "$record" | jq -r '.stats.rows_read')
echo "    query_id=$query_id rows_read=$rows"

echo "==> 3. Download the result"
result=$(curl -sf "$BASE_URL/v1/queries/$query_id/result")
echo "$result"

# Expect 5 JSONL rows, with the first category being "Action".
line_count=$(printf '%s\n' "$result" | grep -c '^{')
[ "$line_count" -eq 5 ] || { echo "FAIL: expected 5 rows, got $line_count" >&2; exit 1; }
printf '%s\n' "$result" | head -1 | jq -e '.name == "Action"' >/dev/null \
  || { echo "FAIL: first row is not 'Action'" >&2; exit 1; }

echo "==> OK: smoke test passed"
