#!/usr/bin/env bash
#
# Async smoke test for a running docker-compose stack (see deploy/docker-compose.yaml).
# Submits a query that deliberately runs for ~3s (pg_sleep) so you can observe the
# in-flight metric while it executes, then polls until completion.
#
# Exercises: dbbridge REST API (async + polling) -> PostgreSQL (dvdrental) -> MinIO,
# plus the Prometheus metrics pipeline.
#
# Usage:
#   make up                              # start the stack first
#   test/smoke/dvdrental_async.sh        # against dbbridge-blue (:8081)
#   BASE_URL=http://localhost:8082 test/smoke/dvdrental_async.sh   # dbbridge-green
#
# Optional:
#   PROM_URL=http://localhost:9090       # Prometheus, for the cumulative-counter check
#
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8081}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
DB_ID="${DB_ID:-dvdrental}"
SLEEP_SECONDS="${SLEEP_SECONDS:-3}"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

# Live in-flight gauge read straight from the instance's /metrics (real-time,
# unlike Prometheus which only scrapes every few seconds).
inflight_now() {
  curl -sf "$BASE_URL/metrics" | awk '/^dbbridge_inflight_queries /{print $2}'
}

echo "==> 1. Submit an async query (~${SLEEP_SECONDS}s via pg_sleep)"
record=$(curl -sf -i -X POST "$BASE_URL/v1/queries" \
  -H 'Content-Type: application/json' \
  -d "{
    \"database_id\": \"$DB_ID\",
    \"sql\": \"SELECT title FROM film, pg_sleep($SLEEP_SECONDS) LIMIT 5\",
    \"options\": {\"mode\": \"async\"}
  }")

http_status=$(printf '%s' "$record" | awk 'NR==1{print $2}')
body=$(printf '%s' "$record" | sed '1,/^\r\{0,1\}$/d')
[ "$http_status" = "202" ] || { echo "FAIL: expected HTTP 202 Accepted, got $http_status" >&2; exit 1; }

query_id=$(echo "$body" | jq -r '.id')
[ -n "$query_id" ] && [ "$query_id" != "null" ] || { echo "FAIL: no query id returned" >&2; exit 1; }
echo "    accepted: query_id=$query_id state=$(echo "$body" | jq -r '.state')"

echo "==> 2. Observe in-flight metric while the query runs"
sleep 1
mid=$(inflight_now)
echo "    dbbridge_inflight_queries (mid-flight) = ${mid:-<none>}"
[ "${mid:-0}" != "0" ] || echo "    WARN: gauge already 0 (query may have finished early)"

echo "==> 3. Poll until terminal state"
deadline=$(( SECONDS + 30 ))
while :; do
  rec=$(curl -sf "$BASE_URL/v1/queries/$query_id")
  state=$(echo "$rec" | jq -r '.state')
  case "$state" in
    SUCCEEDED|FAILED|CANCELED) break ;;
  esac
  [ "$SECONDS" -lt "$deadline" ] || { echo "FAIL: query did not finish within 30s" >&2; exit 1; }
  sleep 0.5
done

echo "$rec" | jq '{state, rows_read: .stats.rows_read, db_exec_ms: (.stats.db_exec_duration/1000000), result_locator: .result.locator}'
[ "$state" = "SUCCEEDED" ] || { echo "FAIL: expected SUCCEEDED, got $state" >&2; exit 1; }

after=$(inflight_now)
echo "    dbbridge_inflight_queries (after) = ${after:-<none>}"

echo "==> 4. Download the result"
result=$(curl -sf "$BASE_URL/v1/queries/$query_id/result")
echo "$result"
line_count=$(printf '%s\n' "$result" | grep -c '^{')
[ "$line_count" -eq 5 ] || { echo "FAIL: expected 5 rows, got $line_count" >&2; exit 1; }

echo "==> 5. Check the cumulative counter in Prometheus"
if curl -sf "$PROM_URL/-/healthy" >/dev/null 2>&1; then
  total=$(curl -sf -G "$PROM_URL/api/v1/query" \
    --data-urlencode 'query=sum(dbbridge_queries_total{state="SUCCEEDED"})' \
    | jq -r '.data.result[0].value[1] // "0"')
  echo "    sum(dbbridge_queries_total{state=\"SUCCEEDED\"}) = $total"
else
  echo "    SKIP: Prometheus not reachable at $PROM_URL"
fi

echo "==> OK: async smoke test passed"
