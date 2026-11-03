
#!/usr/bin/env bash
# Demonstration of worker failure and recovery against Docker Compose.
# Usage: ./scripts/demo_crash.sh
# Requires: docker compose, curl, jq.
# Start stack first: docker compose up --build -d

set -euo pipefail

API="${API:-http://localhost:8000}"

# Generate unique identifiers for every execution
RUN_ID="$(date +%Y%m%d%H%M%S)"
WF="crash-demo-${RUN_ID}"
ORDER="ORD-CRASH-${RUN_ID}"
RECIPIENT="crash-demo-${RUN_ID}@example.com"

TMP_JSON="$(mktemp)"
trap 'rm -f "$TMP_JSON"' EXIT

say() {
    printf '\n\033[1m== %s\033[0m\n' "$*"
}

status() {
    curl -fsS "$API/workflows/$WF" |
        jq '{
            temporal_status,
            status,
            steps: (.steps // {} | map_values(.status)),
            pending_activities
        }'
}

say "1. Preparing a fresh workflow"

# Update workflow ID and test business data in the example JSON.
jq --arg wf "$WF" \
   --arg order "$ORDER" \
   --arg recipient "$RECIPIENT" '
   if has("workflow_id") then
       .workflow_id = $wf
   elif has("id") then
       .id = $wf
   else
       error("Could not find top-level workflow_id or id in JSON")
   end
   | walk(
       if type == "string" then
           gsub("ORD-CRASH-1"; $order)
           | gsub("crash-demo@example.com"; $recipient)
       else .
       end
   )
' examples/crash-demo.json > "$TMP_JSON"

echo "Workflow ID : $WF"
echo "Order ID    : $ORDER"
echo "Recipient   : $RECIPIENT"

say "2. Submit workflow and verify it is a fresh execution"

RESPONSE="$(curl -fsS -X POST "$API/workflows" \
    -H 'content-type: application/json' \
    --data @"$TMP_JSON")"

echo "$RESPONSE" | jq .

DUPLICATE="$(echo "$RESPONSE" | jq -r '.duplicate // false')"

if [ "$DUPLICATE" = "true" ]; then
    echo "ERROR: API returned an existing workflow. Aborting demo."
    exit 1
fi

say "3. Wait for worker failure and restart"

sleep 6

docker compose ps worker || true

say "Workflow status after the crash"
status

say "4. Ensure worker is running again"

docker compose up -d worker

for i in $(seq 1 60); do
    WORKER_STATE="$(docker compose ps --format json worker |
        jq -r '.[0].State // empty' 2>/dev/null || true)"

    if [ "$WORKER_STATE" = "running" ]; then
        break
    fi

    sleep 2
done

say "5. Wait for workflow completion"

COMPLETED=false

for i in $(seq 1 60); do
    CURRENT_STATUS="$(curl -fsS "$API/workflows/$WF" |
        jq -r '.temporal_status // .status')"

    if [ "$CURRENT_STATUS" = "COMPLETED" ]; then
        COMPLETED=true
        break
    elif [ "$CURRENT_STATUS" = "FAILED" ] ||
         [ "$CURRENT_STATUS" = "CANCELED" ]; then
        echo "Workflow ended with status: $CURRENT_STATUS"
        status
        exit 1
    fi

    sleep 2
done

if [ "$COMPLETED" != "true" ]; then
    echo "ERROR: Workflow did not complete within the timeout."
    status
    exit 1
fi

say "6. Final workflow state"
status

say "7. Verify idempotency and business records"

docker compose exec -T \
    -e DEMO_ORDER="$ORDER" \
    -e DEMO_RECIPIENT="$RECIPIENT" \
    worker python - <<'PY'
import sqlite3
import os

db_path = os.environ.get("DATA_DIR", "/data") + "/business.db"
order = os.environ["DEMO_ORDER"]
recipient = os.environ["DEMO_RECIPIENT"]

conn = sqlite3.connect(db_path)

charges = conn.execute(
    "SELECT COUNT(*) FROM provider_charges WHERE reference = ?",
    (order,)
).fetchone()[0]

notifications = conn.execute(
    "SELECT COUNT(*) FROM notifications WHERE recipient = ?",
    (recipient,)
).fetchone()[0]

print("Order ID                       :", order)
print("Provider charges               :", charges)
print("Notifications sent             :", notifications)

print("\nIMPORTANT:")
print("Expected provider charges = 1")
print("Expected notifications = 1")

if charges != 1 or notifications != 1:
    raise SystemExit("IDEMPOTENCY CHECK FAILED")

print("\nIDEMPOTENCY CHECK PASSED")
conn.close()
PY

echo
echo "Workflow ID: $WF"
echo "Open http://localhost:8233 to inspect Temporal workflow history."
