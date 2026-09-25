#!/usr/bin/env bash
# End-to-end test against running services + infrastructure.
# Requires: curl, jq, docker (for redis-cli / awslocal inside the containers).
# Run the worker with SIMULATED_FAILURE_RATE=0 so results are deterministic.
set -euo pipefail

API=${API:-http://localhost:8080}
WORKER=${WORKER:-http://localhost:8082}
RUN_ID=$(date +%s)-$RANDOM
FAILURES=0

step()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pass()  { printf '  \033[32mPASS\033[0m %s\n' "$*"; }
fail()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILURES=$((FAILURES + 1)); }

create() { # $1 idempotency key, $2 json body -> prints "<http_code> <body>"
  curl -s -o /tmp/e2e_body -w '%{http_code}' -X POST "${API}/api/v1/notifications" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $1" -d "$2"
  printf ' '
  cat /tmp/e2e_body
}

field() { # $1 notification id, $2 field
  curl -s "${API}/api/v1/notifications/$1" | jq -r ".$2 // empty"
}

wait_for_status() { # $1 id, $2 expected status, $3 timeout seconds
  local deadline=$((SECONDS + $3)) status=""
  while (( SECONDS < deadline )); do
    status=$(field "$1" status)
    [[ "${status}" == "$2" ]] && return 0
    sleep 1
  done
  echo "  (last status: ${status:-none})"
  return 1
}

metric() { # $1 outcome, $2 channel -> counter value or 0
  curl -s "${WORKER}/actuator/metrics/notification.delivery?tag=outcome:$1&tag=channel:$2" \
    | jq -r '(.measurements // [])[] | select(.statistic=="COUNT") | .value' 2>/dev/null || true
}

# ------------------------------------------------------------------------------------------
step "1. Happy path: EMAIL notification is accepted and delivered"
read -r CODE BODY <<<"$(create "e2e-${RUN_ID}" '{"userId":"u-1001","channel":"EMAIL","subject":"Welcome","body":"Hello"}')"
ID=$(jq -r .notificationId <<<"${BODY}")
[[ "${CODE}" == "202" ]] && pass "POST returned 202 (id=${ID})" || fail "expected 202, got ${CODE}: ${BODY}"
wait_for_status "${ID}" SENT 30 && pass "status reached SENT" || fail "status did not reach SENT"
[[ "$(field "${ID}" attempts)" == "1" ]] && pass "delivered on first attempt" || fail "attempts=$(field "${ID}" attempts)"

# ------------------------------------------------------------------------------------------
step "2. API idempotency: same Idempotency-Key returns the original id"
read -r CODE BODY <<<"$(create "e2e-${RUN_ID}" '{"userId":"u-1001","channel":"EMAIL","subject":"Welcome","body":"Hello"}')"
[[ "${CODE}" == "200" ]] && pass "replay returned 200" || fail "expected 200, got ${CODE}"
[[ "$(jq -r .notificationId <<<"${BODY}")" == "${ID}" ]] && pass "same notificationId returned" || fail "different id: ${BODY}"
[[ "$(jq -r .result <<<"${BODY}")" == "DUPLICATE_REQUEST" ]] && pass "result=DUPLICATE_REQUEST" || fail "${BODY}"

# ------------------------------------------------------------------------------------------
step "3. Delivery idempotency: a redelivered SQS copy is suppressed"
[[ "$(docker exec dns-redis redis-cli GET "notif:delivery:${ID}")" == "DELIVERED" ]] \
  && pass "Redis marker notif:delivery:${ID} = DELIVERED" || fail "delivery marker missing"
BEFORE=$(metric duplicate EMAIL); BEFORE=${BEFORE:-0}
DUP_EVENT=$(jq -cn --arg id "${ID}" '{notificationId:$id,userId:"u-1001",channel:"EMAIL",subject:"dup",body:"dup",createdAt:"2026-01-01T00:00:00Z"}')
docker exec dns-localstack sh -c \
  "awslocal sqs send-message --queue-url \$(awslocal sqs get-queue-url --queue-name notification-email-queue --query QueueUrl --output text) --message-body '${DUP_EVENT}'" > /dev/null
deadline=$((SECONDS + 30)); AFTER=${BEFORE}
while (( SECONDS < deadline )); do
  AFTER=$(metric duplicate EMAIL); AFTER=${AFTER:-0}
  awk "BEGIN{exit !(${AFTER} > ${BEFORE})}" && break
  sleep 1
done
awk "BEGIN{exit !(${AFTER} > ${BEFORE})}" && pass "duplicate counter ${BEFORE} -> ${AFTER}" || fail "duplicate not observed"
[[ "$(field "${ID}" status)" == "SENT" ]] && pass "status still SENT" || fail "status changed to $(field "${ID}" status)"

# ------------------------------------------------------------------------------------------
step "4. Retry + DLQ: a permanently failing SMS is retried 3x, then dead-lettered"
read -r CODE BODY <<<"$(create "e2e-${RUN_ID}-poison" '{"userId":"u-2002","channel":"SMS","body":"[FAIL_ALWAYS] code 123456"}')"
POISON=$(jq -r .notificationId <<<"${BODY}")
wait_for_status "${POISON}" DEAD_LETTERED 60 && pass "status reached DEAD_LETTERED" || fail "not dead-lettered"
[[ "$(field "${POISON}" attempts)" == "3" ]] && pass "attempts=3 (maxReceiveCount)" || fail "attempts=$(field "${POISON}" attempts)"
[[ -n "$(field "${POISON}" lastError)" ]] && pass "lastError recorded: $(field "${POISON}" lastError)" || fail "no lastError"

deadline=$((SECONDS + 30)); DEPTH=0
while (( SECONDS < deadline )); do
  DEPTH=$(curl -s "${WORKER}/admin/dlq/SMS" | jq -r .approximateMessages)
  (( DEPTH >= 1 )) && break
  sleep 1
done
(( DEPTH >= 1 )) && pass "SMS DLQ depth=${DEPTH}" || fail "message not in SMS DLQ"

# ------------------------------------------------------------------------------------------
step "5. Failure recovery: redrive the DLQ back to the source queue"
REDRIVE=$(curl -s -X POST "${WORKER}/admin/dlq/SMS/redrive?max=10")
jq -e --arg id "${POISON}" '.notificationIds | index($id)' <<<"${REDRIVE}" > /dev/null \
  && pass "redrive moved ${POISON} ($(jq -r .moved <<<"${REDRIVE}") message(s))" || fail "redrive result: ${REDRIVE}"
# Still poisoned, so it should cycle through retries and return to DEAD_LETTERED.
wait_for_status "${POISON}" DEAD_LETTERED 60 && pass "redriven message re-processed and dead-lettered again" \
  || fail "redriven message not re-processed"

# ------------------------------------------------------------------------------------------
step "6. Validation: bad requests are rejected"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${API}/api/v1/notifications" \
  -H 'Content-Type: application/json' -d '{"userId":"u-1","channel":"FAX","body":"x"}')
[[ "${CODE}" == "400" ]] && pass "unknown channel -> 400" || fail "expected 400, got ${CODE}"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${API}/api/v1/notifications" \
  -H 'Content-Type: application/json' -d '{"channel":"SMS","body":"x"}')
[[ "${CODE}" == "400" ]] && pass "missing userId -> 400" || fail "expected 400, got ${CODE}"

# ------------------------------------------------------------------------------------------
echo
if (( FAILURES > 0 )); then
  printf '\033[31m%d check(s) failed\033[0m\n' "${FAILURES}"
  exit 1
fi
printf '\033[32mAll end-to-end checks passed\033[0m\n'
