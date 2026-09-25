#!/bin/bash
# Provisions SNS -> SQS fan-out with per-channel DLQs.
#   notification-events (SNS)
#     ├─ filter channel=EMAIL -> notification-email-queue -> (3 failures) -> notification-email-dlq
#     ├─ filter channel=SMS   -> notification-sms-queue   -> (3 failures) -> notification-sms-dlq
#     └─ filter channel=PUSH  -> notification-push-queue  -> (3 failures) -> notification-push-dlq
set -euo pipefail

MAX_RECEIVE=3          # keep in sync with app.worker.max-receive-count
VISIBILITY_TIMEOUT=30
DLQ_RETENTION=1209600  # 14 days

TOPIC_ARN=$(awslocal sns create-topic --name notification-events --query TopicArn --output text)
echo "SNS topic: ${TOPIC_ARN}"

for CHANNEL in EMAIL SMS PUSH; do
  LOWER=$(echo "${CHANNEL}" | tr '[:upper:]' '[:lower:]')

  DLQ_URL=$(awslocal sqs create-queue --queue-name "notification-${LOWER}-dlq" \
    --attributes "MessageRetentionPeriod=${DLQ_RETENTION}" --query QueueUrl --output text)
  DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "${DLQ_URL}" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)

  QUEUE_URL=$(awslocal sqs create-queue --queue-name "notification-${LOWER}-queue" \
    --attributes "{\"VisibilityTimeout\":\"${VISIBILITY_TIMEOUT}\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE}\\\"}\"}" \
    --query QueueUrl --output text)
  QUEUE_ARN=$(awslocal sqs get-queue-attributes --queue-url "${QUEUE_URL}" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)

  # RawMessageDelivery: SQS body is our JSON, not the SNS envelope. FilterPolicy routes by channel.
  awslocal sns subscribe --topic-arn "${TOPIC_ARN}" --protocol sqs --notification-endpoint "${QUEUE_ARN}" \
    --attributes "{\"RawMessageDelivery\":\"true\",\"FilterPolicy\":\"{\\\"channel\\\":[\\\"${CHANNEL}\\\"]}\"}" > /dev/null

  echo "Channel ${CHANNEL}: ${QUEUE_URL} (DLQ ${DLQ_URL})"
done

touch /tmp/notification-infra-ready
echo "LocalStack notification infrastructure ready."
