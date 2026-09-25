# Code Walkthrough

A guided tour of the codebase: where to start, how one notification travels through the code, and
exercises to check your understanding. Read [DESIGN.md](DESIGN.md) first for the *why*; this document
covers the *where*.

All paths below are relative to `src/main/java/com/huiying/notification/` inside each module.

## 1. Suggested reading order

| Step | File | What to look for | Time |
|---|---|---|---|
| 1 | `common/NotificationEvent.java` | The one message type that flows everywhere | 2 min |
| 2 | `common/RedisKeys.java` | All three Redis key families | 2 min |
| 3 | `common/NotificationStatus.java` | The state machine as an enum (`allowedPredecessors`) | 5 min |
| 4 | `common/NotificationStatusStore.java` | Lua compare-and-set; why not plain `HSET` | 10 min |
| 5 | `api/web/NotificationController.java` | 202 vs 200, `Idempotency-Key` header | 5 min |
| 6 | `api/service/NotificationService.java` | `SET NX`, publish with timeout, rollback on failure | 10 min |
| 7 | `dispatcher/kafka/NotificationRequestedConsumer.java` | Kafka → SNS, why duplicates are acceptable here | 5 min |
| 8 | `dispatcher/kafka/KafkaConfig.java` | Backoff, DLT, non-retryable exceptions | 10 min |
| 9 | `dispatcher/sns/SnsPublisher.java` | Message attributes that drive SNS filter policies | 5 min |
| 10 | `localstack/init-aws.sh` (repo root) | Queues, DLQs, RedrivePolicy, subscriptions | 5 min |
| 11 | `worker/listener/NotificationQueueListener.java` | **The core**: every branch of `handle` and `deliver` | 20 min |
| 12 | `worker/idempotency/IdempotencyGuard.java` | Lease acquire / mark / compare-and-delete | 15 min |
| 13 | `worker/retry/BackoffPolicy.java` | Exponential backoff with jitter and caps | 5 min |
| 14 | `worker/dlq/DlqRedriveService.java` | Send-then-delete ordering | 10 min |
| 15 | Tests in each module | Tests document the intended behavior of each branch | 20 min |

Tip: read `NotificationQueueListenerTest` side by side with `NotificationQueueListener` — each test
name describes one branch of the decision tree.

## 2. Follow one notification

### 2.1 Happy path

```
POST /api/v1/notifications  {userId:"u-1", channel:"EMAIL", ...}  Idempotency-Key: k1
```

1. **`NotificationController.create`** validates the body (`@Valid` on `CreateNotificationRequest`)
   and the header length, then calls the service.
2. **`NotificationService.create`**
   - generates `notificationId` (UUID);
   - `SET notif:api-idem:k1 <id> NX EX 24h` → first time, returns `true`;
   - `NotificationStatusStore.initialize` writes hash `notif:status:<id>` with `status=ACCEPTED`;
   - `kafkaTemplate.send("notification.requested", "u-1", json).get(5s)` — blocks until the broker
     acknowledges (`acks=all`), so a 202 means the event is durable.
3. Controller returns **202** with `Location: /api/v1/notifications/<id>`.
4. **`NotificationRequestedConsumer.onMessage`** (dispatcher, consumer group `notification-dispatcher`)
   deserializes the record and calls `SnsPublisher.publish`, which attaches the message attribute
   `channel=EMAIL`. It then calls `transition(id, DISPATCHED)`.
5. SNS evaluates each subscription's **FilterPolicy** (`init-aws.sh`); only
   `notification-email-queue` matches. `RawMessageDelivery=true` means the SQS body is our JSON,
   not an SNS envelope.
6. **`NotificationQueueListener.onMessage`** (worker) receives it; `receiveCount=1`.
   - `IdempotencyGuard.tryAcquire` → `SET notif:delivery:<id> LEASE:<uuid> NX EX 60s` → `ACQUIRED`;
   - `SenderRegistry.get(EMAIL).send(event)` → `EmailSender` (a `MockChannelSender`) logs delivery;
   - `markDelivered` → value becomes `DELIVERED` (TTL 7 d);
   - `transition(id, SENT)`; counter `notification.delivery{outcome=sent}` +1;
   - method returns normally → Spring Cloud AWS deletes the SQS message.

> Note the race in steps 4 and 6: the worker may run `transition(SENT)` *before* the dispatcher runs
> `transition(DISPATCHED)`. Find where this is handled (answer: the Lua script in
> `NotificationStatusStore` rejects `SENT → DISPATCHED`).

### 2.2 Failure path (`body` contains `[FAIL_ALWAYS]`)

| Receive | What `deliver` does | Status | Visibility |
|---|---|---|---|
| 1 | `send` throws → `release` lease → not final attempt | `RETRYING` | `changeTo(2s)` |
| 2 | same | `RETRYING` | `changeTo(4s)` |
| 3 | same, but `receiveCount >= maxReceiveCount` | `DEAD_LETTERED` | `changeTo(0)` |
| (4) | SQS sees receive count exceeded → moves message to `notification-sms-dlq` | — | — |

Each attempt ends by throwing `DeliveryFailedException`, which is what keeps the message on the queue.

Then `POST /admin/dlq/SMS/redrive` → `DlqRedriveService.redrive` moves it back, `transition(REDRIVEN)`,
and the cycle starts again with a fresh receive count.

### 2.3 Duplicate path

A second copy of an already-delivered message arrives:
`tryAcquire` → `SET NX` fails → `GET` returns `DELIVERED` → `ALREADY_DELIVERED` →
log + counter `outcome=duplicate` → **return normally** so the copy is deleted.
`scripts/e2e-test.sh` step 3 reproduces this by injecting a copy directly into SQS.

## 3. Where each concern lives

| Concern | Code |
|---|---|
| Kafka + SNS/SQS asynchronous delivery | `NotificationService` (produce), `NotificationRequestedConsumer` + `SnsPublisher` (fan-out), `NotificationQueueListener` (consume), `init-aws.sh` (topology) |
| Redis-backed idempotency | `NotificationService` (API key), `IdempotencyGuard` (delivery lease), `RedisKeys` |
| Retry handling | `KafkaConfig.kafkaErrorHandler` (dispatcher), `BackoffPolicy` + `NotificationQueueListener.deliver` (worker) |
| Dead-letter queues | `init-aws.sh` (RedrivePolicy), `KafkaConfig` (DLT), `DeadLetterConsumer` |
| Failure recovery workflow | `DlqRedriveService`, `DlqAdminController` |
| Consistent status under concurrency | `NotificationStatus.allowedPredecessors`, `NotificationStatusStore.transition` |
| Configuration | each module's `application.yml`, `WorkerProperties` |
| Fault injection | `FailureSimulator` |

## 4. Exercises

Try to answer from the code before expanding the hint.

**Q1.** A client sends a request, Kafka is down, and the API returns 503. The client retries with the same
`Idempotency-Key` after Kafka recovers. Does it get a new notification or a `DUPLICATE_REQUEST`?

<details><summary>Answer</summary>
A new notification. In <code>NotificationService.create</code>, the catch block deletes both the status
hash and the idempotency key, so the retry is treated as first-seen. Returning DUPLICATE_REQUEST would
point the client at a notification that was never published.
</details>

**Q2.** Why does `IdempotencyGuard.release` use a Lua script instead of `redis.delete(key)`?

<details><summary>Answer</summary>
Worker A acquires the lease, then stalls for longer than 60 s; the lease expires; worker B acquires a new
lease and starts sending. When A finally fails and calls a plain DEL, it would delete B's lease, letting a
third worker acquire and send concurrently. Comparing the token first means A can only delete its own lease.
</details>

**Q3.** What happens if you set `app.worker.max-receive-count: 5` but leave the queue's
`maxReceiveCount` at 3?

<details><summary>Answer</summary>
SQS still moves the message to the DLQ after 3 receives, but the worker never sees
<code>receiveCount >= 5</code>, so it records <code>RETRYING</code> instead of <code>DEAD_LETTERED</code>.
The status would be wrong even though the message is correctly dead-lettered. That is why the two values
must match (see comments in <code>application.yml</code> and <code>init-aws.sh</code>).
</details>

**Q4.** Why does the malformed-payload branch call `changeTo(0)` rather than just throwing?

<details><summary>Answer</summary>
A malformed message will never succeed. With the default 30 s visibility timeout it would take ~90 s to
reach the DLQ, being received and failing each time. Visibility 0 makes it reappear immediately so it
exhausts its receives in seconds.
</details>

**Q5.** In `DlqRedriveService.redrive`, what would go wrong if the order were delete-then-send?

<details><summary>Answer</summary>
A crash between the two calls would lose the message permanently. With send-then-delete, a crash produces
a duplicate in the source queue, which the delivery lease absorbs.
</details>

**Q6.** The dispatcher's error handler marks `JsonProcessingException` as not retryable. Why doesn't the
worker need an equivalent list?

<details><summary>Answer</summary>
The worker handles it explicitly: the parse happens in its own try/catch before any delivery logic, and
the catch fast-tracks the message to the DLQ with visibility 0. Retries in the worker are driven by SQS,
not by an in-process error handler.
</details>

**Q7.** Can `SENT` ever be overwritten? Trace every call to `transition` to prove your answer.

<details><summary>Answer</summary>
No. <code>SENT</code> is not in any state's <code>allowedPredecessors</code>, so the Lua script returns 0
(REJECTED) for every transition out of it — including a late <code>DISPATCHED</code>, a
<code>RETRYING</code> from a concurrent duplicate, or a <code>REDRIVEN</code>.
</details>

**Q8 (design).** Per-user ordering is preserved in Kafka. Is it preserved at delivery time? What would you
change to guarantee it?

<details><summary>Answer</summary>
No — SNS fan-out into standard SQS queues and parallel workers reorder messages, and retries delay
individual messages. Options: SQS FIFO queues with <code>MessageGroupId = userId</code> (and FIFO SNS),
at the cost of throughput and head-of-line blocking per user.
</details>

## 5. Hands-on experiments

1. `docker compose --profile tools up -d` and open Kafka UI (http://localhost:8090). Send a notification
   and find it in `notification.requested`. Which partition did it land on? Send another for the same
   `userId` — same partition?
2. Start the worker with `SIMULATED_FAILURE_RATE=0.7`, send 20 notifications and watch the logs for
   backoff delays. Compare `/actuator/metrics/notification.delivery?tag=outcome:retry` with
   `outcome:sent`.
3. Put a breakpoint in `IdempotencyGuard.tryAcquire`, run two worker instances
   (`SERVER_PORT=8083` for the second) and inject the same message twice. Observe `IN_PROGRESS`.
4. Stop Redis (`docker stop dns-redis`) while sending notifications. What does the API return? Where do
   worker messages end up? Restart Redis and redrive.
5. Add a fourth channel `WEBHOOK`: enum value, sender, queue + DLQ + subscription, config. Count how many
   files change — the dispatcher should not be one of them.
