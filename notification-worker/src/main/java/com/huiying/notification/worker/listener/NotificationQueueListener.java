package com.huiying.notification.worker.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.common.NotificationEvent;
import com.huiying.notification.common.NotificationStatus;
import com.huiying.notification.common.NotificationStatusStore;
import com.huiying.notification.worker.config.WorkerProperties;
import com.huiying.notification.worker.idempotency.IdempotencyGuard;
import com.huiying.notification.worker.retry.BackoffPolicy;
import com.huiying.notification.worker.sender.SenderRegistry;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.Visibility;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * Consumes the three per-channel SQS queues and delivers each notification exactly once
 * from the user's point of view.
 *
 * <p>Acknowledgement mode is ON_SUCCESS (the Spring Cloud AWS default):
 * <ul>
 *   <li>returning normally deletes the message from the queue;</li>
 *   <li>throwing leaves it on the queue, and it becomes visible again after its visibility timeout.</li>
 * </ul>
 * On failure we change the visibility timeout to implement exponential backoff. After
 * {@code maxReceiveCount} receives, the queue's RedrivePolicy moves the message to its DLQ.
 */
@Component
public class NotificationQueueListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationQueueListener.class);

    /** Header populated by Spring Cloud AWS from the SQS ApproximateReceiveCount system attribute. */
    static final String RECEIVE_COUNT_HEADER = "Sqs_Msa_ApproximateReceiveCount";
    static final String METRIC_NAME = "notification.delivery";

    private final ObjectMapper objectMapper;
    private final IdempotencyGuard idempotencyGuard;
    private final SenderRegistry senderRegistry;
    private final NotificationStatusStore statusStore;
    private final BackoffPolicy backoffPolicy;
    private final WorkerProperties properties;
    private final MeterRegistry meterRegistry;

    public NotificationQueueListener(ObjectMapper objectMapper, IdempotencyGuard idempotencyGuard,
                                     SenderRegistry senderRegistry, NotificationStatusStore statusStore,
                                     BackoffPolicy backoffPolicy, WorkerProperties properties,
                                     MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.idempotencyGuard = idempotencyGuard;
        this.senderRegistry = senderRegistry;
        this.statusStore = statusStore;
        this.backoffPolicy = backoffPolicy;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
    }

    @SqsListener({"${app.worker.queues.EMAIL}", "${app.worker.queues.SMS}", "${app.worker.queues.PUSH}"})
    public void onMessage(Message<String> message, Visibility visibility) {
        handle(message.getPayload(), receiveCount(message), visibility);
    }

    /** Package-private entry point so the full decision logic can be unit tested without SQS. */
    void handle(String payload, int receiveCount, Visibility visibility) {
        NotificationEvent event;
        try {
            event = objectMapper.readValue(payload, NotificationEvent.class);
        } catch (JsonProcessingException ex) {
            // Poison message: make it visible again immediately so it exhausts maxReceiveCount
            // quickly and lands in the DLQ instead of occupying the queue.
            log.error("Malformed message (receiveCount={}), routing toward DLQ: {}", receiveCount, ex.getOriginalMessage());
            record("unknown", DeliveryOutcome.MALFORMED);
            changeVisibility(visibility, 0);
            throw new DeliveryFailedException("Malformed notification payload", ex);
        }

        String id = event.notificationId();
        statusStore.recordAttempt(id, receiveCount);

        IdempotencyGuard.Acquisition acquisition = idempotencyGuard.tryAcquire(id);
        switch (acquisition.outcome()) {
            case ALREADY_DELIVERED -> {
                // Duplicate from SQS at-least-once delivery, a dispatcher re-publish, or a DLQ redrive.
                // Return normally so the copy is deleted.
                log.info("Duplicate suppressed for notificationId={} (receiveCount={})", id, receiveCount);
                record(event, DeliveryOutcome.DUPLICATE);
            }
            case IN_PROGRESS -> {
                log.info("notificationId={} is being delivered by another worker, retrying later", id);
                record(event, DeliveryOutcome.IN_PROGRESS);
                changeVisibility(visibility, (int) properties.inProgressRetryDelay().toSeconds());
                throw new DeliveryFailedException("Delivery already in progress for " + id);
            }
            case ACQUIRED -> deliver(event, acquisition.leaseToken(), receiveCount, visibility);
        }
    }

    private void deliver(NotificationEvent event, String leaseToken, int receiveCount, Visibility visibility) {
        String id = event.notificationId();
        try {
            senderRegistry.get(event.channel()).send(event);
        } catch (RuntimeException ex) {
            // Give up the lease so the next attempt (possibly on another worker) can proceed.
            idempotencyGuard.release(id, leaseToken);

            if (receiveCount >= properties.maxReceiveCount()) {
                statusStore.transition(id, NotificationStatus.DEAD_LETTERED, ex.getMessage());
                record(event, DeliveryOutcome.DEAD_LETTERED);
                changeVisibility(visibility, 0);
                log.error("notificationId={} failed final attempt {}/{}, moving to DLQ: {}",
                        id, receiveCount, properties.maxReceiveCount(), ex.getMessage());
            } else {
                int delay = backoffPolicy.delaySeconds(receiveCount);
                statusStore.transition(id, NotificationStatus.RETRYING, ex.getMessage());
                record(event, DeliveryOutcome.RETRY);
                changeVisibility(visibility, delay);
                log.warn("notificationId={} attempt {}/{} failed, retrying in {}s: {}",
                        id, receiveCount, properties.maxReceiveCount(), delay, ex.getMessage());
            }
            throw new DeliveryFailedException("Delivery failed for " + id, ex);
        }

        idempotencyGuard.markDelivered(id);
        statusStore.transition(id, NotificationStatus.SENT, null);
        record(event, DeliveryOutcome.SENT);
    }

    private void changeVisibility(Visibility visibility, int seconds) {
        try {
            visibility.changeTo(seconds);
        } catch (RuntimeException ex) {
            // Not fatal: the queue's default visibility timeout still applies.
            log.warn("Could not change message visibility to {}s: {}", seconds, ex.getMessage());
        }
    }

    private void record(NotificationEvent event, DeliveryOutcome outcome) {
        record(event.channel() == null ? "unknown" : event.channel().name(), outcome);
    }

    private void record(String channel, DeliveryOutcome outcome) {
        meterRegistry.counter(METRIC_NAME, "channel", channel, "outcome", outcome.tag()).increment();
    }

    private static int receiveCount(Message<?> message) {
        Object raw = message.getHeaders().get(RECEIVE_COUNT_HEADER);
        if (raw == null) {
            return 1;
        }
        try {
            return Integer.parseInt(raw.toString());
        } catch (NumberFormatException ex) {
            return 1;
        }
    }
}
