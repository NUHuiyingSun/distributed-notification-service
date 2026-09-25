package com.huiying.notification.common;

import java.time.Instant;

/**
 * The event that flows through the whole pipeline:
 * API -> Kafka -> Dispatcher -> SNS -> SQS -> Worker.
 *
 * notificationId is generated once by the API and is the idempotency key for delivery.
 */
public record NotificationEvent(
        String notificationId,
        String userId,
        Channel channel,
        String subject,
        String body,
        Instant createdAt) {
}
