package com.huiying.notification.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.api.web.CreateNotificationRequest;
import com.huiying.notification.common.NotificationEvent;
import com.huiying.notification.common.NotificationStatusStore;
import com.huiying.notification.common.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);
    static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);
    private static final long PUBLISH_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StringRedisTemplate redis;
    private final NotificationStatusStore statusStore;
    private final ObjectMapper objectMapper;
    private final String topic;

    public NotificationService(KafkaTemplate<String, String> kafkaTemplate,
                               StringRedisTemplate redis,
                               NotificationStatusStore statusStore,
                               ObjectMapper objectMapper,
                               @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.redis = redis;
        this.statusStore = statusStore;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public CreateResult create(CreateNotificationRequest request, String idempotencyKey) {
        String notificationId = UUID.randomUUID().toString();

        // Layer 1 idempotency: a client retrying the same request (e.g. after a timeout)
        // gets the original notificationId back instead of creating a second notification.
        String idempotencyRedisKey = null;
        if (StringUtils.hasText(idempotencyKey)) {
            idempotencyRedisKey = RedisKeys.apiIdempotency(idempotencyKey);
            Boolean firstSeen = redis.opsForValue().setIfAbsent(idempotencyRedisKey, notificationId, IDEMPOTENCY_TTL);
            if (!Boolean.TRUE.equals(firstSeen)) {
                String existingId = redis.opsForValue().get(idempotencyRedisKey);
                if (existingId == null) {
                    throw new IdempotencyConflictException("Idempotency-Key state changed concurrently, please retry");
                }
                log.info("Idempotent replay for key={} -> notificationId={}", idempotencyKey, existingId);
                return new CreateResult(existingId, false);
            }
        }

        NotificationEvent event = new NotificationEvent(notificationId, request.userId(), request.channel(),
                request.subject(), request.body(), Instant.now());
        statusStore.initialize(event);

        try {
            // Keyed by userId: all notifications for a user land on the same partition,
            // which preserves per-user ordering through the dispatcher.
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, event.userId(), payload).get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // Roll back so a client retry with the same key is treated as a fresh request.
            statusStore.delete(notificationId);
            if (idempotencyRedisKey != null) {
                redis.delete(idempotencyRedisKey);
            }
            throw new PublishFailedException("Failed to publish notification " + notificationId, ex);
        }

        log.info("Accepted notificationId={} userId={} channel={}", notificationId, event.userId(), event.channel());
        return new CreateResult(notificationId, true);
    }
}
