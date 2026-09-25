package com.huiying.notification.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.api.web.CreateNotificationRequest;
import com.huiying.notification.common.Channel;
import com.huiying.notification.common.NotificationEvent;
import com.huiying.notification.common.NotificationStatusStore;
import com.huiying.notification.common.RedisKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationServiceTest {

    private static final String TOPIC = "notification.requested";

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOps = mock(ValueOperations.class);
    private final NotificationStatusStore statusStore = mock(NotificationStatusStore.class);

    private NotificationService service;
    private final CreateNotificationRequest request =
            new CreateNotificationRequest("u-1", Channel.EMAIL, "Hi", "Hello there");

    @BeforeEach
    void setUp() {
        when(redis.opsForValue()).thenReturn(valueOps);
        service = new NotificationService(kafka, redis, statusStore, new ObjectMapper().findAndRegisterModules(), TOPIC);
    }

    @Test
    void newRequestIsRecordedAndPublishedKeyedByUser() {
        when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));

        CreateResult result = service.create(request, "key-1");

        assertThat(result.created()).isTrue();
        verify(statusStore).initialize(any(NotificationEvent.class));
        verify(kafka).send(eq(TOPIC), eq("u-1"), anyString());
    }

    @Test
    void replayedIdempotencyKeyReturnsOriginalIdWithoutPublishing() {
        when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false);
        when(valueOps.get(RedisKeys.apiIdempotency("key-1"))).thenReturn("original-id");

        CreateResult result = service.create(request, "key-1");

        assertThat(result.created()).isFalse();
        assertThat(result.notificationId()).isEqualTo("original-id");
        verify(kafka, never()).send(anyString(), anyString(), anyString());
        verify(statusStore, never()).initialize(any());
    }

    @Test
    void requestWithoutIdempotencyKeySkipsRedisCheck() {
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));

        CreateResult result = service.create(request, null);

        assertThat(result.created()).isTrue();
        verify(valueOps, never()).setIfAbsent(anyString(), anyString(), any(Duration.class));
    }

    @Test
    void publishFailureRollsBackStatusAndIdempotencyKey() {
        when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> service.create(request, "key-1")).isInstanceOf(PublishFailedException.class);

        verify(statusStore).delete(anyString());
        verify(redis).delete(RedisKeys.apiIdempotency("key-1"));
    }
}
