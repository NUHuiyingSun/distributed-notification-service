package com.huiying.notification.worker.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.common.Channel;
import com.huiying.notification.common.NotificationEvent;
import com.huiying.notification.common.NotificationStatus;
import com.huiying.notification.common.NotificationStatusStore;
import com.huiying.notification.worker.TestFixtures;
import com.huiying.notification.worker.idempotency.IdempotencyGuard;
import com.huiying.notification.worker.idempotency.IdempotencyGuard.Acquisition;
import com.huiying.notification.worker.idempotency.IdempotencyGuard.Outcome;
import com.huiying.notification.worker.retry.BackoffPolicy;
import com.huiying.notification.worker.sender.ChannelSender;
import com.huiying.notification.worker.sender.DeliveryException;
import com.huiying.notification.worker.sender.SenderRegistry;
import io.awspring.cloud.sqs.listener.Visibility;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationQueueListenerTest {

    private static final String ID = "n-42";

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final IdempotencyGuard guard = mock(IdempotencyGuard.class);
    private final SenderRegistry registry = mock(SenderRegistry.class);
    private final ChannelSender sender = mock(ChannelSender.class);
    private final NotificationStatusStore statusStore = mock(NotificationStatusStore.class);
    private final BackoffPolicy backoff = mock(BackoffPolicy.class);
    private final Visibility visibility = mock(Visibility.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    private NotificationQueueListener listener;
    private NotificationEvent event;
    private String payload;

    @BeforeEach
    void setUp() throws Exception {
        listener = new NotificationQueueListener(mapper, guard, registry, statusStore, backoff, TestFixtures.properties(), meters);
        event = new NotificationEvent(ID, "u-1", Channel.EMAIL, "Hi", "Hello", Instant.now());
        payload = mapper.writeValueAsString(event);
        when(registry.get(Channel.EMAIL)).thenReturn(sender);
    }

    @Test
    void successfulDeliveryMarksDeliveredAndSent() {
        when(guard.tryAcquire(ID)).thenReturn(new Acquisition(Outcome.ACQUIRED, "LEASE:t1"));

        listener.handle(payload, 1, visibility);

        verify(sender).send(event);
        verify(guard).markDelivered(ID);
        verify(statusStore).transition(ID, NotificationStatus.SENT, null);
        assertThat(count("EMAIL", DeliveryOutcome.SENT)).isEqualTo(1.0);
    }

    @Test
    void duplicateMessageIsAcknowledgedWithoutSending() {
        when(guard.tryAcquire(ID)).thenReturn(new Acquisition(Outcome.ALREADY_DELIVERED, null));

        listener.handle(payload, 2, visibility);   // returns normally -> SQS deletes the message

        verify(sender, never()).send(any());
        verify(guard, never()).markDelivered(anyString());
        assertThat(count("EMAIL", DeliveryOutcome.DUPLICATE)).isEqualTo(1.0);
    }

    @Test
    void inProgressElsewhereIsRetriedLater() {
        when(guard.tryAcquire(ID)).thenReturn(new Acquisition(Outcome.IN_PROGRESS, null));

        assertThatThrownBy(() -> listener.handle(payload, 1, visibility)).isInstanceOf(DeliveryFailedException.class);

        verify(sender, never()).send(any());
        verify(visibility).changeTo(5);
    }

    @Test
    void transientFailureReleasesLeaseAndBacksOff() {
        when(guard.tryAcquire(ID)).thenReturn(new Acquisition(Outcome.ACQUIRED, "LEASE:t1"));
        doThrow(new DeliveryException("timeout")).when(sender).send(any());
        when(backoff.delaySeconds(1)).thenReturn(2);

        assertThatThrownBy(() -> listener.handle(payload, 1, visibility)).isInstanceOf(DeliveryFailedException.class);

        verify(guard).release(ID, "LEASE:t1");
        verify(guard, never()).markDelivered(anyString());
        verify(statusStore).transition(ID, NotificationStatus.RETRYING, "timeout");
        verify(visibility).changeTo(2);
        assertThat(count("EMAIL", DeliveryOutcome.RETRY)).isEqualTo(1.0);
    }

    @Test
    void finalAttemptFailureMarksDeadLettered() {
        when(guard.tryAcquire(ID)).thenReturn(new Acquisition(Outcome.ACQUIRED, "LEASE:t3"));
        doThrow(new DeliveryException("provider down")).when(sender).send(any());

        assertThatThrownBy(() -> listener.handle(payload, 3, visibility)).isInstanceOf(DeliveryFailedException.class);

        verify(statusStore).transition(ID, NotificationStatus.DEAD_LETTERED, "provider down");
        verify(visibility).changeTo(0);
        verify(backoff, never()).delaySeconds(anyInt());
        assertThat(count("EMAIL", DeliveryOutcome.DEAD_LETTERED)).isEqualTo(1.0);
    }

    @Test
    void malformedPayloadIsFastTrackedToDlq() {
        assertThatThrownBy(() -> listener.handle("{oops", 1, visibility)).isInstanceOf(DeliveryFailedException.class);

        verify(visibility).changeTo(0);
        verify(guard, never()).tryAcquire(anyString());
        assertThat(count("unknown", DeliveryOutcome.MALFORMED)).isEqualTo(1.0);
    }

    @Test
    void visibilityErrorsDoNotMaskDeliveryFailure() {
        when(guard.tryAcquire(ID)).thenReturn(new Acquisition(Outcome.ACQUIRED, "LEASE:t1"));
        doThrow(new DeliveryException("timeout")).when(sender).send(any());
        when(backoff.delaySeconds(1)).thenReturn(2);
        doThrow(new RuntimeException("sqs unavailable")).when(visibility).changeTo(eq(2));

        assertThatThrownBy(() -> listener.handle(payload, 1, visibility)).isInstanceOf(DeliveryFailedException.class);
        verify(guard).release(ID, "LEASE:t1");
    }

    private double count(String channel, DeliveryOutcome outcome) {
        return meters.counter(NotificationQueueListener.METRIC_NAME, "channel", channel, "outcome", outcome.tag()).count();
    }
}
