package com.huiying.notification.dispatcher.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.common.Channel;
import com.huiying.notification.common.NotificationEvent;
import com.huiying.notification.common.NotificationStatus;
import com.huiying.notification.common.NotificationStatusStore;
import com.huiying.notification.dispatcher.sns.SnsPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationRequestedConsumerTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final SnsPublisher snsPublisher = mock(SnsPublisher.class);
    private final NotificationStatusStore statusStore = mock(NotificationStatusStore.class);
    private final NotificationRequestedConsumer consumer =
            new NotificationRequestedConsumer(mapper, snsPublisher, statusStore);

    @Test
    void publishesToSnsThenMarksDispatched() throws Exception {
        NotificationEvent event = new NotificationEvent("n-1", "u-1", Channel.SMS, null, "code 1234", Instant.now());
        when(snsPublisher.publish(any())).thenReturn("sns-msg-1");

        consumer.onMessage(new ConsumerRecord<>("notification.requested", 0, 0L, "u-1", mapper.writeValueAsString(event)));

        verify(snsPublisher).publish(event);
        verify(statusStore).transition("n-1", NotificationStatus.DISPATCHED, null);
    }

    @Test
    void malformedPayloadIsRejectedWithoutPublishing() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("notification.requested", 0, 0L, "u-1", "{not json");

        assertThatThrownBy(() -> consumer.onMessage(record)).isInstanceOf(JsonProcessingException.class);
        verify(snsPublisher, never()).publish(any());
    }
}
