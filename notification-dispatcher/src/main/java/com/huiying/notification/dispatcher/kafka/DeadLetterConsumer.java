package com.huiying.notification.dispatcher.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.common.NotificationEvent;
import com.huiying.notification.common.NotificationStatus;
import com.huiying.notification.common.NotificationStatusStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/** Makes dispatch-stage failures visible: marks the notification DEAD_LETTERED with the root error. */
@Component
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    private final ObjectMapper objectMapper;
    private final NotificationStatusStore statusStore;

    public DeadLetterConsumer(ObjectMapper objectMapper, NotificationStatusStore statusStore) {
        this.objectMapper = objectMapper;
        this.statusStore = statusStore;
    }

    @KafkaListener(topics = "${app.kafka.topic}.DLT", groupId = "notification-dispatcher-dlt")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        String error = headerAsString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        try {
            NotificationEvent event = objectMapper.readValue(record.value(), NotificationEvent.class);
            statusStore.transition(event.notificationId(), NotificationStatus.DEAD_LETTERED,
                    "Dispatch failed: " + error);
            log.error("Notification {} dead-lettered at dispatch stage: {}", event.notificationId(), error);
        } catch (JsonProcessingException | RuntimeException ex) {
            log.error("Unparseable record in DLT partition={} offset={} error={}",
                    record.partition(), record.offset(), error);
        }
    }

    private static String headerAsString(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "unknown" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
