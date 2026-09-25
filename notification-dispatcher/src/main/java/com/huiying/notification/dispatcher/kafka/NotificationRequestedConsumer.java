package com.huiying.notification.dispatcher.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.common.NotificationEvent;
import com.huiying.notification.common.NotificationStatus;
import com.huiying.notification.common.NotificationStatusStore;
import com.huiying.notification.dispatcher.sns.SnsPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka is the durable ingestion log; SNS handles fan-out to per-channel SQS queues.
 *
 * Delivery here is at-least-once: if the process crashes after publishing to SNS but before the
 * offset commit, the event is published again. That is safe because the worker deduplicates on
 * notificationId (see IdempotencyGuard) — deduplication happens at the sink, not at every hop.
 */
@Component
public class NotificationRequestedConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationRequestedConsumer.class);

    private final ObjectMapper objectMapper;
    private final SnsPublisher snsPublisher;
    private final NotificationStatusStore statusStore;

    public NotificationRequestedConsumer(ObjectMapper objectMapper, SnsPublisher snsPublisher,
                                         NotificationStatusStore statusStore) {
        this.objectMapper = objectMapper;
        this.snsPublisher = snsPublisher;
        this.statusStore = statusStore;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "notification-dispatcher",
            concurrency = "${app.kafka.concurrency:3}")
    public void onMessage(ConsumerRecord<String, String> record) throws JsonProcessingException {
        NotificationEvent event = objectMapper.readValue(record.value(), NotificationEvent.class);
        if (event.notificationId() == null || event.channel() == null) {
            throw new IllegalArgumentException("Event missing notificationId or channel at offset " + record.offset());
        }

        String snsMessageId = snsPublisher.publish(event);
        statusStore.transition(event.notificationId(), NotificationStatus.DISPATCHED, null);

        log.info("Dispatched notificationId={} channel={} partition={} offset={} snsMessageId={}",
                event.notificationId(), event.channel(), record.partition(), record.offset(), snsMessageId);
    }
}
