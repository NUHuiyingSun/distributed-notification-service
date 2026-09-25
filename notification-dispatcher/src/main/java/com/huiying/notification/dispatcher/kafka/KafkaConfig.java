package com.huiying.notification.dispatcher.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public NewTopic notificationRequestedTopic(@Value("${app.kafka.topic}") String topic,
                                               @Value("${app.kafka.partitions:6}") int partitions) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic notificationRequestedDeadLetterTopic(@Value("${app.kafka.topic}") String topic,
                                                         @Value("${app.kafka.partitions:6}") int partitions) {
        return TopicBuilder.name(topic + ".DLT").partitions(partitions).replicas(1).build();
    }

    /**
     * Retry transient failures (e.g. SNS throttling / network) with exponential backoff: 1s, 2s, 4s.
     * After retries are exhausted, the record is published to "<topic>.DLT" and the offset is committed,
     * so one bad record never blocks the partition forever.
     * Deserialization errors are not retryable and go straight to the DLT.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                 @Value("${app.kafka.retry.max-attempts:3}") int maxRetries,
                                                 @Value("${app.kafka.retry.initial-interval-ms:1000}") long initialInterval,
                                                 @Value("${app.kafka.retry.max-interval-ms:10000}") long maxInterval) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialInterval);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(maxInterval);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Dispatch attempt {} failed for key={} offset={}: {}",
                        attempt, record.key(), record.offset(), ex.getMessage()));
        return handler;
    }
}
