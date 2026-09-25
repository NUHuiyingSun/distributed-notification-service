package com.huiying.notification.api.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic notificationRequestedTopic(@Value("${app.kafka.topic}") String topic,
                                               @Value("${app.kafka.partitions:6}") int partitions) {
        return TopicBuilder.name(topic).partitions(partitions).replicas(1).build();
    }

    /** DLT must have at least as many partitions as the source topic (same-partition routing). */
    @Bean
    public NewTopic notificationRequestedDeadLetterTopic(@Value("${app.kafka.topic}") String topic,
                                                         @Value("${app.kafka.partitions:6}") int partitions) {
        return TopicBuilder.name(topic + ".DLT").partitions(partitions).replicas(1).build();
    }
}
