package com.huiying.notification.dispatcher.sns;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.common.NotificationEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.util.Map;

/**
 * Publishes to a single SNS topic. The "channel" message attribute is matched by each SQS
 * subscription's filter policy, so adding a new channel = new queue + subscription, no code change here.
 */
@Component
public class SnsPublisher {

    private final SnsClient snsClient;
    private final ObjectMapper objectMapper;
    private final String topicArn;

    public SnsPublisher(SnsClient snsClient, ObjectMapper objectMapper,
                        @Value("${app.sns.topic-arn}") String topicArn) {
        this.snsClient = snsClient;
        this.objectMapper = objectMapper;
        this.topicArn = topicArn;
    }

    public String publish(NotificationEvent event) throws JsonProcessingException {
        PublishRequest request = PublishRequest.builder()
                .topicArn(topicArn)
                .message(objectMapper.writeValueAsString(event))
                .messageAttributes(Map.of(
                        "channel", stringAttribute(event.channel().name()),
                        "notificationId", stringAttribute(event.notificationId())))
                .build();
        return snsClient.publish(request).messageId();
    }

    private static MessageAttributeValue stringAttribute(String value) {
        return MessageAttributeValue.builder().dataType("String").stringValue(value).build();
    }
}
