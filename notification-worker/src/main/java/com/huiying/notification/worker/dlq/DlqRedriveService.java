package com.huiying.notification.worker.dlq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huiying.notification.common.Channel;
import com.huiying.notification.common.NotificationStatus;
import com.huiying.notification.common.NotificationStatusStore;
import com.huiying.notification.worker.config.WorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Failure-recovery workflow: once the root cause is fixed (provider back up, bad config corrected),
 * an operator moves dead-lettered messages back to the source queue. Redriven messages get a fresh
 * receive count, and the idempotency guard still prevents double delivery if one was actually sent.
 */
@Service
public class DlqRedriveService {

    private static final Logger log = LoggerFactory.getLogger(DlqRedriveService.class);
    private static final int SQS_MAX_BATCH = 10;

    private final SqsAsyncClient sqs;
    private final WorkerProperties properties;
    private final ObjectMapper objectMapper;
    private final NotificationStatusStore statusStore;

    public DlqRedriveService(SqsAsyncClient sqs, WorkerProperties properties,
                             ObjectMapper objectMapper, NotificationStatusStore statusStore) {
        this.sqs = sqs;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.statusStore = statusStore;
    }

    public Map<String, Object> stats(Channel channel) {
        String dlqName = properties.deadLetterQueues().get(channel);
        Map<QueueAttributeName, String> attributes = sqs.getQueueAttributes(r -> r
                        .queueUrl(queueUrl(dlqName))
                        .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
                .join()
                .attributes();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("channel", channel);
        result.put("deadLetterQueue", dlqName);
        result.put("approximateMessages", attributes.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0"));
        result.put("inFlight", attributes.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0"));
        return result;
    }

    public RedriveResult redrive(Channel channel, int maxMessages) {
        String dlqUrl = queueUrl(properties.deadLetterQueues().get(channel));
        String sourceUrl = queueUrl(properties.queues().get(channel));

        int moved = 0;
        List<String> ids = new ArrayList<>();
        while (moved < maxMessages) {
            int batchSize = Math.min(SQS_MAX_BATCH, maxMessages - moved);
            List<Message> messages = sqs.receiveMessage(r -> r
                            .queueUrl(dlqUrl)
                            .maxNumberOfMessages(batchSize)
                            .waitTimeSeconds(1)
                            .messageAttributeNames("All"))
                    .join()
                    .messages();
            if (messages.isEmpty()) {
                break;
            }
            for (Message message : messages) {
                // Send first, then delete: a crash in between yields a duplicate (handled by idempotency), never a loss.
                sqs.sendMessage(r -> r
                        .queueUrl(sourceUrl)
                        .messageBody(message.body())
                        .messageAttributes(message.messageAttributes())).join();
                sqs.deleteMessage(r -> r.queueUrl(dlqUrl).receiptHandle(message.receiptHandle())).join();

                extractNotificationId(message.body()).ifPresent(id -> {
                    statusStore.transition(id, NotificationStatus.REDRIVEN, null);
                    ids.add(id);
                });
                moved++;
            }
        }
        log.info("Redrove {} message(s) from {} DLQ back to source queue", moved, channel);
        return new RedriveResult(channel, moved, ids);
    }

    private String queueUrl(String queueName) {
        if (queueName == null) {
            throw new IllegalArgumentException("No queue configured");
        }
        return sqs.getQueueUrl(r -> r.queueName(queueName)).join().queueUrl();
    }

    private Optional<String> extractNotificationId(String body) {
        try {
            JsonNode node = objectMapper.readTree(body).get("notificationId");
            return node == null || node.isNull() ? Optional.empty() : Optional.of(node.asText());
        } catch (Exception ex) {
            return Optional.empty();
        }
    }
}
