package com.huiying.notification.worker.config;

import com.huiying.notification.common.Channel;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

/**
 * @param maxReceiveCount       must match the queue RedrivePolicy.maxReceiveCount
 * @param backoffBase           first retry delay; doubles per attempt
 * @param backoffMax            upper bound for retry delay
 * @param leaseTtl              how long a worker holds the delivery lease; must exceed worst-case send time
 * @param deliveredTtl          how long "DELIVERED" markers are kept for dedup; must exceed max redelivery window
 * @param inProgressRetryDelay  delay when another worker currently holds the lease
 * @param simulatedFailureRate  0.0-1.0, injects transient provider failures for demos
 */
@ConfigurationProperties(prefix = "app.worker")
public record WorkerProperties(
        int maxReceiveCount,
        Duration backoffBase,
        Duration backoffMax,
        Duration leaseTtl,
        Duration deliveredTtl,
        Duration inProgressRetryDelay,
        double simulatedFailureRate,
        Map<Channel, String> queues,
        Map<Channel, String> deadLetterQueues) {
}
