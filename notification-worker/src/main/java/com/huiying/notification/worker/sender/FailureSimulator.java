package com.huiying.notification.worker.sender;

import com.huiying.notification.common.NotificationEvent;
import com.huiying.notification.worker.config.WorkerProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Fault injection for demos and resilience testing.
 * - Random transient failures at the configured rate -> exercises retry + backoff.
 * - Bodies containing [FAIL_ALWAYS] always fail -> exercises the DLQ path.
 */
@Component
public class FailureSimulator {

    public static final String ALWAYS_FAIL_MARKER = "[FAIL_ALWAYS]";

    private final double failureRate;

    public FailureSimulator(WorkerProperties properties) {
        this.failureRate = properties.simulatedFailureRate();
    }

    public void maybeFail(NotificationEvent event) {
        if (event.body() != null && event.body().contains(ALWAYS_FAIL_MARKER)) {
            throw new DeliveryException("Simulated permanent provider failure");
        }
        if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new DeliveryException("Simulated transient provider failure (timeout)");
        }
    }
}
