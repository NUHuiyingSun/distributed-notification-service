package com.huiying.notification.worker;

import com.huiying.notification.worker.config.WorkerProperties;

import java.time.Duration;
import java.util.Map;

public final class TestFixtures {

    private TestFixtures() {
    }

    public static WorkerProperties properties() {
        return new WorkerProperties(3, Duration.ofSeconds(2), Duration.ofSeconds(60), Duration.ofSeconds(60),
                Duration.ofDays(7), Duration.ofSeconds(5), 0.0, Map.of(), Map.of());
    }
}
