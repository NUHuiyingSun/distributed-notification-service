package com.huiying.notification.worker.retry;

import org.junit.jupiter.api.RepeatedTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffPolicyTest {

    private final BackoffPolicy policy = new BackoffPolicy(Duration.ofSeconds(2), Duration.ofSeconds(60));

    @RepeatedTest(20)
    void growsExponentiallyWithBoundedJitter() {
        assertThat(policy.delaySeconds(1)).isEqualTo(2);        // 2s + <=0.4s jitter
        assertThat(policy.delaySeconds(2)).isEqualTo(4);         // 4s + <=0.8s jitter
        assertThat(policy.delaySeconds(3)).isBetween(8, 9);     // 8s + <=1.6s jitter
    }

    @RepeatedTest(5)
    void isCappedAtMax() {
        assertThat(policy.delaySeconds(10)).isEqualTo(60);
        assertThat(policy.delaySeconds(1000)).isEqualTo(60);
    }
}
