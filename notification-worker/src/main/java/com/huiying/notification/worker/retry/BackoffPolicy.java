package com.huiying.notification.worker.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with up to +20% jitter, capped at max.
 * Jitter spreads retries out so a provider outage does not produce synchronized retry storms.
 */
public class BackoffPolicy {

    /** SQS limit for a message visibility timeout. */
    private static final int SQS_MAX_VISIBILITY_SECONDS = 43_200;

    private final long baseMillis;
    private final long maxMillis;

    public BackoffPolicy(Duration base, Duration max) {
        this.baseMillis = base.toMillis();
        this.maxMillis = max.toMillis();
    }

    /** @param attempt 1-based attempt number that just failed */
    public int delaySeconds(int attempt) {
        int exponent = Math.max(0, Math.min(attempt - 1, 20));
        long exponential = Math.min(baseMillis * (1L << exponent), maxMillis);
        long jitter = ThreadLocalRandom.current().nextLong(exponential / 5 + 1);
        long total = Math.min(exponential + jitter, maxMillis);
        return (int) Math.max(1, Math.min(total / 1000, SQS_MAX_VISIBILITY_SECONDS));
    }
}
