package com.huiying.notification.worker.idempotency;

import com.huiying.notification.common.RedisKeys;
import com.huiying.notification.worker.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdempotencyGuardTest {

    private static final String ID = "n-1";
    private static final String KEY = RedisKeys.deliveryLease(ID);

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> ops = mock(ValueOperations.class);
    private IdempotencyGuard guard;

    @BeforeEach
    void setUp() {
        when(redis.opsForValue()).thenReturn(ops);
        guard = new IdempotencyGuard(redis, TestFixtures.properties());
    }

    @Test
    void firstWorkerAcquiresLeaseWithToken() {
        when(ops.setIfAbsent(eq(KEY), anyString(), any(Duration.class))).thenReturn(true);

        IdempotencyGuard.Acquisition result = guard.tryAcquire(ID);

        assertThat(result.outcome()).isEqualTo(IdempotencyGuard.Outcome.ACQUIRED);
        assertThat(result.leaseToken()).startsWith(IdempotencyGuard.LEASE_PREFIX);
    }

    @Test
    void alreadyDeliveredMessageIsReportedAsDuplicate() {
        when(ops.setIfAbsent(eq(KEY), anyString(), any(Duration.class))).thenReturn(false);
        when(ops.get(KEY)).thenReturn(IdempotencyGuard.DELIVERED);

        assertThat(guard.tryAcquire(ID).outcome()).isEqualTo(IdempotencyGuard.Outcome.ALREADY_DELIVERED);
    }

    @Test
    void concurrentLeaseHolderMeansInProgress() {
        when(ops.setIfAbsent(eq(KEY), anyString(), any(Duration.class))).thenReturn(false);
        when(ops.get(KEY)).thenReturn(IdempotencyGuard.LEASE_PREFIX + "other-worker");

        assertThat(guard.tryAcquire(ID).outcome()).isEqualTo(IdempotencyGuard.Outcome.IN_PROGRESS);
    }

    @Test
    void retriesAcquireWhenLeaseExpiresBetweenSetAndGet() {
        when(ops.setIfAbsent(eq(KEY), anyString(), any(Duration.class))).thenReturn(false, true);
        when(ops.get(KEY)).thenReturn(null);

        assertThat(guard.tryAcquire(ID).outcome()).isEqualTo(IdempotencyGuard.Outcome.ACQUIRED);
    }

    @Test
    void markDeliveredUsesLongTtl() {
        guard.markDelivered(ID);
        verify(ops).set(KEY, IdempotencyGuard.DELIVERED, Duration.ofDays(7));
    }

    @Test
    @SuppressWarnings("unchecked")
    void releaseIsCompareAndDelete() {
        when(redis.execute(any(RedisScript.class), eq(List.of(KEY)), eq("LEASE:abc"))).thenReturn(1L);

        assertThat(guard.release(ID, "LEASE:abc")).isTrue();
    }
}
