package com.huiying.notification.worker.idempotency;

import com.huiying.notification.common.RedisKeys;
import com.huiying.notification.worker.config.WorkerProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Delivery-level idempotency backed by Redis.
 *
 * Key notif:delivery:{id} moves through:
 *   (absent) --SET NX EX leaseTtl--> "LEASE:{token}" --success--> "DELIVERED" (long TTL)
 *                                                    --failure--> (absent, via compare-and-delete)
 *
 * - SET NX makes acquisition atomic: of N concurrent copies of the same message, only one sends.
 * - The lease TTL means a crashed worker never blocks the message forever.
 * - Release is compare-and-delete (Lua) with a per-attempt token, so a slow worker whose lease
 *   already expired cannot delete a lease that another worker now holds.
 */
@Component
public class IdempotencyGuard {

    public enum Outcome { ACQUIRED, ALREADY_DELIVERED, IN_PROGRESS }

    public record Acquisition(Outcome outcome, String leaseToken) {
    }

    static final String DELIVERED = "DELIVERED";
    static final String LEASE_PREFIX = "LEASE:";

    private static final RedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
            Long.class);

    private final StringRedisTemplate redis;
    private final WorkerProperties properties;

    public IdempotencyGuard(StringRedisTemplate redis, WorkerProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public Acquisition tryAcquire(String notificationId) {
        String key = RedisKeys.deliveryLease(notificationId);
        String token = LEASE_PREFIX + UUID.randomUUID();

        // Two rounds handle the rare case where the key expires between SET NX and GET.
        for (int round = 0; round < 2; round++) {
            Boolean acquired = redis.opsForValue().setIfAbsent(key, token, properties.leaseTtl());
            if (Boolean.TRUE.equals(acquired)) {
                return new Acquisition(Outcome.ACQUIRED, token);
            }
            String current = redis.opsForValue().get(key);
            if (DELIVERED.equals(current)) {
                return new Acquisition(Outcome.ALREADY_DELIVERED, null);
            }
            if (current != null) {
                return new Acquisition(Outcome.IN_PROGRESS, null);
            }
        }
        return new Acquisition(Outcome.IN_PROGRESS, null);
    }

    public void markDelivered(String notificationId) {
        redis.opsForValue().set(RedisKeys.deliveryLease(notificationId), DELIVERED, properties.deliveredTtl());
    }

    /** @return true if this worker's lease was released; false if it had already expired or been taken over */
    public boolean release(String notificationId, String leaseToken) {
        Long removed = redis.execute(RELEASE_SCRIPT, List.of(RedisKeys.deliveryLease(notificationId)), leaseToken);
        return removed != null && removed == 1L;
    }
}
