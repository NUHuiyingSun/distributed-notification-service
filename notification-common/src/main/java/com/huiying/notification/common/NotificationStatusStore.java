package com.huiying.notification.common;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Redis-backed status tracking shared by all services.
 *
 * Status changes go through a Lua script that performs compare-and-set against the allowed
 * predecessor states. Because several services update the same record concurrently
 * (dispatcher and worker can race), a plain HSET could move a notification backwards.
 */
@Component
public class NotificationStatusStore {

    static final Duration TTL = Duration.ofDays(7);
    private static final int MAX_ERROR_LENGTH = 500;

    private static final RedisScript<Long> TRANSITION_SCRIPT = new DefaultRedisScript<>("""
            local current = redis.call('HGET', KEYS[1], 'status')
            if not current then
              return -1
            end
            for i = 4, #ARGV do
              if ARGV[i] == current then
                redis.call('HSET', KEYS[1], 'status', ARGV[1], 'updatedAt', ARGV[2])
                if ARGV[3] ~= '' then
                  redis.call('HSET', KEYS[1], 'lastError', ARGV[3])
                end
                return 1
              end
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;

    public NotificationStatusStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public void initialize(NotificationEvent event) {
        String key = RedisKeys.status(event.notificationId());
        Map<String, String> fields = new HashMap<>();
        fields.put("notificationId", event.notificationId());
        fields.put("userId", event.userId());
        fields.put("channel", event.channel().name());
        fields.put("status", NotificationStatus.ACCEPTED.name());
        fields.put("attempts", "0");
        fields.put("createdAt", event.createdAt().toString());
        fields.put("updatedAt", Instant.now().toString());
        redis.opsForHash().putAll(key, fields);
        redis.expire(key, TTL);
    }

    public TransitionResult transition(String notificationId, NotificationStatus target, String error) {
        List<String> args = new ArrayList<>();
        args.add(target.name());
        args.add(Instant.now().toString());
        args.add(truncate(error));
        target.allowedPredecessors().forEach(s -> args.add(s.name()));

        Long result = redis.execute(TRANSITION_SCRIPT, List.of(RedisKeys.status(notificationId)), args.toArray());
        if (result == null || result == 0L) {
            return TransitionResult.REJECTED;
        }
        return result == 1L ? TransitionResult.APPLIED : TransitionResult.NOT_FOUND;
    }

    public void recordAttempt(String notificationId, int attempt) {
        redis.opsForHash().put(RedisKeys.status(notificationId), "attempts", String.valueOf(attempt));
    }

    public Optional<Map<String, String>> find(String notificationId) {
        Map<Object, Object> raw = redis.opsForHash().entries(RedisKeys.status(notificationId));
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        Map<String, String> result = new TreeMap<>();
        raw.forEach((k, v) -> result.put(String.valueOf(k), String.valueOf(v)));
        return Optional.of(result);
    }

    public void delete(String notificationId) {
        redis.delete(RedisKeys.status(notificationId));
    }

    private static String truncate(String error) {
        if (error == null) {
            return "";
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
