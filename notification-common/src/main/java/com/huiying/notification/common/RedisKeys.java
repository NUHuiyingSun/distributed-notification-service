package com.huiying.notification.common;

/** Central place for Redis key naming so every service agrees on the layout. */
public final class RedisKeys {

    private RedisKeys() {
    }

    /** Hash holding the notification's current status and metadata. */
    public static String status(String notificationId) {
        return "notif:status:" + notificationId;
    }

    /** API-level idempotency: client Idempotency-Key -> notificationId. */
    public static String apiIdempotency(String idempotencyKey) {
        return "notif:api-idem:" + idempotencyKey;
    }

    /** Delivery-level idempotency: lease token while processing, "DELIVERED" once sent. */
    public static String deliveryLease(String notificationId) {
        return "notif:delivery:" + notificationId;
    }
}
