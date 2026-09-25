package com.huiying.notification.api.service;

/** created=false means the request was a replay of an earlier Idempotency-Key. */
public record CreateResult(String notificationId, boolean created) {
}
