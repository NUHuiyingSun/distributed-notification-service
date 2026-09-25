package com.huiying.notification.worker.listener;

/** Tag values for the {@code notification.delivery} counter. */
public enum DeliveryOutcome {
    /** Delivered by this attempt. */
    SENT,
    /** Already delivered earlier; this copy was suppressed. */
    DUPLICATE,
    /** Another worker holds the lease; retried later. */
    IN_PROGRESS,
    /** Failed, will be retried after backoff. */
    RETRY,
    /** Failed on the final attempt; SQS moves it to the DLQ. */
    DEAD_LETTERED,
    /** Payload could not be parsed. */
    MALFORMED;

    public String tag() {
        return name().toLowerCase();
    }
}
