package com.huiying.notification.common;

import java.util.EnumSet;
import java.util.Set;

/**
 * Notification lifecycle. Transitions are enforced atomically in Redis (see NotificationStatusStore),
 * so a late "DISPATCHED" update can never overwrite an earlier "SENT".
 *
 * ACCEPTED -> DISPATCHED -> SENT
 *                 |  ^
 *                 v  |
 *              RETRYING -> DEAD_LETTERED -> REDRIVEN -> (SENT | RETRYING | DEAD_LETTERED)
 */
public enum NotificationStatus {
    ACCEPTED,
    DISPATCHED,
    RETRYING,
    SENT,
    DEAD_LETTERED,
    REDRIVEN;

    /** States from which a transition into this state is allowed. */
    public Set<NotificationStatus> allowedPredecessors() {
        return switch (this) {
            case ACCEPTED -> EnumSet.noneOf(NotificationStatus.class);
            case DISPATCHED -> EnumSet.of(ACCEPTED);
            case RETRYING, SENT, DEAD_LETTERED -> EnumSet.of(ACCEPTED, DISPATCHED, RETRYING, REDRIVEN);
            case REDRIVEN -> EnumSet.of(DEAD_LETTERED);
        };
    }
}
