package com.huiying.notification.common;

public enum TransitionResult {
    /** Status was updated. */
    APPLIED,
    /** Current status does not allow this transition (e.g. already SENT). */
    REJECTED,
    /** No status record exists (expired or never created). */
    NOT_FOUND
}
