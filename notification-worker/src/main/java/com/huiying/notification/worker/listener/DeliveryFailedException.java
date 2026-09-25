package com.huiying.notification.worker.listener;

/** Thrown to leave the SQS message un-acknowledged so it is redelivered (or moved to the DLQ). */
public class DeliveryFailedException extends RuntimeException {
    public DeliveryFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeliveryFailedException(String message) {
        super(message);
    }
}
