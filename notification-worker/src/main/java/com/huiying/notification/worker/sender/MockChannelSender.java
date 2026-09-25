package com.huiying.notification.worker.sender;

import com.huiying.notification.common.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stand-in for a real provider (SES / Twilio / FCM). A real implementation should pass
 * notificationId as the provider-side idempotency key where supported, which closes the
 * small gap between "provider accepted" and "we recorded DELIVERED".
 */
public abstract class MockChannelSender implements ChannelSender {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final FailureSimulator failureSimulator;

    protected MockChannelSender(FailureSimulator failureSimulator) {
        this.failureSimulator = failureSimulator;
    }

    @Override
    public void send(NotificationEvent event) {
        failureSimulator.maybeFail(event);
        log.info("[{}] delivered notificationId={} to userId={} subject='{}'",
                channel(), event.notificationId(), event.userId(), event.subject());
    }
}
