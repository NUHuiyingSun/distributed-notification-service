package com.huiying.notification.worker.sender;

import com.huiying.notification.common.Channel;
import com.huiying.notification.common.NotificationEvent;

/** A delivery provider for one channel. Throwing any RuntimeException signals a failed attempt. */
public interface ChannelSender {

    Channel channel();

    void send(NotificationEvent event);
}
