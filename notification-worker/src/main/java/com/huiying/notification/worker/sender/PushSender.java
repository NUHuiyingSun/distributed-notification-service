package com.huiying.notification.worker.sender;

import com.huiying.notification.common.Channel;
import org.springframework.stereotype.Component;

@Component
public class PushSender extends MockChannelSender {

    public PushSender(FailureSimulator failureSimulator) {
        super(failureSimulator);
    }

    @Override
    public Channel channel() {
        return Channel.PUSH;
    }
}
