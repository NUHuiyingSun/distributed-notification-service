package com.huiying.notification.worker.sender;

import com.huiying.notification.common.Channel;
import org.springframework.stereotype.Component;

@Component
public class SmsSender extends MockChannelSender {

    public SmsSender(FailureSimulator failureSimulator) {
        super(failureSimulator);
    }

    @Override
    public Channel channel() {
        return Channel.SMS;
    }
}
