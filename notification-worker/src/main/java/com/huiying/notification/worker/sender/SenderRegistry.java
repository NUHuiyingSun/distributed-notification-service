package com.huiying.notification.worker.sender;

import com.huiying.notification.common.Channel;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class SenderRegistry {

    private final Map<Channel, ChannelSender> senders = new EnumMap<>(Channel.class);

    public SenderRegistry(List<ChannelSender> senderBeans) {
        for (ChannelSender sender : senderBeans) {
            if (senders.put(sender.channel(), sender) != null) {
                throw new IllegalStateException("Duplicate sender for channel " + sender.channel());
            }
        }
    }

    public ChannelSender get(Channel channel) {
        ChannelSender sender = senders.get(channel);
        if (sender == null) {
            throw new IllegalArgumentException("No sender registered for channel " + channel);
        }
        return sender;
    }
}
