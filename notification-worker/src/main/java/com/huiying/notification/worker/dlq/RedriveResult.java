package com.huiying.notification.worker.dlq;

import com.huiying.notification.common.Channel;

import java.util.List;

public record RedriveResult(Channel channel, int moved, List<String> notificationIds) {
}
