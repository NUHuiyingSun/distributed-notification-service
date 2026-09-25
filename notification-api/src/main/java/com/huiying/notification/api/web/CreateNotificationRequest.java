package com.huiying.notification.api.web;

import com.huiying.notification.common.Channel;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record CreateNotificationRequest(
        @NotBlank @Size(max = 64) String userId,
        @NotNull Channel channel,
        @Size(max = 200) String subject,
        @NotBlank @Size(max = 4000) String body) {
}
