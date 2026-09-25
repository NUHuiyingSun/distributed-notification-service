package com.huiying.notification.api.web;

import com.huiying.notification.api.service.CreateResult;
import com.huiying.notification.api.service.NotificationService;
import com.huiying.notification.common.NotificationStatusStore;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/notifications")
public class NotificationController {

    static final int MAX_IDEMPOTENCY_KEY_LENGTH = 128;

    private final NotificationService notificationService;
    private final NotificationStatusStore statusStore;

    public NotificationController(NotificationService notificationService, NotificationStatusStore statusStore) {
        this.notificationService = notificationService;
        this.statusStore = statusStore;
    }

    /**
     * 202 Accepted for a new notification; 200 OK when the Idempotency-Key was already seen
     * (the original notificationId is returned and nothing is re-published).
     */
    @PostMapping
    public ResponseEntity<CreateNotificationResponse> create(
            @Valid @RequestBody CreateNotificationRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {

        if (idempotencyKey != null && idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new IllegalArgumentException("Idempotency-Key must be at most " + MAX_IDEMPOTENCY_KEY_LENGTH + " characters");
        }

        CreateResult result = notificationService.create(request, idempotencyKey);
        HttpStatus status = result.created() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status)
                .location(URI.create("/api/v1/notifications/" + result.notificationId()))
                .body(new CreateNotificationResponse(result.notificationId(),
                        result.created() ? "ACCEPTED" : "DUPLICATE_REQUEST"));
    }

    @GetMapping("/{notificationId}")
    public ResponseEntity<Map<String, String>> get(@PathVariable String notificationId) {
        return statusStore.find(notificationId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
