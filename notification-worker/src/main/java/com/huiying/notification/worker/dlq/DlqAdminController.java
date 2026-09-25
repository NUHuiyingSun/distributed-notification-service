package com.huiying.notification.worker.dlq;

import com.huiying.notification.common.Channel;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Operator endpoints. In production these would sit behind authentication. */
@RestController
@RequestMapping("/admin/dlq")
public class DlqAdminController {

    private static final int MAX_REDRIVE_PER_CALL = 100;

    private final DlqRedriveService redriveService;

    public DlqAdminController(DlqRedriveService redriveService) {
        this.redriveService = redriveService;
    }

    @GetMapping("/{channel}")
    public Map<String, Object> stats(@PathVariable Channel channel) {
        return redriveService.stats(channel);
    }

    @PostMapping("/{channel}/redrive")
    public RedriveResult redrive(@PathVariable Channel channel,
                                 @RequestParam(defaultValue = "10") int max) {
        int bounded = Math.max(1, Math.min(max, MAX_REDRIVE_PER_CALL));
        return redriveService.redrive(channel, bounded);
    }
}
