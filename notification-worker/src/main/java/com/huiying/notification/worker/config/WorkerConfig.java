package com.huiying.notification.worker.config;

import com.huiying.notification.worker.retry.BackoffPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkerConfig {

    @Bean
    public BackoffPolicy backoffPolicy(WorkerProperties properties) {
        return new BackoffPolicy(properties.backoffBase(), properties.backoffMax());
    }
}
