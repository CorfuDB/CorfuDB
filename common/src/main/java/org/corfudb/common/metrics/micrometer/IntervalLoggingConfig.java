package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import lombok.AllArgsConstructor;

import java.time.Duration;

@AllArgsConstructor
public class IntervalLoggingConfig implements LoggingRegistryConfig {

    private final Duration intervalBetweenLogs;

    @Override
    public String get(String key) {
        throw new IllegalArgumentException("Getting config values is disabled.");
    }

    @Override
    public Duration step() {
        return intervalBetweenLogs;
    }
}
