package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import lombok.AllArgsConstructor;

import java.time.Duration;

/**
 * A configuration for the logging meter registry that configures the interval
 * between each logging event.
 */
@AllArgsConstructor
public class IntervalLoggingConfig implements LoggingRegistryConfig {

    private final Duration intervalBetweenLogs;

    @Override
    public Duration step() {
        return intervalBetweenLogs;
    }

    @Override
    public String get(String key) {
        return null;
    }
}
