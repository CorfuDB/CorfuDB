package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;

import java.util.Optional;

/**
 * Provides JVM level metrics.
 */

public final class JVMMetrics {

    private static void subscribeThreadMetrics(MeterRegistry meterRegistry) {
        JvmThreadMetrics threadMetrics = new JvmThreadMetrics();
        threadMetrics.bindTo(meterRegistry);
    }

    private static void subscribeMemoryMetrics(MeterRegistry meterRegistry) {
        JvmMemoryMetrics memoryMetrics = new JvmMemoryMetrics();
        memoryMetrics.bindTo(meterRegistry);
    }

    public static void register(Optional<MeterRegistry> metricsRegistry) {

        if (metricsRegistry.isPresent()) {
            final MeterRegistry meterRegistry = metricsRegistry.get();
            subscribeMemoryMetrics(meterRegistry);
            subscribeThreadMetrics(meterRegistry);
        }
    }
}

