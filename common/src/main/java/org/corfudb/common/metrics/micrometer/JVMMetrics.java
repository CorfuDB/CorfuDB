package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import lombok.Getter;
import lombok.Data;

import java.util.Optional;

/**
 * Provides JVM level metrics.
 */

public final class JVMMetrics {

    @Getter(lazy=true)
    private final static sun.management.HotspotRuntimeMBean runtimeMBean = sun.management
            .ManagementFactoryHelper.getHotspotRuntimeMBean();

    @Data
    private static class SafePointStats {
        long safepointTime;
        long safepointCount;
    }

    public static void subscribeSafepointMetrics(MeterRegistry metricsRegistry) {

        SafePointStats safePointStats = new SafePointStats();

        Gauge.builder("jvm.safe_point_time", safePointStats, data -> {
            long current = getRuntimeMBean().getTotalSafepointTime();
            long delta = current - data.safepointTime;
            data.safepointTime = current;
            return delta;
        }).strongReference(true).register(metricsRegistry);

        Gauge.builder("jvm.safe_point_count", safePointStats, data -> {
            long current = getRuntimeMBean().getSafepointCount();
            long delta = current - data.safepointCount;
            data.safepointCount = current;
            return delta;
        }).strongReference(true).register(metricsRegistry);
    }

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
            subscribeSafepointMetrics(meterRegistry);
        }
    }
}

