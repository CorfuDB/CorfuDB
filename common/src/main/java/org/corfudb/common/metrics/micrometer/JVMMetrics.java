package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import lombok.Data;
import lombok.Getter;
import sun.management.HotspotRuntimeMBean;
import sun.management.ManagementFactoryHelper;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Provides JVM level metrics.
 */

public final class JVMMetrics {

    @Getter(lazy = true)
    private final static HotspotRuntimeMBean runtimeMBean = ManagementFactoryHelper.getHotspotRuntimeMBean();

    @Data
    private static class SafePointStats {
        long safepointTime;
        long safepointCount;
    }

    private static void subscribeThreadMetrics(MeterRegistry meterRegistry) {
        JvmThreadMetrics threadMetrics = new JvmThreadMetrics();
        threadMetrics.bindTo(meterRegistry);
    }

    private static void subscribeMemoryMetrics(MeterRegistry meterRegistry) {
        JvmMemoryMetrics memoryMetrics = new JvmMemoryMetrics();
        memoryMetrics.bindTo(meterRegistry);
    }

    private static void subscribeSafePointMetrics(MeterRegistry meterRegistry) {
        SafePointStats safePointStats = new SafePointStats();
        Gauge.builder("jvm.safe_point_time", safePointStats, data -> {
            long current = getRuntimeMBean().getTotalSafepointTime();
            long delta = current - data.safepointTime;
            data.safepointTime = current;
            return delta;
        }).baseUnit(TimeUnit.MILLISECONDS.toString())
                .strongReference(true)
                .register(meterRegistry);

        Gauge.builder("jvm.safe_point_count", safePointStats, data -> {
            long current = getRuntimeMBean().getSafepointCount();
            long delta = current - data.safepointCount;
            data.safepointCount = current;
            return delta;
        }).baseUnit(TimeUnit.MILLISECONDS.toString())
                .strongReference(true)
                .register(meterRegistry);
    }

    public static void register(Optional<MeterRegistry> metricsRegistry) {

        if (metricsRegistry.isPresent()) {
            final MeterRegistry meterRegistry = metricsRegistry.get();
            subscribeMemoryMetrics(meterRegistry);
            subscribeThreadMetrics(meterRegistry);
            subscribeSafePointMetrics(meterRegistry);
        }
    }
}

