package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.Getter;
import sun.management.HotspotRuntimeMBean;
import sun.management.ManagementFactoryHelper;

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

    public static void register(Optional<MeterRegistry> metricsRegistry) {

        if (metricsRegistry.isPresent()) {
            SafePointStats safePointStats = new SafePointStats();

            Gauge.builder("jvm.safe_point_time", safePointStats, data -> {
                long current = getRuntimeMBean().getTotalSafepointTime();
                long delta = current - data.safepointTime;
                data.safepointTime = current;
                return delta;
            }).baseUnit(TimeUnit.MILLISECONDS.toString())
                    .strongReference(true)
                    .register(metricsRegistry.get());

            Gauge.builder("jvm.safe_point_count", safePointStats, data -> {
                long current = getRuntimeMBean().getSafepointCount();
                long delta = current - data.safepointCount;
                data.safepointCount = current;
                return delta;
            }).baseUnit(TimeUnit.MILLISECONDS.toString())
                    .strongReference(true)
                    .register(metricsRegistry.get());
        }
    }
}
