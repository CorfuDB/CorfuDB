package org.corfudb.util.metrics;

public class NullMetricsProvider implements MetricsProvider {

    public static MetricsProvider INSTANCE = new NullMetricsProvider();

    @Override
    public StatsLogger getLogger(String name) {
        return NullStatsLogger.INSTANCE;
    }
}
