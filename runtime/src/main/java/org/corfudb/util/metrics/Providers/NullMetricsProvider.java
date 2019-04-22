package org.corfudb.util.metrics.Providers;

import org.corfudb.util.metrics.MetricsProvider;
import org.corfudb.util.metrics.StatsLogger;
import org.corfudb.util.metrics.loggers.NullStatsLogger;

public class NullMetricsProvider implements MetricsProvider {

    public static MetricsProvider INSTANCE = new NullMetricsProvider();

    @Override
    public StatsLogger getLogger(String name) {
        return NullStatsLogger.INSTANCE;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
