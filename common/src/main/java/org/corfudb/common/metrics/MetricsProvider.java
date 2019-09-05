package org.corfudb.common.metrics;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;

/**
 * MetricsProvider provide metrics by given name and prefix.
 * It will be extended for other metric library if necessary.
 */

public interface MetricsProvider {
    Counter getCounter(String name);

    Timer getTimer(String name);

    void registerGauge(String gaugeName, Gauge gauge);
}
