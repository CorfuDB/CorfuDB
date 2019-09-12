package org.corfudb.common.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;


/**
 * MetricsProvider provide metrics by given name and prefix.
 * It will be extended for other metric library if necessary.
 */

public interface MetricsProvider {
    Counter getCounter(String name);

    Timer getTimer(String name);

    void registerGauge(String name, Gauge gauge);

    Histogram getHistogram(String name);

    Histogram registerHistogram(String name, Reservoir reservoir);
}
