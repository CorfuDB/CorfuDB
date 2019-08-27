package org.corfudb.baseline;

import org.corfudb.baseline.metrics.Counter;
import org.corfudb.baseline.metrics.Gauge;
import org.corfudb.baseline.metrics.Timer;

public interface StatsLogger {
    Counter getCounter(String name);
    Timer getTimer(String name);
    <T extends Number> void registerGauge(String name, Gauge<T> gauge);
    StatsLogger scope(String name);
}
