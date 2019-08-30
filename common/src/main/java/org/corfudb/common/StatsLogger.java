package org.corfudb.common;

import org.corfudb.common.metrics.Counter;
import org.corfudb.common.metrics.Gauge;
import org.corfudb.common.metrics.Timer;

public interface StatsLogger {
    Counter getCounter(String name);
    Timer getTimer(String name);
    <T extends Number> void registerGauge(String name, Gauge<T> gauge);
    StatsLogger scope(String name);
}
