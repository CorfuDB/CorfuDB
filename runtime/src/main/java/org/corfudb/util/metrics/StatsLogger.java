package org.corfudb.util.metrics;

public interface StatsLogger {

    Counter getCounter(String name);
    Timer getTimer(String name);
    StatsLogger scope(String name);
    <T extends Number> void registerGauge(String name, Gauge<T> gauge);
}
