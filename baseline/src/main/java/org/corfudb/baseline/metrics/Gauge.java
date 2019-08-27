package org.corfudb.baseline.metrics;

public interface Gauge<T extends Number> {
    T getValue();
}
