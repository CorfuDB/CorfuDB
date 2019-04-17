package org.corfudb.util.metrics;

public interface Gauge<T extends Number> {
    T getValue();
}
