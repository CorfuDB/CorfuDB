package org.corfudb.common.metrics;

public interface Gauge<T extends Number> {
    T getValue();
}
