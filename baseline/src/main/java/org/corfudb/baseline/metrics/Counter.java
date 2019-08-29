package org.corfudb.baseline.metrics;

public interface Counter {
    void inc();
    void dec();
    long getCount();
}
