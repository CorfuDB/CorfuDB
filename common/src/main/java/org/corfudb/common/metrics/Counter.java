package org.corfudb.common.metrics;

public interface Counter {
    void inc();
    void dec();
    long getCount();
}
