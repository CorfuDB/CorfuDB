package org.corfudb.util.metrics;

public interface Counter {

    long getCount();
    void inc();
    void dec();
}
