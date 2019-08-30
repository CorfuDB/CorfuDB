package org.corfudb.common;

public interface Provider {
    StatsLogger getLogger(String name);
    void start();
    void stop();
}
