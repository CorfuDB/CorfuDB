package org.corfudb.baseline;

public interface Provider {
    StatsLogger getLogger(String name);
    void start();
    void stop();
}
