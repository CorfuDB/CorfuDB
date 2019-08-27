package org.corfudb.baseline;

public interface IProvider {
    StatsLogger getLogger(String name);
    void start();
    void stop();
}
