package org.corfudb.util.metrics;

/**
 * Created by box on 4/17/19.
 */
public interface MetricsProvider {

    StatsLogger getLogger(String name);

}
