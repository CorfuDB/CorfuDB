package org.corfudb.common.metrics;

/**
 * MetricsServer can be implemented by different metrics exporters, like prometheus.
 */

public interface MetricsServer {
    /**
     * Start exporter by config.
     */
    void start();

    /**
     * Clean up.
     */
    void stop();
}
