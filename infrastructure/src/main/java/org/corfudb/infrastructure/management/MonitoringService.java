package org.corfudb.infrastructure.management;

import java.time.Duration;

/**
 * MonitoringService implementations run by the management agent.
 */
public interface MonitoringService {

    /**
     * Starts the long running service.
     */
    void start(Duration monitoringInterval);

    /**
     * Clean up.
     */
    void shutdown();
}
