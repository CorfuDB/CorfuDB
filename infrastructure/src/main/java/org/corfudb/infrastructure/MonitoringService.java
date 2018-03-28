package org.corfudb.infrastructure;

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
