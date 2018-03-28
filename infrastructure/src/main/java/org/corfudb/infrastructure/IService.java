package org.corfudb.infrastructure;

/**
 * Service implementations run by the management agent.
 */
public interface IService {

    /**
     * Starts the long running service.
     */
    void runTask();

    /**
     * Clean up.
     */
    void shutdown();
}
