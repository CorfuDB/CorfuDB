package org.corfudb.infrastructure;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * ManagementService implementations run by the management agent.
 */
public interface ManagementService {

    /**
     * Starts the long running service.
     *
     * @param interval interval to run the service
     */
    void start(Duration interval);

    /**
     * Clean up.
     */
    CompletableFuture<Void> shutdown();
}
