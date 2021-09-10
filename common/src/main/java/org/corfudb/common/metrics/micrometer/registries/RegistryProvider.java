package org.corfudb.common.metrics.micrometer.registries;

import org.corfudb.common.metrics.micrometer.initializers.RegistryInitializer;

/**
 * Clients who are willing to integrate their registries into Corfu metrics composite registry
 * must implement this interface.
 */
public interface RegistryProvider extends RegistryInitializer {

    void close();
}

