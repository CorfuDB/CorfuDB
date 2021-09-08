package org.corfudb.common.metrics.micrometer.registries;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Clients who are willing to integrate their registries into Corfu metrics composite registry
 * must implement this interface.
 */
public interface RegistryProvider {

    MeterRegistry provideRegistry();

    void close();

}

