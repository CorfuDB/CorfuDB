package org.corfudb.common.metrics.micrometer.registries;

import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;

/**
 * Clients who are willing to integrate their registries into Corfu metrics composite registry
 * must implement this interface.
 */
public interface RegistryProvider {

    MeterRegistry provideRegistry(Map<String, String> registryMetadata);

    void close();

}

