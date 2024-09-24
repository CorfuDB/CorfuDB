package org.corfudb.common.metrics.micrometer.registries;

import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;
import java.util.Optional;

/**
 * Clients who are willing to integrate their registries into Corfu metrics composite registry
 * must implement this interface.
 */
public interface RegistryProvider {

    Optional<MeterRegistry> provideRegistry(Map<String, String> registryMetadata);

    void close();

}

