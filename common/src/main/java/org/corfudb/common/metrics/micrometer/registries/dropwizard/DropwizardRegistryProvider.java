package org.corfudb.common.metrics.micrometer.registries.dropwizard;

import com.codahale.metrics.MetricRegistry;

/**
 * Clients who are willing to integrate their dropwizard registries into Corfu metrics registry
 * must implement this interface.
 */
public interface DropwizardRegistryProvider {

    /**
     * Configure a dropwizard registry and return it.
     *
     * @return Configured dropwizard registry.
     */
    MetricRegistry provideRegistry();
}
