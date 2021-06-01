package org.corfudb.common.metrics.micrometer.registries;

import com.codahale.metrics.MetricRegistry;

/**
 * Clients who are willing to provide their dropwizard registries to Corfu metrics registry
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
