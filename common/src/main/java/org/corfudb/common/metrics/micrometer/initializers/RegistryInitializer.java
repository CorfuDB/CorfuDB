package org.corfudb.common.metrics.micrometer.initializers;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * If implemented, creates a metrics registry.
 */
public interface RegistryInitializer {

    MeterRegistry createRegistry();

}
