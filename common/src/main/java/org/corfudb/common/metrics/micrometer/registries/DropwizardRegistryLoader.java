package org.corfudb.common.metrics.micrometer.registries;


import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Lazily loads the dropwizard registry providers.
 */
public class DropwizardRegistryLoader {

    ServiceLoader<DropwizardRegistryProvider> loader = ServiceLoader.load(DropwizardRegistryProvider.class);

    public Iterator<DropwizardRegistryProvider> getRegistries() {
        return loader.iterator();
    }
}
