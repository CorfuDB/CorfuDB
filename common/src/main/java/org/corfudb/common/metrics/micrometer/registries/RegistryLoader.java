package org.corfudb.common.metrics.micrometer.registries;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Lazily loads the registry providers.
 */
public class RegistryLoader {

    private final ServiceLoader<RegistryProvider> loader = ServiceLoader.load(RegistryProvider.class,
            ClassLoader.getSystemClassLoader());

    public Iterator<RegistryProvider> getRegistries() {
        return loader.iterator();
    }
}

