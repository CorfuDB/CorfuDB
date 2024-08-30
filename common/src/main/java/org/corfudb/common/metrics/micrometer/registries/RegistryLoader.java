package org.corfudb.common.metrics.micrometer.registries;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Lazily loads the registry providers.
 */
public class RegistryLoader {

    private final ServiceLoader<RegistryProvider> loader = ServiceLoader.load(RegistryProvider.class);

    public Iterator<RegistryProvider> getRegistries() {
        return loader.iterator();
    }
}

