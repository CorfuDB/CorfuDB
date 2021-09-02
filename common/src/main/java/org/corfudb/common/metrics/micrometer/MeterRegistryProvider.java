package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.initializers.RegistryInitializer;

import java.util.List;
import java.util.Optional;

/**
 * A configuration class for a meter (metrics) registry.
 */
@Slf4j
public class MeterRegistryProvider {
    private static final CompositeMeterRegistry meterRegistry = new CompositeMeterRegistry();
    private static Optional<String> id = Optional.empty();
    private static Optional<MetricType> metricType = Optional.empty();

    private MeterRegistryProvider() {

    }

    static enum MetricType {
        SERVER,
        CLIENT
    }

    /**
     * Class that initializes the Meter Registry.
     */
    public static class MeterRegistryInitializer extends MeterRegistryProvider {

        /**
         * Configure the registries for the corfu server.
         *
         * @param inits      A list of registry initializers.
         * @param identifier A global identifier to tag every metric with.
         */
        public static void initServerMetrics(List<RegistryInitializer> inits, String identifier) {
            metricType = Optional.of(MetricType.SERVER);
            id = Optional.of(identifier);
            populateCompositeRegistry(inits);
        }

        /**
         * Configure the registries for the corfu client.
         *
         * @param inits      A list of registry initializers.
         * @param identifier A global identifier to tag every metric with.
         */
        public static void initClientMetrics(List<RegistryInitializer> inits, String identifier) {
            metricType = Optional.of(MetricType.CLIENT);
            id = Optional.of(identifier);
            populateCompositeRegistry(inits);
        }

        private static void populateCompositeRegistry(List<RegistryInitializer> inits) {
            inits.forEach(init -> {
                try {
                    MeterRegistry registry = init.createRegistry();
                    addToCompositeRegistry(registry);
                } catch (RuntimeException re) {
                    log.error("Issue initializing this registry. Skipping.", re);
                }
            });
        }

        private static void addToCompositeRegistry(MeterRegistry registry) {
            if (containsRegistry(registry)) {
                log.warn("This registry has already been initialized.");
                removeRegistry(registry);
            }
            addRegistry(registry);
        }

        private static void addRegistry(MeterRegistry componentRegistry) {
            meterRegistry.add(componentRegistry);
        }

        private static void removeRegistry(MeterRegistry componentRegistry) {
            meterRegistry.remove(componentRegistry);
        }
    }


    /**
     * Returns true if the composite contains component.
     *
     * @return Returns true if this registry is present.
     */
    public static boolean containsRegistry(MeterRegistry componentRegistry) {
        return meterRegistry.getRegistries().contains(componentRegistry);
    }


    /**
     * Returns true if no registries were registered with the composite registry.
     *
     * @return Returns true if 0 registries, false otherwise.
     */
    public static boolean isEmpty() {
        return meterRegistry.getRegistries().isEmpty();
    }

    /**
     * Get the previously configured meter registry.
     * If the registry has not been previously configured, return an empty option.
     *
     * @return An optional configured meter registry.
     */
    public static synchronized Optional<MeterRegistry> getInstance() {
        return Optional.of(meterRegistry);
    }

    /**
     * Get the metric type of this registry.
     *
     * @return An optional metric type.
     */
    public static synchronized Optional<MetricType> getMetricType() {
        return metricType;
    }

    /**
     * Remove the meter by id.
     *
     * @param name Name of a meter.
     * @param tags Tags.
     * @param type Type of a meter.
     */
    public static synchronized void deregisterServerMeter(String name, Tags tags, Meter.Type type) {
        if (!id.isPresent()) {
            log.warn("Id must be present to deregister meters.");
            return;
        }
        String server = id.get();
        Tags tagsToLookFor = tags.and(Tag.of("id", server));
        Meter.Id meterId = new Meter.Id(name, tagsToLookFor, null, null, type);
        meterRegistry.remove(meterId);
    }
}
