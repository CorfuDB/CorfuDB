package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.registries.LoggingMeterRegistryWithHistogramSupport;
import org.corfudb.common.metrics.micrometer.registries.RegistryLoader;
import org.corfudb.common.metrics.micrometer.registries.RegistryProvider;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A configuration class for a meter (metrics) registry.
 */
@Slf4j
public class MeterRegistryProvider {
    private static final CompositeMeterRegistry meterRegistry = new CompositeMeterRegistry();
    private static Optional<String> id = Optional.empty();
    private static Optional<RegistryProvider> provider = Optional.empty();

    private MeterRegistryProvider() {

    }

    /**
     * Class that initializes the Meter Registry.
     */
    public static class MeterRegistryInitializer extends MeterRegistryProvider {
        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported via provided logging sink with
         * the provided loggingInterval frequency.
         *
         * @param logger          An instance of the logger to print metrics.
         * @param loggingInterval A duration between log appends for every metric.
         * @param identifier      A global identifier to tag every metric with.
         */
        public static synchronized void initLoggingRegistry(Logger logger, Duration loggingInterval, String identifier) {
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
                LoggingMeterRegistryWithHistogramSupport registry =
                        new LoggingMeterRegistryWithHistogramSupport(config, logger::debug);
                registry.config().commonTags("id", identifier);
                id = Optional.of(identifier);
                Optional<MeterRegistry> ret = Optional.of(registry);
                JVMMetrics.register(ret);
                return ret;
            };

            addToCompositeRegistry(supplier);
        }

        /**
         * Looks up the implementations of RegistryProvider on the classpath, initializes registries,
         * and registers them with a global composite registry.
         */
        public static synchronized void registerProvidedRegistries() {
            RegistryLoader loader = new RegistryLoader();
            Iterator<RegistryProvider> registries = loader.getRegistries();
            while (registries.hasNext()) {
                try {
                    RegistryProvider registryProvider = registries.next();
                    log.info("Registering provider: {}", registryProvider);
                    provider = Optional.of(registryProvider);
                    MeterRegistry registry = registryProvider.provideRegistry();
                    addToCompositeRegistry(() -> Optional.of(registry));
                } catch (Throwable exception) {
                    log.error("Problems registering a registry", exception);
                }
            }
        }

        private static void addToCompositeRegistry(Supplier<Optional<MeterRegistry>> meterRegistrySupplier) {
            Optional<MeterRegistry> componentRegistry = meterRegistrySupplier.get();
            componentRegistry.ifPresent(registry -> {
                if (containsRegistry(registry)) {
                    log.warn("Registry has already been initialized.");
                    removeRegistry(registry);
                }
                addRegistry(registry);
            });
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
     * Close all the registries.
     */
    public static synchronized void close() {
        meterRegistry.close();
        provider.ifPresent(RegistryProvider::close);
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
        Meter.Id id = new Meter.Id(name, tagsToLookFor, null, null, type);
        meterRegistry.remove(id);
    }
}
