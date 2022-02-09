package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import lombok.Getter;
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
    @Getter
    private static CompositeMeterRegistry meterRegistry;
    @Getter
    private static Optional<String> id = Optional.empty();
    @Getter
    private static Optional<MetricType> metricType = Optional.empty();
    private static Optional<RegistryProvider> provider = Optional.empty();

    private MeterRegistryProvider() {

    }

    public static enum MetricType {
        SERVER,
        CLIENT
    }

    /**
     * Class that initializes the Meter Registry.
     */
    public static class MeterRegistryInitializer extends MeterRegistryProvider {

        private static synchronized void initMetrics(Logger logger, Duration loggingInterval, String identifier, MetricType type) {
            if (metricType.isPresent()) {
                log.warn("Registry has already been initialized with a type: " + metricType.get());
                return;
            }

            meterRegistry = new CompositeMeterRegistry();
            metricType = Optional.of(type);
            id = Optional.of(identifier);

            initLoggingRegistry(logger, loggingInterval, identifier);
            registerProvidedRegistries();
        }

        /**
         * Configure the meter registry for Corfu server.
         * All the metrics will be exported to the logging registry and optionally to any third party provided registries.
         *
         * @param logger          An instance of the logger to print metrics.
         * @param loggingInterval A duration between log appends for every metric.
         * @param identifier      A global identifier to tag every metric with.
         */
        public static void initServerMetrics(Logger logger, Duration loggingInterval, String identifier) {
            initMetrics(logger, loggingInterval, identifier, MetricType.SERVER);
            log.info("Server metrics initialized");
        }

        /**
         * Configure the meter registry for Corfu client.
         * All the metrics will be exported to the logging registry and optionally to any third party provided registries.
         *
         * @param logger          An instance of the logger to print metrics.
         * @param loggingInterval A duration between log appends for every metric.
         * @param identifier      A global identifier to tag every metric with.
         */
        public static synchronized void initClientMetrics(Logger logger, Duration loggingInterval, String identifier) {
            initMetrics(logger, loggingInterval, identifier, MetricType.CLIENT);
            log.info("Client metrics initialized");
        }

        private static synchronized void initLoggingRegistry(Logger logger, Duration loggingInterval, String identifier) {
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
                LoggingMeterRegistryWithHistogramSupport registry =
                        new LoggingMeterRegistryWithHistogramSupport(config, logger::debug);
                registry.config().commonTags("id", identifier);
                Optional<MeterRegistry> ret = Optional.of(registry);
                JVMMetrics.register(ret);
                return ret;
            };

            addToCompositeRegistry(supplier);
        }

        private static synchronized void registerProvidedRegistries() {
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
            componentRegistry.ifPresent(MeterRegistryInitializer::addRegistry);
        }

        private static void addRegistry(MeterRegistry componentRegistry) {
            meterRegistry.add(componentRegistry);
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
     * Get the composite meter registry.
     *
     * @return An optional configured meter registry.
     */
    public static synchronized Optional<MeterRegistry> getInstance() {
        return Optional.ofNullable(meterRegistry);
    }

    /**
     * Close all the registries.
     */
    public static synchronized void close() {
        meterRegistry.close();
        meterRegistry.getRegistries().forEach(registry -> meterRegistry.remove(registry));
        provider.ifPresent(RegistryProvider::close);
        provider = Optional.empty();
        metricType = Optional.empty();
        id = Optional.empty();
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
     * @param meterId Meter id.
     */
    public static synchronized void deregisterByMeterId(Meter.Id meterId) {
        meterRegistry.remove(meterId);
    }

    public static Optional<Tag> getTagId() {
        return id.map(uId -> Tag.of("id", uId));
    }
}
