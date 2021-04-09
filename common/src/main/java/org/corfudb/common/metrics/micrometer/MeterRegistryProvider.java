package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.wavefront.WavefrontConfig;
import io.micrometer.wavefront.WavefrontMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.registries.LoggingMeterRegistryWithHistogramSupport;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A configuration class for a meter (metrics) registry.
 */
@Slf4j
public class MeterRegistryProvider {
    private static Optional<MeterRegistry> meterRegistry = Optional.empty();
    private static Optional<String> id = Optional.empty();

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
        public static synchronized void initLoggingRegistry(Logger logger, Duration loggingInterval,
                                                            String identifier) {
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
                LoggingMeterRegistryWithHistogramSupport registry =
                        new LoggingMeterRegistryWithHistogramSupport(config, logger::debug);
                return Optional.of(registry);
            };

            init(supplier, identifier);
        }

        /**
         * Configure the meter registry of type WavefrontMeterRegistry. All the metrics registered
         * with this meter registry will be exported to the configured Wavefront proxy.
         *
         * @param config          A config for Wavefront proxy.
         * @param identifier      A global identifier to tag every metric with.
         */
        public static synchronized void initWavefrontRegistry(WavefrontProxyConfig config,
                                                              String identifier) {
            Supplier<WavefrontConfig> wavefrontConfigSupplier = () -> new WavefrontConfig() {
                @Override
                public String uri() {
                    return String.format("proxy://%s:%s", config.getHost(), config.getPort());
                }

                @Override
                public String get(String key) {
                    return null;
                }

                @Override
                public String apiToken() {
                    return config.getApiToken();
                }
            };
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                MeterRegistry registry =
                        new WavefrontMeterRegistry(wavefrontConfigSupplier.get(), Clock.SYSTEM);
                return Optional.of(registry);
            };

            init(supplier, identifier);
        }

        private static void init(Supplier<Optional<MeterRegistry>> meterRegistrySupplier,
                                 String identifier) {
            if (meterRegistry.isPresent()) {
                log.warn("Registry has already been initialized.");
            }
            else {
                meterRegistry = meterRegistrySupplier.get();
                meterRegistry.ifPresent(registry -> registry.config().commonTags("id", identifier));
                JVMMetrics.register(meterRegistry);
                id = Optional.of(identifier);
            }
        }
    }

    /**
     * Register timer if needed.
     *
     * @param name Name of a timer.
     * @param tags Tags for a timer.
     */
    public static void timer(String name, String... tags) {
        MeterRegistryProvider.getInstance().ifPresent(registry -> registry.timer(name, tags));
    }

    /**
     * Get the previously configured meter registry.
     * If the registry has not been previously configured, return an empty option.
     *
     * @return An optional configured meter registry.
     */
    public static synchronized Optional<MeterRegistry> getInstance() {
        return meterRegistry;
    }

    /**
     * Remove the meter by id.
     *
     * @param name Name of a meter.
     * @param tags Tags.
     * @param type Type of a meter.
     */
    public static synchronized void deregisterServerMeter(String name, Tags tags, Meter.Type type) {
        if (!meterRegistry.isPresent()) {
            return;
        }
        if (!id.isPresent()) {
            throw new IllegalStateException("Id must be present to deregister meters.");
        }
        String server = id.get();
        Tags tagsToLookFor = tags.and(Tag.of("id", server));
        Meter.Id id = new Meter.Id(name, tagsToLookFor, null, null, type);
        meterRegistry.ifPresent(registry -> registry.remove(id));
    }
}
