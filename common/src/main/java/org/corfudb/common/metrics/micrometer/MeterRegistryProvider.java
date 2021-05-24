package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
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
        public static synchronized void init(Logger logger, Duration loggingInterval, String identifier) {
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

            init(supplier);
        }

        private static void init(Supplier<Optional<MeterRegistry>> meterRegistrySupplier) {
            if (meterRegistry.isPresent()) {
               log.warn("Registry has already been initialized.");
            }
            meterRegistry = meterRegistrySupplier.get();
        }
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
