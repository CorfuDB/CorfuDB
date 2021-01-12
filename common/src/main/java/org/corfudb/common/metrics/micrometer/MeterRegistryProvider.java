package org.corfudb.common.metrics.micrometer;

import static org.corfudb.common.metrics.micrometer.registries.LoggingMeterRegistryWithHistogramSupport.DataProtocol.INFLUX;


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
import java.util.UUID;
import java.util.function.Supplier;

/**
 * A configuration class for a meter (metrics) registry.
 */
@Slf4j
public class MeterRegistryProvider {
    private static Optional<MeterRegistry> meterRegistry = Optional.empty();
    private static Optional<String> endpoint = Optional.empty();
    private MeterRegistryProvider() {

    }

    /**
     * Class that initializes the Meter Registry.
     */
    public static class MeterRegistryInitializer extends MeterRegistryProvider {

        /**
         * Create a new instance of MeterRegistry with the given logger, loggingInterval
         * and clientId.
         * @param logger A configured logger.
         * @param loggingInterval A duration between log appends for every metric.
         * @param clientId An id of a client for this metric.
         * @return A new meter registry.
         */
        public static MeterRegistry newInstance(Logger logger, Duration loggingInterval,
                                                UUID clientId) {
            LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
            LoggingMeterRegistryWithHistogramSupport registry =
                    new LoggingMeterRegistryWithHistogramSupport(config, logger::debug, INFLUX);
            registry.config().commonTags("clientId", clientId.toString());
            return registry;
        }

        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported via provided logging sink with
         * the provided loggingInterval frequency.
         * @param logger          An instance of the logger to print metrics.
         * @param loggingInterval A duration between log appends for every metric.
         * @param localEndpoint A local endpoint to tag every metric with.
         */
        public static synchronized void init(Logger logger, Duration loggingInterval, String localEndpoint) {
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
                LoggingMeterRegistryWithHistogramSupport registry =
                        new LoggingMeterRegistryWithHistogramSupport(config, logger::debug, INFLUX);
                registry.config().commonTags("endpoint", localEndpoint);
                endpoint = Optional.of(localEndpoint);
                Optional<MeterRegistry> ret = Optional.of(registry);
                JVMMetrics.register(ret);
                return ret;
            };

            init(supplier);
        }

        private static void init(Supplier<Optional<MeterRegistry>> meterRegistrySupplier) {
            if (meterRegistry.isPresent()) {
                throw new IllegalStateException("Registry has already been initialized.");
            }
            meterRegistry = meterRegistrySupplier.get();
        }
    }

    /**
     * Register timer if needed.
     * @param name Name of a timer.
     * @param tags Tags for a timer.
     */
    public static void timer(String name, String... tags) {
        MeterRegistryProvider.getInstance().ifPresent(registry -> registry.timer(name, tags));
    }

    /**
     * Get the previously configured meter registry.
     * If the registry has not been previously configured, return an empty option.
     * @return An optional configured meter registry.
     */
    public static synchronized Optional<MeterRegistry> getInstance() {
        return meterRegistry;
    }

    /**
     * Remove the meter by id.
     * @param name Name of a meter.
     * @param tags Tags.
     * @param type Type of a meter.
     */
    public static synchronized void deregisterServerMeter(String name, Tags tags, Meter.Type type) {
        if (!meterRegistry.isPresent()) {
            return;
        }
        if (!endpoint.isPresent()) {
            throw new IllegalStateException("Endpoint must be present to deregister meters.");
        }
        String server = endpoint.get();
        Tags tagsToLookFor = tags.and(Tag.of("endpoint", server));
        Meter.Id id = new Meter.Id(name, tagsToLookFor, null, null, type);
        meterRegistry.ifPresent(registry -> registry.remove(id));
    }
}
