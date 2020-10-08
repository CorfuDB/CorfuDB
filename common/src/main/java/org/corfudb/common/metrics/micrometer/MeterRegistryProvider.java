package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import org.corfudb.common.metrics.micrometer.loggingsink.InfluxLineProtocolLoggingSink;
import org.corfudb.common.metrics.micrometer.loggingsink.LoggingSink;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A configuration class for a meter (metrics) registry.
 */
public class MeterRegistryProvider {
    private static Optional<MeterRegistry> meterRegistry = Optional.empty();

    private MeterRegistryProvider() {

    }

    /**
     * Class that initializes the Meter Registry.
     */
    public static class MeterRegistryInitializer extends MeterRegistryProvider {
        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported in the InfluxDB line protocol format
         * (https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/)
         * with  the provided loggingInterval frequency.
         * @param logger A configured logger.
         * @param loggingInterval A duration between log appends for every metric.
         * @param localEndpoint A local endpoint to tag every metric with.
         */
        public static synchronized void init(Logger logger, Duration loggingInterval,
                                                      String localEndpoint) {
            InfluxLineProtocolLoggingSink influxLineProtocolLoggingSink =
                    new InfluxLineProtocolLoggingSink(logger);
            init(loggingInterval, localEndpoint, influxLineProtocolLoggingSink);
        }

        /**
         * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
         * with this meter registry will be exported via provided logging sink with
         * the provided loggingInterval frequency.
         * @param sink A configured logging sink.
         * @param loggingInterval A duration between log appends for every metric.
         * @param localEndpoint A local endpoint to tag every metric with.
         */
        public static synchronized void init(Duration loggingInterval,
                                                      String localEndpoint,
                                                      LoggingSink sink) {
            Supplier<Optional<MeterRegistry>> supplier = () -> {
                LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
                LoggingMeterRegistry registry = LoggingMeterRegistry.builder(config)
                        .loggingSink(sink).build();
                registry.config().commonTags("endpoint", localEndpoint);
                return Optional.of(registry);
            };

            init(supplier);
        }

        /**
         * Configure the default registry of type LoggingMeterRegistry.
         */
        public static synchronized void init() {
            Supplier<Optional<MeterRegistry>> supplier = () -> Optional.of(new LoggingMeterRegistry());
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
     * Get the previously configured meter registry.
     * If the registry has not been previously configured, return an empty option.
     * @return An optional configured meter registry.
     */
    public static synchronized Optional<MeterRegistry> getInstance() {
        return meterRegistry;
    }
}
