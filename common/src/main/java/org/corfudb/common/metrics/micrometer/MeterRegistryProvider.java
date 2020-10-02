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
     * Get the previously configured meter registry.
     * If the registry has not been previously configured, create a default logging MeterRegistry
     * and return.
     * @return An optional configured meter registry.
     */
    public static Optional<MeterRegistry> getInstance() {
        if (!meterRegistry.isPresent()) {
            synchronized (MeterRegistry.class) {
                createLoggingMeterRegistry();
                return meterRegistry;
            }
        }
        return meterRegistry;
    }

    /**
     * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
     * with this meter registry will be exported in the InfluxDB line protocol format
     * (https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/)
     * with  the provided loggingInterval frequency.
     * @param logger A configured logger.
     * @param loggingInterval A duration between log appends for every metric.
     * @param localEndpoint A local endpoint to tag every metric with.
     */
    public static void createLoggingMeterRegistry(Logger logger, Duration loggingInterval,
                                                  String localEndpoint) {
        InfluxLineProtocolLoggingSink influxLineProtocolLoggingSink =
                new InfluxLineProtocolLoggingSink(logger);
        createLoggingMeterRegistry(loggingInterval, localEndpoint, influxLineProtocolLoggingSink);
    }

    /**
     * Configure the meter registry of type LoggingMeterRegistry. All the metrics registered
     * with this meter registry will be exported via provided logging sink with
     * the provided loggingInterval frequency.
     * @param sink A configured logging sink.
     * @param loggingInterval A duration between log appends for every metric.
     * @param localEndpoint A local endpoint to tag every metric with.
     */
    public static void createLoggingMeterRegistry(Duration loggingInterval,
                                                  String localEndpoint,
                                                  LoggingSink sink) {
        Supplier<Optional<MeterRegistry>> supplier = () -> {
            LoggingRegistryConfig config = new IntervalLoggingConfig(loggingInterval);
            LoggingMeterRegistry registry = LoggingMeterRegistry.builder(config)
                    .loggingSink(sink).build();
            registry.config().commonTags("endpoint", localEndpoint);
            return Optional.of(registry);
        };

        create(supplier);
    }

    /**
     * Configure the default registry of type LoggingMeterRegistry.
     */
    public static void createLoggingMeterRegistry() {
        Supplier<Optional<MeterRegistry>> supplier = () -> Optional.of(new LoggingMeterRegistry());
        create(supplier);
    }

    private static synchronized void create(Supplier<Optional<MeterRegistry>> meterRegistrySupplier) {
        if (!meterRegistry.isPresent()) {
            meterRegistry = meterRegistrySupplier.get();
        }
    }
}
