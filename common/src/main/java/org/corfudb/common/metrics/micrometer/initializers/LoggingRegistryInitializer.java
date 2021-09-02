package org.corfudb.common.metrics.micrometer.initializers;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.common.metrics.micrometer.IntervalLoggingConfig;
import org.corfudb.common.metrics.micrometer.registries.LoggingMeterRegistryWithHistogramSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static lombok.Builder.Default;

/**
 * A configuration class for creating a LoggingMeterRegistryWithHistogramSupport.
 * Defines the sink logger as well as the exporting frequency.
 */
@Builder
@ToString
@Getter
public class LoggingRegistryInitializer implements RegistryInitializer {

    @Default
    private final Logger logger = LoggerFactory.getLogger(LoggingRegistryInitializer.class);

    @Default
    private final Duration exportDuration = Duration.ofMinutes(1);

    @Override
    public MeterRegistry createRegistry() {
        LoggingRegistryConfig config = new IntervalLoggingConfig(exportDuration);
        return new LoggingMeterRegistryWithHistogramSupport(config, logger::debug);
    }
}
