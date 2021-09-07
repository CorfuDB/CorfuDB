package org.corfudb.common.metrics.micrometer.registries.dropwizard;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;

import java.util.concurrent.TimeUnit;

/**
 * This class overrides the timer and histogram of DropwizardMeterRegistry and adds a
 * SlidingWindowReservoirArray. This allows for the reporting of measurements made within the last
 * n TimeUnits. This is the default behavior in other micrometer registries.
 */
public class DropwizardMeterRegistryWithSlidingTimeWindow extends DropwizardMeterRegistry {
    private final long windowSize;
    private final TimeUnit windowUnits;
    private final HierarchicalNameMapper nameMapper;

    public DropwizardMeterRegistryWithSlidingTimeWindow(DropwizardConfig config, MetricRegistry registry, HierarchicalNameMapper nameMapper, Clock clock, long windowSize, TimeUnit windowUnits) {
        super(config, registry, nameMapper, clock);
        this.windowSize = windowSize;
        this.windowUnits = windowUnits;
        this.nameMapper = nameMapper;
    }

    private String getName(Meter.Id id) {
        return nameMapper.toHierarchicalName(id, config().namingConvention());
    }

    @Override
    protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, PauseDetector pauseDetector) {
        SlidingTimeWindowArrayReservoir reservoir = new SlidingTimeWindowArrayReservoir(windowSize, windowUnits);
        com.codahale.metrics.Timer timer = new com.codahale.metrics.Timer(reservoir);
        MetricRegistry registry = super.getDropwizardRegistry();
        return new DropwizardTimerDelegate(id, registry.timer(getName(id), () -> timer), clock, distributionStatisticConfig, pauseDetector);
    }

    @Override
    protected DistributionSummary newDistributionSummary(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, double scale) {
        SlidingTimeWindowArrayReservoir reservoir = new SlidingTimeWindowArrayReservoir(windowSize, windowUnits);
        Histogram histogram = new Histogram(reservoir);
        MetricRegistry registry = super.getDropwizardRegistry();
        return new DropwizardDistributionSummaryDelegate(id, clock, registry.histogram(getName(id), () -> histogram), distributionStatisticConfig, scale);
    }

    @Override
    protected Double nullGaugeValue() {
        return 0.0;
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MICROSECONDS;
    }
}
