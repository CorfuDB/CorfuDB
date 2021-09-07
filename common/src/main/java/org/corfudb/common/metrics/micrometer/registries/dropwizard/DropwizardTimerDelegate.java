package org.corfudb.common.metrics.micrometer.registries.dropwizard;

import com.codahale.metrics.Timer;
import io.micrometer.core.instrument.AbstractTimer;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.TimeWindowMax;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.util.TimeUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class delegates the functionality of a micrometer timer to
 * the dropwizard timer
 */
public class DropwizardTimerDelegate extends AbstractTimer {
    private final Timer impl;
    private final AtomicLong totalTime = new AtomicLong(0);
    private final TimeWindowMax max;

    DropwizardTimerDelegate(Id id, Timer impl, Clock clock, DistributionStatisticConfig distributionStatisticConfig, PauseDetector pauseDetector) {
        super(id, clock, distributionStatisticConfig, pauseDetector, TimeUnit.MICROSECONDS, false);
        this.impl = impl;
        this.max = new TimeWindowMax(clock, distributionStatisticConfig);
    }

    @Override
    protected void recordNonNegative(long amount, TimeUnit unit) {
        if (amount >= 0) {
            impl.update(amount, unit);

            long nanoAmount = TimeUnit.NANOSECONDS.convert(amount, unit);
            max.record(nanoAmount, TimeUnit.NANOSECONDS);
            totalTime.addAndGet(nanoAmount);
        }
    }

    @Override
    public long count() {
        return impl.getCount();
    }

    @Override
    public double totalTime(TimeUnit unit) {
        return TimeUtils.nanosToUnit(totalTime.get(), unit);
    }

    @Override
    public double max(TimeUnit unit) {
        return max.poll(unit);
    }

    public Timer getImpl() {
        return impl;
    }
}
