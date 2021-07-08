package org.corfudb.common.metrics.micrometer.registries.dropwizard;

import io.micrometer.core.instrument.AbstractDistributionSummary;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.TimeWindowMax;
import io.micrometer.core.instrument.util.MeterEquivalence;
import io.micrometer.core.lang.Nullable;

import java.util.concurrent.atomic.DoubleAdder;

public class DropwizardDistributionSummaryCustom extends AbstractDistributionSummary {
    private final com.codahale.metrics.Histogram impl;
    private final DoubleAdder totalAmount = new DoubleAdder();
    private final TimeWindowMax max;

    DropwizardDistributionSummaryCustom(Id id, Clock clock, com.codahale.metrics.Histogram impl, DistributionStatisticConfig distributionStatisticConfig,
                                        double scale) {
        super(id, clock, distributionStatisticConfig, scale, false);
        this.impl = impl;
        this.max = new TimeWindowMax(clock, distributionStatisticConfig);
    }

    @Override
    protected void recordNonNegative(double amount) {
        if (amount >= 0) {
            impl.update((long) amount);
            totalAmount.add(amount);
            max.record(amount);
        }
    }

    @Override
    public long count() {
        return impl.getCount();
    }

    @Override
    public double totalAmount() {
        return totalAmount.doubleValue();
    }

    @Override
    public double max() {
        return max.poll();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(@Nullable Object o) {
        return MeterEquivalence.equals(this, o);
    }

    @Override
    public int hashCode() {
        return MeterEquivalence.hashCode(this);
    }
}