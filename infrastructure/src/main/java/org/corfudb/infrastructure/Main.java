package org.corfudb.infrastructure;

import io.micrometer.core.instrument.DistributionSummary;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.Optional;

public class Main {

    public static void main(String[] args) {

        MeterRegistryProvider.MeterRegistryInitializer.initDropwizardRegistry();
        for (int i = 0; i < 100; i++) {

            Optional<DistributionSummary> sum = MeterRegistryProvider.getInstance().map(r -> DistributionSummary.builder("logunit.read.throughput")
                    .publishPercentiles(0.50, 0.95, 0.99)
                    .publishPercentileHistogram()
                    .register(r));

            Sleep.sleepUninterruptibly(Duration.ofMillis(250));
            sum.ifPresent(s -> s.record(20));
        }
    }
}
