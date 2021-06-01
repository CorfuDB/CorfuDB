package org.corfudb.common.metrics.micrometer.registries;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;


/**
 * Wraps dropwizard registry into micrometer registry.
 */
public class DropwizardRegistryWrapper {
    private DropwizardRegistryWrapper() {

    }

    public static MeterRegistry wrap(MetricRegistry registry) {
        DropwizardConfig config = new DropwizardConfig() {
            @Override
            public String prefix() {
                return "";
            }

            @Override
            public String get(String key) {
                return null;
            }
        };
        return new DropwizardMeterRegistry(config, registry, HierarchicalNameMapper.DEFAULT, Clock.SYSTEM) {
            @Override
            protected Double nullGaugeValue() {
                return null;
            }
        };
    }
}
