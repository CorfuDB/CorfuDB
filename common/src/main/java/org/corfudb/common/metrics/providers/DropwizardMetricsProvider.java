package org.corfudb.common.metrics.providers;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import lombok.Getter;

import org.corfudb.common.metrics.MetricsProvider;

public class DropwizardMetricsProvider implements MetricsProvider {

    @Getter
    private final MetricRegistry metricRegistry;
    private final String prefix;

    public DropwizardMetricsProvider(String name, MetricRegistry registry) {
        this.prefix = name;
        this.metricRegistry = registry;
    }

    @Override
    public Counter getCounter(String counterName) {
        String metricName = MetricRegistry.name(prefix, counterName);
        return metricRegistry.counter(metricName);
    }

    @Override
    public Timer getTimer(String timerName) {
        String metricName = MetricRegistry.name(prefix, timerName);
        return metricRegistry.timer(metricName);
    }

    @Override
    public void registerGauge(String gaugeName, Gauge gauge) {
        String metricName = MetricRegistry.name(prefix, gaugeName);
        metricRegistry.register(metricName, gauge);
    }
}
