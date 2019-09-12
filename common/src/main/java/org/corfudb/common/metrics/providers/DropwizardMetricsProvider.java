package org.corfudb.common.metrics.providers;

import com.codahale.metrics.*;
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

    @Override
    public Histogram getHistogram(String histogramName) {
        String metricName = MetricRegistry.name(prefix, histogramName);
        return metricRegistry.histogram(metricName);
    }

    @Override
    public Histogram registerHistogram(String histogramName, Reservoir reservoir) {
        String metricName = MetricRegistry.name(prefix, histogramName);
        Histogram histogram = new Histogram(reservoir);
        metricRegistry.register(metricName, histogram);
        return histogram;
    }
}
