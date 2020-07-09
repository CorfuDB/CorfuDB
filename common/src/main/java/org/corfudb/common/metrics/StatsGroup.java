package org.corfudb.common.metrics;

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Maithem on 7/7/20.
 */
public class StatsGroup {

    @Getter
    private final String prefix;

    @Getter
    private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();

    @Getter
    private final Map<String, Meter> meters = new ConcurrentHashMap<>();

    @Getter
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    @Getter
    private final Map<String, Gauge> gauges = new ConcurrentHashMap<>();

    @Getter
    private final Map<String, StatsGroup> scopes = new ConcurrentHashMap<>();

    public StatsGroup(String prefix) {
        this.prefix = prefix;
    }

    private String name(String prefix, String name) {
        return prefix + "_" + name;
    }

    public Histogram createHistogram(String name) {
        return histograms.merge(name, new Histogram(name), (k, v) -> {
            throw new IllegalStateException(name(prefix, name) + " already exists!");
        });
    }

    public Meter createMeter(String name) {
        return meters.merge(name, new Meter(name), (k, v) -> {
            throw new IllegalStateException(name(prefix, name) + " already exists!");
        });
    }

    public Counter createCounter(String name) {
        return counters.merge(name, new Counter(name), (k, v) -> {
            throw new IllegalStateException(name(prefix, name) + " already exists!");
        });
    }

    public void createGauge(String name, Gauge gauge) {
        gauges.merge(name, gauge, (k, v) -> {
            throw new IllegalStateException(name(prefix, name) + " already exists!");
        });
    }

    public StatsGroup scope(String scope) {
        String scopePrefix = name(prefix, scope);
        return scopes.computeIfAbsent(scopePrefix, k -> new StatsGroup(scopePrefix));
    }

    // TODO(Maithem): clean child scopes?
    public void unregisterScopes() {
        scopes.clear();
    }

    //TODO(Maithem): scan and filter/ rocksdb

    //TODO(Maithem): need to un-register (sequencer bootstrap?)

}
