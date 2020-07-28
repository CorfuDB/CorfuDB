package org.corfudb.common.metrics;

import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Created by Maithem on 7/7/20. */
public class StatsGroup {

  @Getter private final String prefix;

  @Getter private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();

  @Getter private final Map<String, Meter> meters = new ConcurrentHashMap<>();

  @Getter private final Map<String, Counter> counters = new ConcurrentHashMap<>();

  @Getter private final Map<String, Gauge> gauges = new ConcurrentHashMap<>();

  @Getter private final Map<String, StatsGroup> scopes = new ConcurrentHashMap<>();

  public StatsGroup(String prefix) {
    this.prefix = prefix;
  }

  private String name(String prefix, String name) {
    if (prefix.isEmpty()) {
      return name;
    }

    return prefix + "_" + name;
  }

  private <T extends Metric> T create(Map<String, T> map, T metric)
      throws IllegalArgumentException {
    return map.merge(
        metric.getName(),
        metric,
        (k, v) -> {
          throw new IllegalArgumentException(name(prefix, metric.getName()) + " already exists!");
        });
  }

  public Histogram createHistogram(String name) {
    return create(histograms, new Histogram(name));
  }

  public Meter createMeter(String name) {
    return create(meters, new Meter(name));
  }

  public Counter createCounter(String name) {
    return create(counters, new Counter(name));
  }

  public void createGauge(Gauge gauge) {
    create(gauges, gauge);
  }

  public StatsGroup scope(String scope) {
    String scopePrefix = name(prefix, scope);
    return scopes.computeIfAbsent(scopePrefix, k -> new StatsGroup(scopePrefix));
  }

  // TODO(Maithem): clean child scopes?
  public void unregisterChildScopes() {
    scopes.clear();
  }

  // TODO(Maithem): scan and filter/ rocksdb

  // TODO(Maithem): need to un-register (sequencer bootstrap?)

}
