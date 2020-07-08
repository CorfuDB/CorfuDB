package org.corfudb.common.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Created by Maithem on 7/7/20.
 */
public class StatsCollector {

    Map<String, StatsGroup> statsGroups = new ConcurrentHashMap<>();

    public StatsCollector() {
    }

    public void register(StatsGroup statsGroup) {
        statsGroups.merge(statsGroup.getPrefix(), statsGroup, (k, v) -> {
            throw new IllegalStateException(statsGroup.getPrefix() + " already exists!");
        });
    }

    // Report
    // Raise Alarm ?
}
