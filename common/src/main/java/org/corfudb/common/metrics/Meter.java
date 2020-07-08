package org.corfudb.common.metrics;

import lombok.Getter;

import java.util.concurrent.atomic.LongAdder;

/**
 *
 * Created by Maithem on 7/7/20.
 */

public class Meter {

    private final LongAdder values = new LongAdder();
    private final LongAdder count = new LongAdder();

    long startTime;
    long elapsed;
    long countSum;
    long valueSum;

    @Getter
    private final String name;

    public Meter(String name) {
        this.name = name;
    }

    public void snapshot() {
    }

    public void record(long value) {
        count.increment();
        values.add(value);
    }
}
