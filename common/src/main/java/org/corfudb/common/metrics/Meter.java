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

    private long startTime;
    private long elapsed;
    private long countSum;
    private long valueSum;

    @Getter
    private final String name;

    public Meter(String name) {
        this.name = name;
        this.startTime = System.currentTimeMillis();
    }

    public void snapshotAndReset() {
        elapsed = System.currentTimeMillis() - startTime;
        startTime = System.currentTimeMillis();
        countSum = count.sumThenReset();
        valueSum = values.sumThenReset();
    }

    public double getCountRate() {
        return (countSum * 1.0) / elapsed;
    }

    public double getValueRate() {
        return (countSum * 1.0) / elapsed;
    }

    public void record(long value) {
        count.increment();
        values.add(value);
    }
}
