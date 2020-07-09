package org.corfudb.common.metrics;

import lombok.Getter;

import java.util.concurrent.atomic.LongAdder;

/**
 *
 * Created by Maithem on 7/7/20.
 */
public class Counter {

    private final LongAdder count = new LongAdder();

    @Getter
    private final String name;

    public Counter(String name) {
        this.name = name;
    }

    public void inc() {
        count.increment();
    }

    public void dec() {
        count.decrement();
    }

    public void inc(long value) {
        count.add(value);
    }

    public long sumThenReset() {
        return count.sumThenReset();
    }
}
