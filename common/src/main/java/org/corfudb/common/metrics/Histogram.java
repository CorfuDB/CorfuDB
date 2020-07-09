package org.corfudb.common.metrics;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

/**
 *
 * Created by Maithem on 7/7/20.
 */

@Slf4j
public class Histogram {

    private static final long MAX_TRACKED_VALUE = TimeUnit.HOURS.toMillis(1);

    private final Recorder recorder = new Recorder(MAX_TRACKED_VALUE, 5);

    private org.HdrHistogram.Histogram snapshot = null;

    @Getter
    private final String name;

    public Histogram(String name) {
        this.name = name;
        snapshot = recorder.getIntervalHistogram(snapshot);
    }

    public void snapshotAndReset() {
        snapshot = recorder.getIntervalHistogram(snapshot);
        //TODO(Maithem): Some data points are lost between the snapshot and the reset
        //TODO(Maithem): perf implication of reset ? (internal locking)
        recorder.reset();
    }

    public long getCount() {
        return snapshot.getTotalCount();
    }

    public long getMin() {
        return snapshot.getMinValue();
    }

    public long getMax() {
        return snapshot.getMaxValue();
    }

    public double getMean() {
        return snapshot.getMean();
    }

    public double getMedian() {
        return snapshot.getValueAtPercentile(50);
    }

    public double get75thPercentile() {
        return snapshot.getValueAtPercentile(50);
    }

    public double get95thPercentile() {
        return snapshot.getValueAtPercentile(95);
    }

    public double get99thPercentile() {
        return snapshot.getValueAtPercentile(99);
    }

    public void recordNs(long value) {
        recordMs(TimeUnit.NANOSECONDS.toMillis(value));
    }

    public void recordMs(long value) {
        if (value > MAX_TRACKED_VALUE) {
            log.error("Histogram[{}] value to large {}", name, value);
            return;
        }
        recorder.recordValue(value);
    }
}
