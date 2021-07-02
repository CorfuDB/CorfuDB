package org.corfudb.common.metrics.micrometer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TestReporter extends ScheduledReporter {

    public TestReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
    }

    class Stats {
        public final long count;
        public final double median;
        public final double p99;

        public Stats(long count, double median, double p99) {
            this.count = count;
            this.median = median;
            this.p99 = p99;
        }

        public String toString() {
            return "Counts: " + count + " Median: " + median + " 99p: " + p99;
        }
    }

    private Optional<Stats> getTimerStats(String key, SortedMap<String, Timer> timers) {
        if (timers.containsKey(key)) {
            Timer timer = timers.get(key);
            long count = timer.getCount();
            Snapshot snapshot = timer.getSnapshot();
            double median = snapshot.getMedian() / 1_000_000;
            double p99 = snapshot.get99thPercentile() / 1_000_000;
            return Optional.of(new Stats(count, median, p99));
        }
        return Optional.empty();
    }

    private Optional<Stats> getHistogramStats(String key, SortedMap<String, Histogram> histograms) {
        if (histograms.containsKey(key)) {
            Histogram histogram = histograms.get(key);
            long count = histogram.getCount();
            Snapshot snapshot = histogram.getSnapshot();
            double median = snapshot.getMedian();
            double p99 = snapshot.get99thPercentile();
            return Optional.of(new Stats(count, median, p99));
        }
        return Optional.empty();
    }


    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            Random rand = new Random();
            int index = rand.nextInt();
            return "UNKNOWN-" + index;
        }
    }

    private void createLogUnitMetricsMsg(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        String writeTimerKey = "logunitWriteTimer.type.single";
        Optional<Stats> writeTimerStats = getTimerStats(writeTimerKey, timers);
        String readTimerKey = "logunitReadTimer";
        Optional<Stats> readTimerStats = getTimerStats(readTimerKey, timers);
        log.info("{}", readTimerStats);
        String fsyncTimerKey = "logunitFsyncTimer";
        Optional<Stats> fsyncTimerStats = getTimerStats(fsyncTimerKey, timers);
        log.info("{}", fsyncTimerStats);
        String writeThroughputKey = "logunitWriteThroughput";
        Optional<Stats> writeThroughputStats = getHistogramStats(writeThroughputKey, histograms);
        log.info("{}", writeThroughputStats);
        String readThroughputKey = "logunitReadThroughput";
        Optional<Stats> readThroughputStats = getHistogramStats(readThroughputKey, histograms);
        log.info("{}", readThroughputStats);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        // createLogUnitMetricsMsg(gauges, counters, histograms, meters, timers);
        System.out.println(getHostName());
    }
}
