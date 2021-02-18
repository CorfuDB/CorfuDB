package org.corfudb.common.metrics.micrometer.registries;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import org.corfudb.common.metrics.micrometer.IntervalLoggingConfig;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class LoggingMeterRegistryTest {

    static class TestTimer implements Timer {

        @Override
        public void record(long amount, TimeUnit unit) {

        }

        @Override
        public <T> T record(Supplier<T> f) {
            return null;
        }

        @Override
        public <T> T recordCallable(Callable<T> f) throws Exception {
            return null;
        }

        @Override
        public void record(Runnable f) {

        }

        @Override
        public long count() {
            return 100;
        }

        @Override
        public double totalTime(TimeUnit unit) {
            return 200;
        }

        @Override
        public double max(TimeUnit unit) {
            return 300;
        }

        @Override
        public TimeUnit baseTimeUnit() {
            return TimeUnit.MILLISECONDS;
        }

        @Override
        public HistogramSnapshot takeSnapshot() {
            return null;
        }

        @Override
        public Id getId() {
            return new Meter.Id("metric", Tags.of("endpoint", "localhost:9000"), null, null, Type.TIMER);
        }
    }

    static class TestSummary implements DistributionSummary {

        @Override
        public void record(double amount) {

        }

        @Override
        public long count() {
            return 100;
        }

        @Override
        public double totalAmount() {
            return 200;
        }

        @Override
        public double max() {
            return 300;
        }

        @Override
        public HistogramSnapshot takeSnapshot() {
            return null;
        }

        @Override
        public Id getId() {
            return new Meter.Id("metric", Tags.of("endpoint", "localhost:9000"), null, null, Type.DISTRIBUTION_SUMMARY);
        }
    }

    static class AggregateSink implements Consumer<String> {

        private final List<String> lines = new ArrayList<>();

        public boolean substringIsPresent(String substring) {
            for (String line : lines) {
                if (line.contains(substring)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void accept(String s) {
            lines.add(s);
        }
    }

    static LoggingMeterRegistryWithHistogramSupport getInstance() {
        LoggingRegistryConfig config = new IntervalLoggingConfig(Duration.ofSeconds(1));
        return new LoggingMeterRegistryWithHistogramSupport(config, value -> {
        });
    }

    static LoggingMeterRegistryWithHistogramSupport getInstance(Consumer<String> consumer) {
        LoggingRegistryConfig config = new IntervalLoggingConfig(Duration.ofSeconds(1));
        return new LoggingMeterRegistryWithHistogramSupport(config, consumer::accept);
    }


    @Test
    public void testWriteGauge() {
        Meter.Id id = new Meter.Id("metric", Tags.of("endpoint", "localhost:9000"),
                null, null, Meter.Type.GAUGE);
        LoggingMeterRegistryWithHistogramSupport registry = getInstance();
        Stream<String> stream = registry.writeGauge(id, 20.0);
        String line = stream.findFirst().orElseThrow(IllegalArgumentException::new);
        assertTrue(line.contains("metric,endpoint=localhost:9000,metric_type=gauge value=20"));
    }

    @Test
    public void testWriteCounter() {
        Meter.Id id = new Meter.Id("metric", Tags.of("endpoint", "localhost:9000"),
                null, null,
                Meter.Type.COUNTER);
        LoggingMeterRegistryWithHistogramSupport registry = getInstance();
        Stream<String> stream = registry.writeCounter(id, 30);
        String line = stream.findFirst().orElseThrow(IllegalArgumentException::new);
        assertTrue(line.contains("metric,endpoint=localhost:9000,metric_type=counter value=30"));
    }

    @Test
    public void testWriteTimer() {
        LoggingMeterRegistryWithHistogramSupport registry = getInstance();
        Timer timer = new TestTimer();
        String line = registry.writeTimer(timer).findFirst().orElseThrow(IllegalArgumentException::new);
        assertTrue(line.contains("metric,endpoint=localhost:9000,metric_type=timer sum=200,count=100,mean=2,upper=300"));
    }

    @Test
    public void testWriteSummary() {
        LoggingMeterRegistryWithHistogramSupport registry = getInstance();
        TestSummary summary = new TestSummary();
        String line = registry.writeSummary(summary).findFirst().orElseThrow(IllegalArgumentException::new);
        assertTrue(line.contains("metric,endpoint=localhost:9000,metric_type=summary sum=200,count=100,mean=2,upper=300"));
    }

    @Test
    public void testTimerPercentiles() {
        AggregateSink sink = new AggregateSink();

        LoggingMeterRegistryWithHistogramSupport registry = getInstance(sink);

        Timer timer = Timer.builder("timer")
                .publishPercentileHistogram()
                .publishPercentiles(0.99, 0.95, 0.5)
                .tags("endpoint", "localhost:9000")
                .register(registry);
        for (int i = 0; i < 3; i++) {
            timer.record(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {

                }
            });
        }
        assertTrue(sink.substringIsPresent("timer_percentile,endpoint=localhost:9000,phi=0.99,metric_type=gauge"));
        assertTrue(sink.substringIsPresent("timer_percentile,endpoint=localhost:9000,phi=0.95,metric_type=gauge"));
        assertTrue(sink.substringIsPresent("timer_percentile,endpoint=localhost:9000,phi=0.5,metric_type=gauge"));
    }

    @Test
    public void testSummaryPercentiles() {
        AggregateSink sink = new AggregateSink();

        LoggingMeterRegistryWithHistogramSupport registry = getInstance(sink);

        DistributionSummary summary = DistributionSummary.builder("summary")
                .publishPercentileHistogram()
                .publishPercentiles(0.99, 0.95, 0.5)
                .tags("endpoint", "localhost:9000")
                .register(registry);

        for (int i = 0; i < 3; i++) {
            summary.record(100);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {

            }
        }
        assertTrue(sink.substringIsPresent("summary_percentile,endpoint=localhost:9000,phi=0.99,metric_type=gauge value=100"));
        assertTrue(sink.substringIsPresent("summary_percentile,endpoint=localhost:9000,phi=0.95,metric_type=gauge value=100"));
        assertTrue(sink.substringIsPresent("summary_percentile,endpoint=localhost:9000,phi=0.5,metric_type=gauge value=100"));
    }

}
