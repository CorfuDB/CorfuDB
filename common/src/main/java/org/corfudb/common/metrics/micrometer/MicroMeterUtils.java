package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class MicroMeterUtils {

    private static final double[] PERCENTILES = new double[]{0.5, 0.99};
    private static final boolean PUBLISH_HISTOGRAM = true;

    private MicroMeterUtils() {

    }

    public static Optional<Timer> createOrGetTimer(String name, String... tags) {
        return MeterRegistryProvider.getInstance().map(registry ->
                Timer.builder(name)
                        .tags(tags)
                        .publishPercentileHistogram(PUBLISH_HISTOGRAM)
                        .publishPercentiles(PERCENTILES)
                        .register(registry));
    }

    private static Optional<DistributionSummary> createOrGetDistSummary(String name, String... tags) {
        return MeterRegistryProvider.getInstance().map(registry ->
                DistributionSummary.builder(name)
                        .tags(tags)
                        .publishPercentileHistogram(PUBLISH_HISTOGRAM)
                        .publishPercentiles(PERCENTILES)
                        .register(registry));
    }

    public static void time(Duration duration, String name, String... tags) {
        Optional<Timer> timer = createOrGetTimer(name, tags);
        timer.ifPresent(value -> value.record(duration));
    }

    public static void time(Runnable runnable, String name, String... tags) {
        Optional<Timer> timer = createOrGetTimer(name, tags);
        if (timer.isPresent()) {
            timer.get().record(runnable);
        }
        else{
            runnable.run();
        }
    }

    public static <T> T time(Supplier<T> supplier, String name, String... tags) {
        Optional<Timer> timer = createOrGetTimer(name, tags);
        return timer.map(value -> value.record(supplier)).orElseGet(supplier);
    }

    public static void time(Optional<Timer.Sample> maybeSample, String name, String... tags) {
        Optional<Timer> timer = createOrGetTimer(name, tags);
        timer.ifPresent(t -> maybeSample.ifPresent(s -> s.stop(t)));
    }

    public static Optional<Timer.Sample> startTimer() {
        return MeterRegistryProvider.getInstance().map(Timer::start);
    }

    public static void measure(double measuredValue, String name, String... tags) {
        Optional<DistributionSummary> summary = createOrGetDistSummary(name, tags);
        summary.ifPresent(s -> s.record(measuredValue));
    }

    public static <T> CompletableFuture<T> timeWhenCompletes(CompletableFuture<T> future,
                                                             Optional<Timer.Sample> maybeSample,
                                                             String timerName, String... tags) {
        if (timerName.isEmpty() || tags.length % 2 != 0) {
            throw new IllegalArgumentException("Name of the registered timer should be present and" +
                    " the number of tags should be even.");
        }
        Optional<MeterRegistry> maybeRegistry = MeterRegistryProvider.getInstance();
        if (maybeRegistry.isPresent() && maybeSample.isPresent()) {
            MeterRegistry meterRegistry = maybeRegistry.get();
            Timer.Sample sample = maybeSample.get();
            CompletableFuture<T> cf = new CompletableFuture<>();
            future.whenComplete((res, ex) -> {
                sample.stop(meterRegistry.timer(timerName, tags));
                if (ex != null) {
                    cf.completeExceptionally(ex);
                } else {
                    cf.complete(res);
                }
            });
            return cf;
        } else {
            return future;
        }
    }
}
