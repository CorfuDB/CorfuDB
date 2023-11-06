package org.corfudb.common.metrics.micrometer;

import com.google.common.collect.ImmutableSet;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider.MetricType;
import org.corfudb.common.util.Tuple;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

public class MicroMeterUtils {

    private static final double[] PERCENTILES = new double[]{0.5, 0.75, 0.95, 0.99};
    private static final boolean PUBLISH_HISTOGRAM = true;
    /**
     * A list of server metrics that will be ignored.
     */
    private static final Set<String> serverMetricsBlockList = ImmutableSet.of(
            "corfu.infrastructure.message-handler.bootstrap_management_request",
            "corfu.infrastructure.message-handler.bootstrap_sequencer_request",
            "corfu.infrastructure.message-handler.committed_tail_request",
            "corfu.infrastructure.message-handler.inspect_addresses_request",
            "corfu.infrastructure.message-handler.query_node_request",
            "corfu.infrastructure.message-handler.write_log_request",
            "corfu.infrastructure.message-handler.read_log_request",
            "corfu.infrastructure.message-handler.sequencer_metrics_request",
            "corfu.infrastructure.message-handler.trim_mark_request",
            "corfu.infrastructure.message-handler.update_committed_tail_request",
            "corfu.infrastructure.message-handler.write_log_request",
            "corfu.infrastructure.message-handler.range_write_log_request",
            "address_space.read.latency",
            "address_space.write.latency"
    );

    /**
     * A list of client metrics that will be ignored.
     */
    private static final Set<String> clientMetricsBlockList = ImmutableSet.of(
            "openTable",
            "vlo.sync.timer",
            "vlo.write.timer",
            "multi.object.smrentry.serialize.stream",
            "multi.object.smrentry.serialize.stream.size",
            "multi.object.smrentry.serialize.stream.updates",
            "multi.object.smrentry.deserialize.stream",
            "multi.object.smrentry.deserialize.stream.size",
            "multi.object.smrentry.deserialize.stream.lazy",
            "logdata.compress",
            "logdata.decompress"
    );


    private MicroMeterUtils() {

    }

    private static Optional<Set<String>> getMetricsBlockList() {
        return MeterRegistryProvider.getMetricType().map(type -> {
            if (type == MetricType.CLIENT) {
                return clientMetricsBlockList;
            } else if (type == MetricType.SERVER) {
                return serverMetricsBlockList;
            }
            throw new IllegalArgumentException("Unsupported metrics type");
        });
    }

    private static Optional<MeterRegistry> filterGetInstance(String name) {
        return getMetricsBlockList()
                .filter(list -> !list.contains(name))
                .flatMap(list -> MeterRegistryProvider.getInstance());
    }

    public static Optional<Timer> createOrGetTimer(String name, String... tags) {
        return filterGetInstance(name).map(registry ->
                Timer.builder(name)
                        .tags(tags)
                        .publishPercentileHistogram(PUBLISH_HISTOGRAM)
                        .publishPercentiles(PERCENTILES)
                        .register(registry));
    }

    private static Optional<DistributionSummary> createOrGetDistSummary(String name, String... tags) {
        return filterGetInstance(name).map(registry ->
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
        } else {
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

    /**
     * Start sampling conditionally, based on the given rate
     *
     * @param shouldSample start timer if this value is true
     * @return a sample to be used with the timer
     */
    public static Optional<Timer.Sample> startTimer(boolean shouldSample) {
        if (shouldSample) {
            return MeterRegistryProvider.getInstance().map(Timer::start);
        }
        return Optional.empty();
    }

    public static void measure(double measuredValue, String name, String... tags) {
        Optional<DistributionSummary> summary = createOrGetDistSummary(name, tags);
        summary.ifPresent(s -> s.record(measuredValue));
    }

    public static <T> Optional<T> gauge(String name, T state, ToDoubleFunction<T> valueFunction, String... tags) {
        return filterGetInstance(name).map(registry -> {
            Gauge.builder(name, state, valueFunction)
                    .tags(tags)
                    .strongReference(true)
                    .register(registry);
            return state;
        });
    }

    public static <T extends Number> Optional<T> gauge(String name, T state, String... tags) {
        return filterGetInstance(name).map(registry -> {
            Gauge.builder(name, state, Number::doubleValue)
                    .tags(tags)
                    .strongReference(true)
                    .register(registry);
            return state;
        });
    }

    private static void removeMeters(List<Tuple<String, Tags>> ids, Meter.Type meterType) {
        MeterRegistryProvider
                .getTagId()
                .ifPresent(tid ->
                        ids.forEach(id -> {
                            String name = id.first;
                            Tags tags = id.second;
                            Tags allTags = tags.and(tid);
                            Meter.Id meterId = new Meter.Id(name, allTags,
                                    null, null, meterType);
                            MeterRegistryProvider.deregisterByMeterId(meterId);
                        }));
    }

    public static void removeGauges(List<Tuple<String, Tags>> gids) {
        removeMeters(gids, Meter.Type.GAUGE);
    }

    public static void removeGaugesWithNoTags(String... metricNames) {
        MicroMeterUtils.removeGauges(
                Arrays
                        .stream(metricNames)
                        .map(name ->
                                Tuple.of(name, Tags.empty()))
                        .collect(Collectors.toList())
        );
    }

    public static Optional<Counter> counter(String name, String... tags) {
        return filterGetInstance(name).map(registry -> registry.counter(name, tags));
    }

    public static void counterIncrement(double value, String name, String... tags) {
        filterGetInstance(name).ifPresent(registry -> registry.counter(name, tags).increment(value));
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
