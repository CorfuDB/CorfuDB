package org.corfudb.common.metrics.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class MicroMeterUtils {

    private MicroMeterUtils() {

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
