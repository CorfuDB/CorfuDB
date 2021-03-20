package org.corfudb.runtime.collections;

import io.micrometer.core.instrument.Timer;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * List of metrics captured for a stream listener.
 * <p>
 * Created by WenbinZhu on 11/23/20.
 */
public class StreamSubscriptionMetrics {

    private final String listenerId;

    private final double[] percentiles;

    private final Optional<Timer> pollingTimer;

    private final Optional<Timer> deliveryTimer;

    StreamSubscriptionMetrics(CorfuRuntime runtime, StreamListener listener, String namespace, String streamTag) {
        this.listenerId = String.format("listener_%s_%s_%s", listener, namespace, streamTag);
        this.percentiles = new double[]{0.5, 0.95, 0.99};
        this.deliveryTimer = MeterRegistryProvider.getInstance()
                .map(registry -> Timer.builder("stream_sub.delivery.timer")
                        .tags("listenerId", listenerId)
                        .publishPercentiles(percentiles)
                        .publishPercentileHistogram(true)
                        .register(registry));
        this.pollingTimer = MeterRegistryProvider.getInstance()
                .map(registry -> Timer.builder("stream_sub.polling.timer")
                        .tags("listenerId", listenerId)
                        .publishPercentiles(percentiles)
                        .publishPercentileHistogram(true)
                        .register(registry));
    }

    public void recordDeliveryDuration(Runnable runnable) {
        if (deliveryTimer.isPresent()) {
            deliveryTimer.get().record(runnable);
        } else {
            runnable.run();
        }
    }

    public List<ILogData> recordPollingDuration(Callable<List<ILogData>> callable) throws Exception {
        if (pollingTimer.isPresent()) {
            return pollingTimer.get().recordCallable(callable);
        } else {
            return callable.call();
        }
    }
}
