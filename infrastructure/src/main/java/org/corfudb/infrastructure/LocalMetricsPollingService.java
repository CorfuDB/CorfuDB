package org.corfudb.infrastructure;

import static org.corfudb.util.LambdaUtils.runSansThrow;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.protocols.wireprotocol.ServerMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.concurrent.SingletonResource;

@Slf4j
class LocalMetricsPollingService implements IService {

    private final ServerContext serverContext;

    //  Locally collected server metrics polling interval.
    private static final Duration METRICS_POLL_INTERVAL = Duration.ofMillis(3000);

    private final ScheduledExecutorService pollingService;

    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;

    //  Local copy of the local node's server metrics.
    @Getter
    private volatile ServerMetrics localServerMetrics;

    LocalMetricsPollingService(@NonNull ServerContext serverContext,
                               @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        localServerMetrics = new ServerMetrics(NodeLocator.parseString(
                serverContext.getLocalEndpoint()),
                new SequencerMetrics(SequencerStatus.UNKNOWN));
        this.pollingService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "LocalMetricsPolling")
                        .build());
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    /**
     * Queries the local Sequencer Server for SequencerMetrics. This returns UNKNOWN if unable
     * to fetch the status.
     *
     * @param layout Current Management layout.
     * @return Sequencer Metrics.
     */
    private SequencerMetrics queryLocalSequencerMetrics(Layout layout) {
        SequencerMetrics sequencerMetrics;
        // This is an optimization. If this node is not the primary sequencer for the current
        // layout, there is no reason to request metrics from this sequencer.
        if (layout.getSequencers().get(0).equals(serverContext.getLocalEndpoint())) {
            try {
                sequencerMetrics = CFUtils.getUninterruptibly(
                        getCorfuRuntime()
                                .getLayoutView().getRuntimeLayout(layout)
                                .getSequencerClient(serverContext.getLocalEndpoint())
                                .requestMetrics());
            } catch (ServerNotReadyException snre) {
                sequencerMetrics = new SequencerMetrics(SequencerStatus.NOT_READY);
            } catch (Exception e) {
                log.error("Error while requesting metrics from the sequencer: ", e);
                sequencerMetrics = new SequencerMetrics(SequencerStatus.UNKNOWN);
            }
        } else {
            sequencerMetrics = new SequencerMetrics(SequencerStatus.UNKNOWN);
        }
        return sequencerMetrics;
    }

    /**
     * Task to collect local node metrics.
     * This task collects the following:
     * Boolean Status of layout, sequencer and logunit servers.
     * These metrics are then composed into ServerMetrics model and stored locally.
     */
    private void updateLocalMetrics() {

        // Initializing the status of components
        // No need to poll unless node is bootstrapped.
        Layout layout = serverContext.copyManagementLayout();
        if (layout == null) {
            return;
        }

        // Build and replace existing locally stored nodeMetrics
        localServerMetrics = new ServerMetrics(NodeLocator
                .parseString(serverContext.getLocalEndpoint()), queryLocalSequencerMetrics(layout));
    }

    @Override
    public void runTask() {
        // Initiating periodic task to poll for failures.
        pollingService.scheduleAtFixedRate(
                () -> runSansThrow(this::updateLocalMetrics),
                0,
                METRICS_POLL_INTERVAL.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        // Shutting the local metrics polling task.
        pollingService.shutdownNow();

        try {
            pollingService.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(),
                    TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.debug("localMetricsPollingService awaitTermination interrupted : {}", ie);
            Thread.currentThread().interrupt();
        }
        log.info("Local Metrics Polling Task shutting down.");
    }

}
