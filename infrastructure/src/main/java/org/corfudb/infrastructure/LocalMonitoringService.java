package org.corfudb.infrastructure;

import static org.corfudb.util.LambdaUtils.runSansThrow;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Local Metrics Polling Service polls the local server state.
 * Created by zlokhandwala on 11/2/18.
 */
@Slf4j
class LocalMonitoringService implements ManagementService {
    private static final CompletableFuture<SequencerMetrics> UNKNOWN = CompletableFuture
            .completedFuture(SequencerMetrics.UNKNOWN);

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;

    private final ScheduledExecutorService pollingService;

    private final AtomicReference<CompletableFuture<SequencerMetrics>> sequencerMetricsHolder;

    LocalMonitoringService(@NonNull ServerContext serverContext,
                           @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        sequencerMetricsHolder = new AtomicReference<>(UNKNOWN);

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
    private CompletableFuture<SequencerMetrics> queryLocalSequencerMetrics(Layout layout) {
        // This is an optimization. If this node is not the primary sequencer for the current
        // layout, there is no reason to request metrics from this sequencer.
        if (!layout.getPrimarySequencer().equals(serverContext.getLocalEndpoint())) {
            return UNKNOWN;
        }


        return getCorfuRuntime()
                .getLayoutView()
                .getRuntimeLayout(layout)
                .getSequencerClient(serverContext.getLocalEndpoint())
                .requestMetrics()
                //Handle possible exceptions and transform to the sequencer status
                .exceptionally(ex -> {
                    if (ex.getCause() instanceof ServerNotReadyException) {
                        return SequencerMetrics.NOT_READY;
                    }

                    log.error("Error while requesting metrics from the sequencer: ", ex);
                    return SequencerMetrics.UNKNOWN;
                });
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
            log.debug("updateLocalMetrics: Cannot update local metrics. Management Server not bootstrapped.");
            return;
        }

        // Build and replace existing locally stored sequencerMetrics
        sequencerMetricsHolder.set(queryLocalSequencerMetrics(layout));
    }

    /**
     * Runs the task to periodically poll the local server metrics.
     */
    @Override
    public void start(Duration monitoringInterval) {
        // Initiating periodic task to poll for failures.
        pollingService.scheduleAtFixedRate(
                () -> runSansThrow(this::updateLocalMetrics),
                0,
                monitoringInterval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Shutsdown the local metrics polling executor service.
     */
    @Override
    public void shutdown() {
        // Shutting the local metrics polling task.
        pollingService.shutdownNow();
        log.info("Local Metrics Polling Task shutting down.");
    }

    public CompletableFuture<SequencerMetrics> getMetrics() {
        return sequencerMetricsHolder.get();
    }

}
