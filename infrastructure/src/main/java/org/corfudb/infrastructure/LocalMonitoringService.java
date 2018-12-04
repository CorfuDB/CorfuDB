package org.corfudb.infrastructure;

import static org.corfudb.util.LambdaUtils.runSansThrow;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.concurrent.SingletonResource;

/**
 * Local Metrics Polling Service polls the local server state.
 * Created by zlokhandwala on 11/2/18.
 */
@Slf4j
class LocalMonitoringService implements MonitoringService {

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    private final ClusterStateContext clusterStateContext;

    private final ScheduledExecutorService pollingService;

    LocalMonitoringService(@NonNull ServerContext serverContext,
                           @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource,
                           @NonNull ClusterStateContext clusterStateContext) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.clusterStateContext = clusterStateContext;

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
        // This is an optimization. If this node is not the primary sequencer for the current
        // layout, there is no reason to request metrics from this sequencer.
        if (!layout.getPrimarySequencer().equals(serverContext.getLocalEndpoint())) {
            return SequencerMetrics.UNKNOWN;
        }

        SequencerMetrics sequencerMetrics;
        try {
            sequencerMetrics = CFUtils.getUninterruptibly(
                    getCorfuRuntime()
                            .getLayoutView().getRuntimeLayout(layout)
                            .getSequencerClient(serverContext.getLocalEndpoint())
                            .requestMetrics());
        } catch (ServerNotReadyException snre) {
            sequencerMetrics = SequencerMetrics.NOT_READY;
        } catch (Exception e) {
            log.error("Error while requesting metrics from the sequencer: ", e);
            sequencerMetrics = SequencerMetrics.UNKNOWN;
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
            log.debug("updateLocalMetrics: Cannot update local metrics. Management Server not bootstrapped.");
            return;
        }

        // Build and replace existing locally stored sequencerMetrics
        clusterStateContext.updateLocalServerMetrics(queryLocalSequencerMetrics(layout));
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
                TimeUnit.MILLISECONDS);
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

}
