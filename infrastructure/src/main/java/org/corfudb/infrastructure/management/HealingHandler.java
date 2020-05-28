package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableSet;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Builder
@Slf4j
public class HealingHandler {
    @NonNull
    private final ServerContext serverContext;
    @NonNull
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    @NonNull
    private final ClusterAdvisor advisor;

    /**
     * Handle healed node.
     * Cluster advisor provides healed node based on current cluster state if healed node found then
     * save healed node in the history and send a message with the detected healed node
     * to the relevant management server.
     *
     * @param pollReport poll report
     * @param layout     current layout
     */
    public RemoteMonitoringService.DetectorTask detectHealing(PollReport pollReport, Layout layout) {
        log.trace("Handle healing, layout: {}", layout);

        Optional<NodeRank> healed = advisor.healedServer(pollReport.getClusterState());

        //Transform Optional value to a Set
        Set<String> healedNodes = healed
                .map(NodeRank::getEndpoint)
                .map(ImmutableSet::of)
                .orElse(ImmutableSet.of());

        if (healedNodes.isEmpty()) {
            log.trace("Nothing to heal");
            return RemoteMonitoringService.DetectorTask.SKIPPED;
        }

        //save history
        FailureDetectorMetrics history = FailureDetectorMetrics.builder()
                .localNode(serverContext.getLocalEndpoint())
                .graph(advisor.getGraph(pollReport.getClusterState()).connectivityGraph())
                .healed(healed.get())
                .action(FailureDetectorMetrics.FailureDetectorAction.HEAL)
                .unresponsiveNodes(layout.getUnresponsiveServers())
                .layout(layout.getLayoutServers())
                .epoch(layout.getEpoch())
                .build();

        log.info("Handle healing. Failure detector state: {}", history.toJson());

        try {
            CorfuRuntime corfuRuntime = getCorfuRuntime();

            corfuRuntime.getLayoutView()
                    .getRuntimeLayout(layout)
                    .getManagementClient(serverContext.getLocalEndpoint())
                    //handle healing asynchronously
                    .handleHealing(pollReport.getPollEpoch(), healedNodes)
                    //completable future: wait this future to complete and get result
                    .get();

            serverContext.saveFailureDetectorMetrics(history);

            log.info("Healing local node successful: {}", history.toJson());

            return RemoteMonitoringService.DetectorTask.COMPLETED;
        } catch (ExecutionException ee) {
            log.error("Healing local node failed: ", ee);
        } catch (InterruptedException ie) {
            log.error("Healing local node interrupted: ", ie);
            throw new UnrecoverableCorfuInterruptedError(ie);
        }

        return RemoteMonitoringService.DetectorTask.NOT_COMPLETED;
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }
}
