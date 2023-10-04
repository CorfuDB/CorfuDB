package org.corfudb.runtime.view.workflows;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;


@Slf4j
public class HealNode extends WorkflowRequest {

    public HealNode(@NonNull String endpointToHeal, @NonNull CorfuRuntime runtime,
                    int retry, @NonNull Duration timeout,
                    @NonNull Duration pollPeriod) {
        this.nodeForWorkflow = endpointToHeal;
        this.runtime = runtime;
        this.retry = retry;
        this.timeout = timeout;
        this.pollPeriod = pollPeriod;
    }

    @Override
    protected UUID sendRequest(@NonNull ManagementClient managementClient) throws TimeoutException {
        CreateWorkflowResponse resp = managementClient
                .healNodeRequest(nodeForWorkflow, true, true, true, 0);

        log.info("sendRequest: requested to heal {} on {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort()
        );

        return resp.getWorkflowId();
    }

    @Override
    protected boolean verifyRequest(@NonNull Layout layout) {
        log.info("verifyRequest: {} in {}", this, layout);
        Set<LayoutSegment> nodeSegments = layout.getSegmentsForEndpoint(nodeForWorkflow);

        boolean isNodeInTheLayout = layout.getAllServers().contains(nodeForWorkflow);
        boolean segmentConsistency = nodeSegments.size() == layout.getSegments().size();

        return isNodeInTheLayout && segmentConsistency;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }

    @Override
    protected Optional<ManagementClient> getOrchestrator() {
        runtime.invalidateLayout();
        Layout layout = new Layout(runtime.getLayoutView().getLayout());
        BaseClient baseClient = runtime.getLayoutView().getRuntimeLayout(layout).getBaseClient(nodeForWorkflow);
        if (baseClient.pingSync()) {
            log.info("getOrchestrator: selected {}", nodeForWorkflow);
            return Optional.of(runtime.getLayoutView()
                    .getRuntimeLayout(layout).getManagementClient(nodeForWorkflow));
        } else {
            log.warn("getOrchestrator: a server {} is not responding to pings", nodeForWorkflow);
            return Optional.empty();
        }
    }
}
