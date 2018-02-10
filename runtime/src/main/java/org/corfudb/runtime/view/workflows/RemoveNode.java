package org.corfudb.runtime.view.workflows;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.UUID;

/**
 * A workflow request that makes an orchestrator call to remove a node from
 * the cluster.
 * <p>
 * Created by Maithem on 1/19/18.
 */
@Slf4j
public class RemoveNode extends WorkflowRequest {

    public RemoveNode(@Nonnull String endpointToRemove, @Nonnull CorfuRuntime runtime,
                      int retry, @Nonnull Duration timeout,
                      @Nonnull Duration pollPeriod) {
        this.nodeForWorkflow = endpointToRemove;
        this.runtime = runtime;
        this.retry = retry;
        this.timeout = timeout;
        this.pollPeriod = pollPeriod;
    }

    @Override
    protected UUID sendRequest(@Nonnull Layout layout) {
        // Send an remove node request to an orchestrator that is not on the node
        // to be removed

        CreateWorkflowResponse resp = getOrchestrator(layout).removeNode(nodeForWorkflow);
        log.info("sendRequest: requested to remove {} on orchestrator {}:{}, layout {}",
                nodeForWorkflow, getOrchestrator(layout).getRouter().getHost(),
                getOrchestrator(layout).getRouter().getPort(), layout);
        return resp.getWorkflowId();
    }

    @Override
    protected boolean verifyRequest(@Nonnull Layout layout) {
        // Verify that the new layout doesn't include the removed node
        return !runtime.getLayoutView().getLayout().getAllServers()
                .contains(nodeForWorkflow);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }
}
