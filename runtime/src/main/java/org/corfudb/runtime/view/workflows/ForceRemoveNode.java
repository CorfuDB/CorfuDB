package org.corfudb.runtime.view.workflows;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.UUID;

/**
 *
 * A workflow request that makes an orchestrator call to force remove a node from
 * the cluster.
 *
 * Created by Maithem on 1/19/18.
 */
@Slf4j
public class ForceRemoveNode extends RemoveNode {

    public ForceRemoveNode(@Nonnull String endpointToRemove, @Nonnull CorfuRuntime runtime,
                           @Nonnull int retry, @Nonnull Duration timeout,
                           @Nonnull Duration pollPeriod) {
        super(endpointToRemove, runtime, retry, timeout, pollPeriod);
    }

    @Override
    protected UUID sendRequest(@Nonnull Layout layout) {
        // Select the current tail node and send an add node request to the orchestrator
        CreateWorkflowResponse resp = getOrchestrator(layout).forceRemoveNode(nodeForWorkflow);
        log.info("sendRequest: requested to force remove {} on orchestrator {}:{}, layout {}",
                nodeForWorkflow, getOrchestrator(layout).getRouter().getHost(),
                getOrchestrator(layout).getRouter().getPort(), layout);
        return resp.getWorkflowId();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }
}
