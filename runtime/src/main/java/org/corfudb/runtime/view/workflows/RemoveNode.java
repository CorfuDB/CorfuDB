package org.corfudb.runtime.view.workflows;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * A workflow request that makes an orchestrator call to remove a node from
 * the cluster.
 * <p>
 * Created by Maithem on 1/19/18.
 */
@Slf4j
public class RemoveNode extends WorkflowRequest {

    public RemoveNode(@NonNull String endpointToRemove, @NonNull CorfuRuntime runtime,
                      int retry, @NonNull Duration timeout,
                      @NonNull Duration pollPeriod) {
        this.nodeForWorkflow = endpointToRemove;
        this.runtime = runtime;
        this.retry = retry;
        this.timeout = timeout;
        this.pollPeriod = pollPeriod;
    }

    @Override
    protected UUID sendRequest(@NonNull ManagementClient managementClient) throws TimeoutException {
        // Send an remove node request to an orchestrator that is not on the node
        // to be removed
        CreateWorkflowResponse resp = managementClient.removeNode(nodeForWorkflow);
        log.info("sendRequest: requested to remove {} on orchestrator {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort());
        return resp.getWorkflowId();
    }

    @Override
    protected boolean verifyRequest(@NonNull Layout layout) {
        log.info("verifyRequest: {} from {}", this, layout);
        // Verify that the new layout doesn't include the removed node
        return !layout.getAllServers().contains(nodeForWorkflow);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }
}
