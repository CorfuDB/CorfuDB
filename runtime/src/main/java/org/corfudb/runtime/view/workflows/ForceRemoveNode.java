package org.corfudb.runtime.view.workflows;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 *
 * A workflow request that makes an orchestrator call to force remove a node from
 * the cluster.
 *
 * Created by Maithem on 1/19/18.
 */
@Slf4j
public class ForceRemoveNode extends RemoveNode {

    public ForceRemoveNode(@NonNull String endpointToRemove, @NonNull CorfuRuntime runtime,
                           int retry, @NonNull Duration timeout,
                           @NonNull Duration pollPeriod) {
        super(endpointToRemove, runtime, retry, timeout, pollPeriod);
    }

    @Override
    protected UUID sendRequest(@NonNull ManagementClient managementClient) throws TimeoutException {
        // Select the current tail node and send an add node request to the orchestrator
        CreateWorkflowResponse resp = managementClient.forceRemoveNode(nodeForWorkflow);
        log.info("sendRequest: requested to force remove {} on {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort());
        return resp.getWorkflowId();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }
}
