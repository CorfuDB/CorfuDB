package org.corfudb.runtime.view.workflows;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 *
 * A workflow request that makes an orchestrator call to add a new node to
 * the cluster.
 *
 * Created by Maithem on 1/19/18.
 */
@Slf4j
public class AddNode extends WorkflowRequest {

    public AddNode(@Nonnull String endpointToAdd, @Nonnull CorfuRuntime runtime,
                   @Nonnull int retry, @Nonnull Duration timeout,
                   @Nonnull Duration pollPeriod) {
        this.nodeForWorkflow = endpointToAdd;
        this.runtime = runtime;
        this.retry = retry;
        this.timeout = timeout;
        this.pollPeriod = pollPeriod;
    }

    @Override
    protected UUID sendRequest(@Nonnull ManagementClient managementClient) throws TimeoutException {
        // Select the current tail node and send an add node request to the orchestrator
        CreateWorkflowResponse resp = managementClient.addNodeRequest(nodeForWorkflow);
        log.info("sendRequest: requested to add {} on orchestrator {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort());
        return resp.getWorkflowId();
    }

    @Override
    protected boolean verifyRequest(@Nonnull Layout layout) {
        // Verify that the node has been added and that the address space isn't
        // segmented
        log.info("verifyRequest: {} to {}", this, layout);
        return runtime.getLayoutView().getLayout()
                .getAllServers().contains(nodeForWorkflow)
                && layout.getSegmentsForEndpoint(nodeForWorkflow).size()
                == layout.getSegments().size();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }
}
