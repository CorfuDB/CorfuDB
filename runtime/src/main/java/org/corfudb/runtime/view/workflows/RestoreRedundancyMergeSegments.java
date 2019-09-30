package org.corfudb.runtime.view.workflows;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;

/**
 * Sending a workflow request to restore all redundancies and merge all segments.
 * Created by Zeeshan on 2019-02-06.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends WorkflowRequest {

    /**
     * Create a Merge segments workflow request.
     *
     * @param endpointToRestoreRedundancy Endpoint to restore redundancy to.
     * @param runtime                     Connected instance of Corfu runtime.
     * @param retry                       Number of retries.
     * @param timeout                     Workflow timeout.
     * @param pollPeriod                  Poll interval to check workflow status.
     */
    public RestoreRedundancyMergeSegments(@NonNull String endpointToRestoreRedundancy,
                                          @NonNull CorfuRuntime runtime,
                                          int retry,
                                          @NonNull Duration timeout,
                                          @NonNull Duration pollPeriod) {
        this.nodeForWorkflow = endpointToRestoreRedundancy;
        this.runtime = runtime;
        this.retry = retry;
        this.timeout = timeout;
        this.pollPeriod = pollPeriod;
    }

    /**
     * Creates and sends the workflow request to restore redundancy and merge the segments.
     *
     * @param managementClient Management Client to send the orchestrator request.
     * @return Workflow ID.
     * @throws TimeoutException thrown is orchestrator request was not acknowledged within the RPC timeout.
     */
    @Override
    protected UUID sendRequest(@NonNull ManagementClient managementClient) throws TimeoutException {
        CreateWorkflowResponse resp = managementClient.mergeSegments(nodeForWorkflow);
        log.info("sendRequest: request to restore redundancy and merge segments for endpoint {} on orchestrator {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort());
        return resp.getWorkflowId();
    }

    @Override
    protected Predicate<String> orchestratorSelector() {
        return node -> node.equals(nodeForWorkflow);
    }

    /**
     * Verify request has been completed if the layout contains only one segment.
     *
     * @param layout the layout to inspect
     * @return True if the layout has only one segment. False otherwise.
     */
    @Override
    protected boolean verifyRequest(@NonNull Layout layout) {
        log.info("verifyRequest: {} in {}", this, layout);
        return layout.getSegments().size() == 1;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }
}
