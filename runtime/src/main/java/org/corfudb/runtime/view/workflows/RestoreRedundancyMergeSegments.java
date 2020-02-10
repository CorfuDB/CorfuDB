package org.corfudb.runtime.view.workflows;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

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

    /**
     * Verify request has been completed if the layout contains only one segment.
     *
     * @param layout the layout to inspect
     * @return True if the layout has only one segment. False otherwise.
     */
    @Override
    protected boolean verifyRequest(@NonNull Layout layout) {
        log.info("verifyRequest: {} in {}", this, layout);
        return layout.getAllServers().contains(nodeForWorkflow)
                && layout.getSegmentsForEndpoint(nodeForWorkflow).size()
                == layout.getSegments().size();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }

    @Override
    protected Optional<ManagementClient> getOrchestrator(){
        runtime.invalidateLayout();
        Layout layout = new Layout(runtime.getLayoutView().getLayout());
        BaseClient baseClient = runtime.getLayoutView().getRuntimeLayout(layout).getBaseClient(nodeForWorkflow);
        if(baseClient.pingSync()){
            log.info("getOrchestrator: orchestrator selected {}", nodeForWorkflow);
            return Optional.of(runtime.getLayoutView()
                    .getRuntimeLayout(layout).getManagementClient(nodeForWorkflow));
        }
        else{
            log.warn("getOrchestrator: a server {} is not responding to pings", nodeForWorkflow);
            return Optional.empty();
        }
    }
}
