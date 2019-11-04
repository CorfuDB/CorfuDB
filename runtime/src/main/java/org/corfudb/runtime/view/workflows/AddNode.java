package org.corfudb.runtime.view.workflows;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * A workflow request that makes an orchestrator call to add a new node to
 * the cluster.
 * <p>
 * Created by Maithem on 1/19/18.
 */
@Slf4j
public class AddNode extends WorkflowRequest {

    /**
     * Number of retries to ping a base server before selecting an orchestrator.
     */
    private final int pingRetries = 3;
    /**
     * Duration between the pings.
     */
    private final Duration pingInterval = Duration.ofMillis(50);

    public AddNode(@NonNull String endpointToAdd, @NonNull CorfuRuntime runtime,
                   int retry, @NonNull Duration timeout,
                   @NonNull Duration pollPeriod) {
        this.nodeForWorkflow = endpointToAdd;
        this.runtime = runtime;
        this.retry = retry;
        this.timeout = timeout;
        this.pollPeriod = pollPeriod;
    }

    @Override
    protected UUID sendRequest(@NonNull ManagementClient managementClient) throws TimeoutException {
        // Bootstrap a management server first.
        Layout layout = new Layout(runtime.getLayoutView().getLayout());
        runtime.getManagementView().bootstrapManagementServer(nodeForWorkflow, layout).join();
        // Send the add node request to the node's orchestrator.
        CreateWorkflowResponse resp = managementClient.addNodeRequest(nodeForWorkflow);
        log.info("sendRequest: requested to add {} on orchestrator {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort());
        return resp.getWorkflowId();
    }

    @Override
    protected boolean verifyRequest(@NonNull Layout layout) {
        // Verify that the node has been added
        log.info("verifyRequest: {} to {}", this, layout);
        return layout.getAllServers().contains(nodeForWorkflow)
                && layout.getSegmentsForEndpoint(nodeForWorkflow).size()
                == layout.getSegments().size();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }

    @Override
    protected Optional<ManagementClient> getOrchestrator() {
        BaseClient baseClient = runtime.getLayoutView().getRuntimeLayout().getBaseClient(nodeForWorkflow);
        // Add node requires a few pings for orchestrator selection because the current
        // server router might not be immediately ready after the recent shutdown.
        for (int i = 0; i < pingRetries; i++) {
            if (baseClient.pingSync()) {
                log.info("getOrchestrator: orchestrator selected {}", nodeForWorkflow);
                return Optional.of(runtime.getLayoutView()
                        .getRuntimeLayout().getManagementClient(nodeForWorkflow));
            }
            Sleep.sleepUninterruptibly(pingInterval);
            log.info("Retrying to ping a current base server {} times.", i);
        }
        return Optional.empty();

    }
}
