package org.corfudb.runtime.view.workflows;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 *
 * A workflow request that makes an orchestrator call to add a new node to
 * the cluster.
 *
 * Created by Maithem on 1/19/18.
 */
@Slf4j
public class AddNode extends WorkflowRequest{

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
        CompletableFuture<Boolean> bootStrapFuture = runtime.getManagementView()
                .bootstrapManagementServer(nodeForWorkflow, layout);
        CFUtils.getUninterruptibly(bootStrapFuture, TimeoutException.class);
        // Send the add node request to the node's orchestrator.
        CreateWorkflowResponse resp = managementClient.addNodeRequest(nodeForWorkflow);
        log.info("sendRequest: requested to add {} on orchestrator {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort());
        return resp.getWorkflowId();
    }

    @Override
    protected boolean verifyRequest(@NonNull Layout layout) {
        // Verify that the node has been added and that the address space isn't
        // segmented
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
    protected Optional<ManagementClient> getOrchestrator(){

        BaseClient baseClient = runtime.getLayoutView().getRuntimeLayout().getBaseClient(nodeForWorkflow);
        if(baseClient.pingSync()){
            log.info("getOrchestrator: orchestrator selected {}", nodeForWorkflow);
            return Optional.of(runtime.getLayoutView()
                    .getRuntimeLayout().getManagementClient(nodeForWorkflow));
        }
        else{
            return Optional.empty();
        }
    }
}
