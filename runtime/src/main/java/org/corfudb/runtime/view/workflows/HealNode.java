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
import java.util.function.Predicate;


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
        CreateWorkflowResponse resp = managementClient.healNodeRequest(nodeForWorkflow,
                true, true, true, 0);
        log.info("sendRequest: requested to heal {} on orchestrator {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort());
        return resp.getWorkflowId();
    }

    @Override
    protected boolean verifyRequest(@NonNull Layout layout) {
        log.info("verifyRequest: {} in {}", this, layout);
        return layout.getAllServers().contains(nodeForWorkflow)
                && layout.getSegmentsForEndpoint(nodeForWorkflow).size() == 1;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }

    @Override
    protected Optional<ManagementClient> getOrchestrator(){

        BaseClient baseClient = runtime.getLayoutView().getRuntimeLayout().getBaseClient(nodeForWorkflow);
        if(baseClient.pingSync()){
            return Optional.of(runtime.getLayoutView()
                    .getRuntimeLayout().getManagementClient(nodeForWorkflow));
        }
        else{
            return Optional.empty();
        }
    }
}
