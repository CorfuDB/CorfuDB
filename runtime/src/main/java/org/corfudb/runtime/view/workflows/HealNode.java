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


@Slf4j
public class HealNode extends WorkflowRequest {

    public HealNode(@Nonnull String endpointToHeal, @Nonnull CorfuRuntime runtime,
                    int retry, @Nonnull Duration timeout,
                    @Nonnull Duration pollPeriod) {
        this.nodeForWorkflow = endpointToHeal;
        this.runtime = runtime;
        this.retry = retry;
        this.timeout = timeout;
        this.pollPeriod = pollPeriod;
    }

    @Override
    protected UUID sendRequest(@Nonnull ManagementClient managementClient) throws TimeoutException {
        CreateWorkflowResponse resp = managementClient.healNodeRequest(nodeForWorkflow,
                true, true, true, 0);
        log.info("sendRequest: requested to heal {} on orchestrator {}:{}",
                nodeForWorkflow, managementClient.getRouter().getHost(),
                managementClient.getRouter().getPort());
        return resp.getWorkflowId();
    }

    @Override
    protected boolean verifyRequest(Layout layout) {
        log.info("verifyRequest: {} in {}", this, layout);
        return runtime.getLayoutView().getLayout().getAllServers().contains(nodeForWorkflow)
                && layout.getSegmentsForEndpoint(nodeForWorkflow).size() == 1;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + nodeForWorkflow;
    }
}
