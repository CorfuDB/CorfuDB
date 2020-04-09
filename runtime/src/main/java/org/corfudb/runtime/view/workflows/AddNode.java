package org.corfudb.runtime.view.workflows;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
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
    /**
     * Number of retries to bootstrap the management server.
     */
    private final int bootstrapRetries = 3;
    /**
     * Duration between bootstrap retries.
     */
    private final Duration bootstrapInterval = Duration.ofMillis(300);
    /**
     * A Client router to the node being added.
     */
    private final IClientRouter clientRouter;

    public AddNode(@NonNull String endpointToAdd, @NonNull CorfuRuntime runtime,
                   int retry, @NonNull Duration timeout,
                   @NonNull Duration pollPeriod, @NonNull IClientRouter clientRouter) {
        this.nodeForWorkflow = endpointToAdd;
        this.runtime = runtime;
        this.retry = retry;
        this.timeout = timeout;
        this.pollPeriod = pollPeriod;
        this.clientRouter = clientRouter;
    }

    @Override
    protected UUID sendRequest(@NonNull ManagementClient managementClient) throws TimeoutException {
        Layout layout = new Layout(runtime.getLayoutView().getLayout());
        // Bootstrap a management server first.
        // If there are network or timeout exceptions - retry.
        // If the retries fail - throw an exception.
        try {
            BootstrapUtil.retryBootstrap(nodeForWorkflow, layout, bootstrapRetries,
                    bootstrapInterval, clientRouter, BootstrapUtil::bootstrapManagementServer);
        } catch (AlreadyBootstrappedException abe) {
            log.warn("Skipping management server bootstrap for {}.", nodeForWorkflow);
        }
        // Now when the management server is bootstrapped, send the add node request to the node's orchestrator.
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
        runtime.invalidateLayout();
        Layout layout = new Layout(runtime.getLayoutView().getLayout());

        BaseClient baseClient = new BaseClient(clientRouter, layout.getEpoch(), layout.getClusterId());
        for (int i = 0; i < pingRetries; i++) {
            if (baseClient.pingSync()) {
                log.info("getOrchestrator: orchestrator selected {}", nodeForWorkflow);
                return Optional.of(new ManagementClient(clientRouter, layout.getEpoch(), layout.getClusterId()));
            }
            Sleep.sleepUninterruptibly(pingInterval);
            log.info("Retrying to ping a current base server {} times.", i);
        }
        log.warn("getOrchestrator: a server {} is not responding to pings", nodeForWorkflow);
        return Optional.empty();

    }
}
