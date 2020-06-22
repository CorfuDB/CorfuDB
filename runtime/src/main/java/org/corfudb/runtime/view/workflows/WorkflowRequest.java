package org.corfudb.runtime.view.workflows;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WorkflowException;
import org.corfudb.runtime.exceptions.WorkflowResultUnknownException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * An abstract class that defines a generic workflow request structure.
 * <p>
 * Created by Maithem on 1/19/18.
 */
@Slf4j
public abstract class WorkflowRequest {

    protected int retry;

    protected Duration timeout;

    protected Duration pollPeriod;

    protected CorfuRuntime runtime;

    @NonNull
    protected String nodeForWorkflow;

    /**
     * Send a workflow request
     *
     * @param client the client to the selected orchestrator
     * @return a uuid that corresponds to the created workflow
     */
    protected abstract UUID sendRequest(ManagementClient client) throws TimeoutException;

    /**
     * Select an orchestrator on the node that responds to pings and is not on
     * the same node affected by the workflow. The layout might not reflect the state
     * of the responsive servers, so we ping the endpoint before we select it.
     * An example of this would be a 3 node cluster, with two nodes that
     * die immediately, the layout won't have those two nodes as unresponsive
     * because it can't commit to a quorum.
     *
     * @return a management client that is connected to the selected
     * orchestrator
     */
    protected Optional<ManagementClient> getOrchestrator() {

        runtime.invalidateLayout();
        Layout layout = new Layout(runtime.getLayoutView().getLayout());

        List<String> availableLayoutServers = layout.getLayoutServers().stream()
                .filter(s -> !s.equals(nodeForWorkflow))
                .collect(Collectors.toList());

        if (availableLayoutServers.isEmpty()) {
            throw new WorkflowException("getOrchestrator: no available orchestrators " + layout);
        }

        // Select an available orchestrator
        Optional<ManagementClient> managementClient = Optional.empty();

        for (String endpoint : availableLayoutServers) {
            BaseClient client = runtime.getLayoutView().getRuntimeLayout(layout)
                    .getBaseClient(endpoint);
            if (client.pingSync()) {
                log.info("getOrchestrator: orchestrator selected {}, layout {}", endpoint, layout);
                managementClient = Optional.of(runtime.getLayoutView().getRuntimeLayout(layout)
                        .getManagementClient(endpoint));
                break;
            }

        }

        return managementClient;
    }

    /**
     * Infer the completion of the request by inspecting the layout
     *
     * @param layout the layout to inspect
     * @return true if the operation is reflected in the layout, otherwise
     * return false
     */
    protected abstract boolean verifyRequest(Layout layout);

    /**
     * @param workflow   the workflow id to poll
     * @param client     a client that is connected to the orchestrator that is
     *                   running the workflow
     * @param timeout    the total time to wait for the workflow to complete
     * @param pollPeriod the poll period to query the completion of the workflow
     * @throws TimeoutException if the workflow doesn't complete within the timeout
     *                          period
     */
    private void waitForWorkflow(@NonNull UUID workflow, @NonNull ManagementClient client,
                                 @NonNull Duration timeout, @NonNull Duration pollPeriod)
            throws TimeoutException {
        long tries = timeout.toNanos() / pollPeriod.toNanos();
        for (long x = 0; x < tries; x++) {
            if (!client.queryRequest(workflow).isActive()) {
                return;
            }
            Sleep.sleepUninterruptibly(pollPeriod);
            log.debug("waitForWorkflow: waiting for {} on attempt {}", workflow, x);
        }
        throw new TimeoutException();
    }

    /**
     * Starts executing the workflow request.
     *
     * This method will succeed only if the workflow executed successfully, otherwise
     * it can throw a WorkflowResultUnknownException when the timeouts are exhausted
     * and the expected side effect cannot be verified.
     *
     * @throws WorkflowResultUnknownException when the workflow result cannot be
     * verified.
     */
    public void invoke() {
        for (int x = 0; x < retry; x++) {
            try {
                Optional<ManagementClient> orchestrator = getOrchestrator();
                if(orchestrator.isPresent()){
                    UUID workflowId = sendRequest(orchestrator.get());
                    waitForWorkflow(workflowId, orchestrator.get(), timeout, pollPeriod);
                }
                else{
                    throw new IllegalStateException("Orchestrator can not be selected");
                }

            } catch (NetworkException | TimeoutException | IllegalStateException e) {
                log.warn("WorkflowRequest: Error while running {} on attempt {}, cause:", this, x, e);
            }

            for (int y = 0; y < runtime.getParameters().getInvalidateRetry(); y++) {
                runtime.invalidateLayout();
                Layout layoutToVerify = new Layout(runtime.getLayoutView().getLayout());
                if (verifyRequest(layoutToVerify)) {
                    log.info("WorkflowRequest: Successfully completed {}", this);
                    return;
                }
            }
            log.warn("WorkflowRequest: Retrying {} on attempt {}", this, x);
        }

        throw new WorkflowResultUnknownException();
    }
}


