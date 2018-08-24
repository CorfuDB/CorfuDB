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

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * An abstract class that defines a generec workflow request structure.
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
     * Select an orchestrator and return a client. Orchestrator's that
     * are on unresponsive nodes and the affected endpoint by the workflow
     * will not be selected. The layout might not reflect the state
     * of responsive servers, so we ping the endpoint before we select it.
     * An example of this would be a 3 node cluster, with two nodes that
     * die immediately, the layout won't have those two nodes as unresponsive
     * because it can't commit to a quorum.
     *
     * @param layout the layout that is used to select an orchestrator
     * @return a management client that is connected to the selected
     * orchestrator
     */
    protected ManagementClient getOrchestrator(Layout layout) {
        List<String> activeLayoutServers = layout.getLayoutServers().stream()
                .filter(s -> !layout.getUnresponsiveServers().contains(s)
                        && !s.equals(nodeForWorkflow))
                .collect(Collectors.toList());

        if (activeLayoutServers.isEmpty()) {
            throw new WorkflowException("getOrchestrator: no available orchestrators " + layout);
        }

        // Select an available orchestrator
        ManagementClient managementClient = runtime.getLayoutView().getRuntimeLayout(layout)
                .getManagementClient(activeLayoutServers.get(0));

        for (String endpoint : activeLayoutServers) {
            BaseClient client = runtime.getLayoutView().getRuntimeLayout(layout)
                    .getBaseClient(endpoint);
            if (client.pingSync()) {
                log.info("getOrchestrator: orchestrator selected {}, layout {}", endpoint, layout);
                managementClient = runtime.getLayoutView().getRuntimeLayout(layout)
                        .getManagementClient(endpoint);
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
     * @throws TimeoutException if the workflow doesn't complete withint the timout
     *                          period
     */
    private void waitForWorkflow(@Nonnull UUID workflow, @Nonnull ManagementClient client,
                                 @Nonnull Duration timeout, @Nonnull Duration pollPeriod)
            throws TimeoutException {
        long tries = timeout.toNanos() / pollPeriod.toNanos();
        for (long x = 0; x < tries; x++) {
            if (!client.queryRequest(workflow).isActive()) {
                return;
            }
            Sleep.sleepUninterruptibly(pollPeriod);
            log.info("waitForWorkflow: waiting for {} on attempt {}", workflow, x);
        }
        throw new TimeoutException();
    }

    /**
     * Starts executing the workflow request.
     *
     * This method will succeed only if the workflow executed successfully, otherwise
     * it can throw a WorkflowResultUnknownException when the timeouts are exhauseted
     * and the expected side effect cannot be verified.
     *
     * @throws WorkflowResultUnknownException when the workflow result cannot be
     * verified.
     */
    public void invoke() {
        for (int x = 0; x < retry; x++) {
            try {
                runtime.invalidateLayout();
                Layout requestLayout = new Layout(runtime.getLayoutView().getLayout());
                ManagementClient orchestrator = getOrchestrator(requestLayout);
                UUID workflowId = sendRequest(orchestrator);
                waitForWorkflow(workflowId, orchestrator, timeout, pollPeriod);
            } catch (NetworkException | TimeoutException e) {
                log.warn("WorkflowRequest: Error while running {} on attempt {}, cause {}", this, x, e);
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


