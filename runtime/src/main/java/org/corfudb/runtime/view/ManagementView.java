package org.corfudb.runtime.view;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WorkflowException;
import org.corfudb.util.Sleep;

import javax.annotation.Nonnull;

/**
 * A view of the Management Service to manage reconfigurations of the Corfu Cluster.
 *
 * <p>Created by zlokhandwala on 11/20/17.</p>
 */
@Slf4j
public class ManagementView extends AbstractView {

    public ManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     *
     * @param workflow the workflow id to poll
     * @param client a client that is connected to the orchestrator that is
     *               running the workflow
     * @param timeout the total time to wait for the workflow to complete
     * @param pollPeriod the poll period to query the completion of the workflow
     * @throws NetworkException if the client disconnects
     * @throws TimeoutException if the workflow doesn't complete withint the timout
     * period
     */
    void waitForWorkflow(UUID workflow, ManagementClient client, Duration timeout,
                         Duration pollPeriod) throws NetworkException, TimeoutException {
        long tries = timeout.getSeconds() / pollPeriod.getSeconds();
        for (long x = 0; x < tries; x++) {
            if (!client.queryRequest(workflow).isActive()) {
                return;
            }
            Sleep.sleepUninterruptibly(pollPeriod);
            log.info("waitForWorkflow: waiting for {} on try {}", workflow, x);
        }
        log.debug("waitForWorkflow: workflow {} timeout", workflow);
        throw new TimeoutException();
    }

    /**
     * Add a new node to the existing cluster.
     *
     * @param endpointToAdd Endpoint of the new node to be added to the cluster.
     * @param retry the number of times to retry a workflow if it fails
     * @param timeout total time to wait before the workflow times out
     * @param pollPeriod the poll interval to check whether a workflow completed or not
     * @throws WorkflowException throws a workflow exception on failure
     */
    public void addNode(@Nonnull String endpointToAdd, int retry,
                        @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {

        for (int x = 0; x < retry; x++) {
            runtime.invalidateLayout();
            Layout layout = runtime.getLayoutView().getLayout();
            List<String> logServers = layout.getSegments().get(0).getStripes().get(0).getLogServers();
            String orchestratorEndpoint = logServers.get(logServers.size() - 1);
            ManagementClient client = runtime.getRouter(orchestratorEndpoint)
                    .getClient(ManagementClient.class);

            try {
                CreateWorkflowResponse resp = client.addNodeRequest(endpointToAdd);
                log.info("addNode: Workflow id {} for {}", resp.workflowId, endpointToAdd);
                waitForWorkflow(resp.getWorkflowId(), client, timeout, pollPeriod);
                Layout newLayout = null;
                for (int y = 0; y < runtime.getParameters().getInvalidateRetry(); y++) {
                    runtime.invalidateLayout();
                    newLayout = runtime.getLayoutView().getLayout();
                    if (newLayout.getAllServers().contains(endpointToAdd) &&
                            newLayout.getSegmentsForEndpoint(endpointToAdd).size() == 1) {
                        log.info("addNode: Successfully added {}", endpointToAdd);
                        return;
                    }
                }
                log.warn("addNode: Couldn't find {} in {}, retrying...", endpointToAdd, newLayout, x);

            } catch (NetworkException | TimeoutException  e) {
                log.warn("addNode: Error while requesting to add {}", endpointToAdd, e);
                continue;
            }
        }

        log.warn("addNode: Failed to add node {}", endpointToAdd);
        throw new WorkflowException("Failed to add " + endpointToAdd);
    }
}
