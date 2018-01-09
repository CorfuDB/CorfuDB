package org.corfudb.runtime.view;

import java.time.Duration;
import java.util.List;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.orchestrator.WorkflowResult;
import org.corfudb.protocols.wireprotocol.orchestrator.WorkflowStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;

/**
 * A view of the Management Service to manage reconfigurations of the Corfu Cluster.
 *
 * <p>Created by zlokhandwala on 11/20/17.</p>
 */
@Slf4j
public class ManagementView extends AbstractView {

    static final Duration WORKFLOW_TIMEOUT = Duration.ofMinutes(10);
    static final int WORKFLOW_RETRY = 3;

    public ManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Add a new node to the existing cluster.
     *
     * @param endpoint Endpoint of the new node to be added to the cluster.
     * @return True if completed successfully.
     */
    public boolean addNode(String endpoint) {
        return layoutHelper(l -> {

            // Choosing the tail log server to run the workflow to optimize bulk reads on the
            // same node.
            log.debug("addNode: trying with layout {}", l);
            List<String> logServers = l.getSegments().get(0).getStripes().get(0).getLogServers();
            String server = logServers.get(logServers.size() - 1);
            log.debug("addNode: trying with tail {} {}", server, Thread.currentThread().getId());
            ManagementClient client = runtime.getRouter(server).getClient(ManagementClient.class);

            for (int x = 0; x < WORKFLOW_RETRY; x++) {
                WorkflowStatus status = client.addNodeRequest(endpoint, WORKFLOW_TIMEOUT);
                log.debug("addNode: status {} {}", status.getResult().toString(), Thread.currentThread().getId());
                if (status.getResult() == WorkflowResult.COMPLETED) {
                    return true;
                }
                log.debug("addNode: failed, retrying {}", Thread.currentThread().getId());
            }
            return false;
        });
    }
}
