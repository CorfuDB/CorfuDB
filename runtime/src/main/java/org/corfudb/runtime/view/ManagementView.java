package org.corfudb.runtime.view;

import java.util.List;
import java.util.UUID;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.util.Sleep;
import org.corfudb.util.Utils;

/**
 * A view of the Management Service to manage reconfigurations of the Corfu Cluster.
 *
 * <p>Created by zlokhandwala on 11/20/17.</p>
 */
@Slf4j
public class ManagementView extends AbstractView {

    private final long layoutRefreshTimeout = 500;

    public ManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Checks if a given workflow with the given workflow ID is active or not.
     * If returns True - the workflow is active
     * If returns False - the workflow is either completed successfully or failed.
     *
     * @param server     Node endpoint on which the workflow is running.
     * @param workflowId Workflow UUID
     * @return True if active. Else False
     */
    public boolean isWorkflowActive(String server, UUID workflowId) {
        return layoutHelper(l -> {
            QueryResponse response = runtime.getRouter(server)
                    .getClient(ManagementClient.class)
                    .queryRequest(workflowId);
            log.info("isWorkflowActive: {} on server:{} = {}",
                workflowId, server, response.isActive());
            return response.isActive();
        });
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
            List<String> logServers = l.getSegments().get(0).getStripes().get(0).getLogServers();
            String server = logServers.get(logServers.size() - 1);
            ManagementClient client = runtime.getRouter(server).getClient(ManagementClient.class);

            // Dispatch add node request.
            CreateWorkflowResponse addNodeResponse = client.addNodeRequest(endpoint);

            UUID workflowId = addNodeResponse.getWorkflowId();

            while (isWorkflowActive(server, workflowId)) {
                Sleep.MILLISECONDS.sleepUninterruptibly(layoutRefreshTimeout);
            }

            runtime.invalidateLayout();

            if (!runtime.getLayoutView().getLayout().getAllServers().contains(endpoint)
                    || runtime.getLayoutView().getLayout().getSegments().size() != 1) {
                throw new RuntimeException("Adding node:" + endpoint + " failed.");
            }

            return true;
        });
    }
}
