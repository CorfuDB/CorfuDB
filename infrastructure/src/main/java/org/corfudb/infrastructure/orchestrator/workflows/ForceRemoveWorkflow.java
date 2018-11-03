package org.corfudb.infrastructure.orchestrator.workflows;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.IWorkflow;
import org.corfudb.protocols.wireprotocol.orchestrator.ForceRemoveNodeRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.FORCE_REMOVE_NODE;

/**
 *
 * This workflow removes an endpoint from the cluster forcefully by bypassing consensus. It removes
 * the endpoint from the current layout, seals the system and then sends a force layout request
 * to all the endpoints in the new layout.
 *
 * Created by Maithem on 12/13/17.
 */
@Slf4j
public class ForceRemoveWorkflow implements IWorkflow {

    final ForceRemoveNodeRequest request;

    /**
     * The id of this workflow.
     */
    @Getter
    final UUID id;

    @Getter
    final List<Action> actions;
    /**
     * Create this workflow from a force remove request.
     */
    public ForceRemoveWorkflow(@Nonnull ForceRemoveNodeRequest request) {
        this.id = UUID.randomUUID();
        this.request = request;
        actions = Collections.singletonList(new ForceRemoveNode());
    }

    @Override
    public String getName() {
        return FORCE_REMOVE_NODE.toString();
    }

    /**
     * Remove the endpoint from the current layout bypassing consensus and layout
     * constraints, sealing the new cluster and then send a force layout to all the
     * endpoints in the new layout.
     */
    class ForceRemoveNode extends Action {
        @Override
        public String getName() {
            return "ForceRemoveNode";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) {
            runtime.invalidateLayout();
            Layout currentLayout = new Layout(runtime.getLayoutView().getLayout());
            Layout newLayout = new Layout(currentLayout);

            newLayout.setEpoch(newLayout.getEpoch() + 1);

            NodeLocator endpoint = request.getEndpointNode();
            newLayout.removeLayoutServer(endpoint);
            newLayout.removeSequencer(endpoint);
            newLayout.removeUnresponsiveServer(endpoint);

            for (Layout.LayoutSegment segment : newLayout.getSegments()) {
                for (LayoutStripe stripe : segment.getStripes()) {
                    stripe.removeLogServer(endpoint);
                }
            }

            log.info("force removed {} from {},  new layout {}", endpoint.toEndpointUrl(), currentLayout, newLayout);

            runtime.getLayoutManagementView().forceLayout(currentLayout, newLayout);
        }
    }

}