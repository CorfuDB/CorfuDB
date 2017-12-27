package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.FORCE_REMOVE;

/**
 *
 * This workflow removes an endpoint from the cluster forcefully by bypassing consensus. It removes
 * the endpoint from the current layout, seals the system and then sends a force layout request
 * to all the endpoints in the new layout.
 *
 * Created by Maithem on 12/13/17.
 */
public class ForceRemoveWorkflow implements IWorkflow {

    final RemoveNodeRequest request;

    /**
     * The id of this workflow.
     */
    @Getter
    final UUID id;

    /**
     * Create this workflow from a force remove request.
     */
    public ForceRemoveWorkflow(@Nonnull Request request) {
        this.id = UUID.randomUUID();
        this.request = (ForceRemoveRequest) request;
    }

    @Override
    public String getName() {
        return FORCE_REMOVE.toString();
    }

    @Override
    public List<Action> getActions() {
        return Arrays.asList(new ForceRemoveNode());
    }

    /**
     * Remove the endpoint point from the current layout bypassing consensus and layout
     * constraints, sealing the new cluster and then send a force layout to all the
     * endpoints in the new layout.
     */
    class ForceRemoveNode extends Action {
        @Override
        public String getName() {
            return "ForceRemoveNode";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            changeStatus(ActionStatus.STARTED);

            Layout currentLayout = new Layout(runtime.getLayoutView().getLayout());
            Layout newLayout = new Layout(currentLayout);

            newLayout.setEpoch(newLayout.getEpoch() + 1);

            newLayout.getLayoutServers().remove(request.getEndpoint());
            newLayout.getSequencers().remove(request.getEndpoint());

            for (Layout.LayoutSegment segment : newLayout.getSegments()) {
                for (Layout.LayoutStripe stripe : segment.getStripes()) {
                    stripe.getLogServers().remove(request.getEndpoint());
                }
            }

            runtime.getLayoutManagementView().forceLayout(currentLayout, newLayout);

            changeStatus(ActionStatus.COMPLETED);
        }
    }

}
