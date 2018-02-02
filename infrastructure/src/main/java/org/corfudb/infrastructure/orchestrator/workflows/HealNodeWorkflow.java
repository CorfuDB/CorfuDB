package org.corfudb.infrastructure.orchestrator.workflows;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.HEAL_NODE;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.HealNodeRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.Layout;
import lombok.extern.slf4j.Slf4j;

/**
 * A definition of a workflow that heals an existing unresponsive node back to the cluster.
 * NOTE: The healing first resets this node, which erases all existing data.
 * Then, similar to the AddNodeWorkflow, the segment is split, the node is added with all the
 * data transferred from the existing log unit nodes and finally the segments are merged.
 *
 * <p>Created by Zeeshan on 12/8/17.
 */
@Slf4j
public class HealNodeWorkflow extends AddNodeWorkflow {

    private final HealNodeRequest request;

    public HealNodeWorkflow(HealNodeRequest healNodeRequest) {
        super(new AddNodeRequest(healNodeRequest.getEndpoint()));
        this.request = healNodeRequest;
    }

    @Override
    public String getName() {
        return HEAL_NODE.toString();
    }

    @Override
    public List<Action> getActions() {
        return Arrays.asList(new ResetNode(),
                new HealNodeToLayout(),
                new StateTransfer(),
                new MergeSegments());
    }

    /**
     * Resets the node's data.
     */
    class ResetNode extends Action {
        @Override
        public String getName() {
            return "ResetNode";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            runtime.invalidateLayout();
            if (runtime.getLayoutView().getLayout().getUnresponsiveServers()
                    .contains(request.getEndpoint())) {
                runtime.getRouter(request.getEndpoint())
                        .getClient(LogUnitClient.class)
                        .resetLogUnit()
                        .get();
            }
        }
    }


    /**
     * This action adds a new node to the layout. If it is also
     * added as a logunit server, then in addition to adding
     * the node the address space segment is split at the
     * tail determined during the layout modification.
     */
    class HealNodeToLayout extends Action {
        @Override
        public String getName() {
            return "HealNodeToLayout";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            Layout currentLayout = new Layout(runtime.getLayoutView().getLayout());
            runtime.getLayoutManagementView().healNode(currentLayout, request.getEndpoint());
            runtime.invalidateLayout();
            newLayout = new Layout(runtime.getLayoutView().getLayout());
        }
    }
}
