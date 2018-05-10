package org.corfudb.infrastructure.orchestrator.workflows;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.IWorkflow;
import org.corfudb.protocols.wireprotocol.orchestrator.RemoveNodeRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.REMOVE_NODE;

/**
 * A workflow definition that removes a node from the cluster, if it exists. This workflow
 * will fail if the remove results in a cluster that is not redundant.
 * @author Maithem
 */

@Slf4j
public class RemoveNodeWorkflow implements IWorkflow {

    private final RemoveNodeRequest request;

    /**
     * The id of this workflow
     */
    @Getter
    final UUID id;

    @Getter
    final List<Action> actions;

    /**
     * Create a remove node workflow from a request.
     * @param request the remove node request
     */
    public RemoveNodeWorkflow(@Nonnull RemoveNodeRequest request) {
        this.id = UUID.randomUUID();
        this.request = request;
        actions = Collections.singletonList(new RemoveNode());

    }

    @Override
    public String getName() {
        return REMOVE_NODE.toString();
    }

    /**
     * Remove the node from the current layout.
     */
    class RemoveNode extends Action {
        @Override
        public String getName() {
            return REMOVE_NODE.toString();
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            Layout layout = new Layout(runtime.getLayoutView().getLayout());
            runtime.getLayoutManagementView().removeNode(layout,
                    request.getEndpoint());
        }
    }

}
