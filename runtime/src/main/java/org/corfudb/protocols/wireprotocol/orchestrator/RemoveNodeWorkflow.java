package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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

    final RemoveNodeRequest request;

    /**
     * The id of this workflow
     */
    @Getter
    final UUID id;

    /**
     * Create a remove node workflow from a request.
     * @param request the remove node request
     */
    public RemoveNodeWorkflow(@Nonnull Request request) {
        this.id = UUID.randomUUID();
        this.request = (RemoveNodeRequest) request;
    }

    @Override
    public String getName() {
        return REMOVE_NODE.toString();
    }

    @Override
    public List<Action> getActions() {
        return Collections.singletonList(new RemoveNode());
    }

    /**
     * Remove the node from the current layout.
     */
    class RemoveNode extends Action {
        @Override
        public String getName() {
            return "RemoveNode";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            changeStatus(ActionStatus.STARTED);
            Layout layout = new Layout(runtime.getLayoutView().getLayout());
            if (layout.getAllServers().contains(request.getEndpoint())) {
                runtime.getLayoutManagementView().removeNode(layout,
                        request.getEndpoint());
            } else {
                log.info("impl: Ignoring remove node on {} because it doesn't exist",
                        request.getEndpoint());
            }
            changeStatus(ActionStatus.COMPLETED);
        }
    }

}
