package org.corfudb.infrastructure.orchestrator.workflows;

import static org.corfudb.infrastructure.orchestrator.actions.StateTransfer.transfer;
import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.ADD_NODE;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.IWorkflow;
import org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;


/**
 * A definition of a workflow that adds a new node to the cluster. This workflow
 * has almost no retry logic, therefore errors can result in its failure and the
 * the client is responsible to ensure that it has completed by restarting the
 * workflow.
 *
 * @author Maithem
 */
@NotThreadSafe
@Slf4j
public class AddNodeWorkflow implements IWorkflow {

    private final AddNodeRequest request;

    Layout newLayout;

    @Getter
    final UUID id;

    @Getter
    List<Action> actions;

    /**
     * Creates a new add node workflow from a request.
     *
     * @param request request to add a node
     */
    public AddNodeWorkflow(AddNodeRequest request) {
        this.id = UUID.randomUUID();
        this.request = request;
        actions = ImmutableList.of(new BootstrapNode(),
                new AddNodeToLayout(),
                new RestoreRedundancyMergeSegments(request.getEndpoint()));
    }

    @Override
    public String getName() {
        return ADD_NODE.toString();
    }

    /**
     * Bootstrap the new node to be added to the cluster, or ignore
     * bootstrap if it's already bootstrapped.
     */
    protected class BootstrapNode extends Action {
        @Override
        public String getName() {
            return "BootstrapNode";
        }

        /**
         * Completes if bootstrap was successful.
         *
         * @param runtime A runtime that the action will use to execute
         */
        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            runtime.getLayoutManagementView().bootstrapNewNode(request.getEndpoint()).get();
        }
    }


    /**
     * This action adds a new node to the layout. If it is also
     * added as a logunit server, then in addition to adding
     * the node the address space segment is split at the
     * tail determined during the layout modification.
     */
    private class AddNodeToLayout extends Action {
        @Override
        public String getName() {
            return "AddNodeToLayout";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            Layout currentLayout = new Layout(runtime.getLayoutView().getLayout());
            runtime.getLayoutManagementView().addNode(currentLayout, request.getEndpoint(),
                    true, true,
                    true, false,
                    0);

            runtime.invalidateLayout();
            newLayout = new Layout(runtime.getLayoutView().getLayout());
        }
    }
}
