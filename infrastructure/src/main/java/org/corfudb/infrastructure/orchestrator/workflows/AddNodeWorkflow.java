package org.corfudb.infrastructure.orchestrator.workflows;

import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.IWorkflow;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.corfudb.infrastructure.orchestrator.actions.StateTransfer.*;
import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.ADD_NODE;

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
                new RestoreRedundancy());
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

    /**
     * The new server is caught up with all data.
     * This server is then added to all the segments to mark it open to all reads and writes.
     */
    protected class RestoreRedundancy extends Action {
        @Nonnull
        @Override
        public String getName() {
            return "RestoreRedundancy";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws OutrankedException,
                ExecutionException,
                InterruptedException {
            runtime.invalidateLayout();
            newLayout = runtime.getLayoutView().getLayout();

            // A newly added node can be marked as unresponsive by the fault detector by the
            // time this action is executed. There are 2 cases following this:

            // Case 1. The node remains unresponsive.
            //      State transfer fails.
            // Case 2. The node is marked responsive again.
            //      In this case, the node was removed from all segments and was wiped clean.
            //      So either the healing workflow or the add node workflow will attempt
            //      to catchup the new node.
            if (newLayout.getAllActiveServers().contains(request.endpoint)) {
                // Transfer only till the second last segment as the last segment is unbounded.
                // The new server is already a part of the last segment. This is based on an
                // assumption that the newly added node is not removed from the layout.
                for (int i = 0; i < newLayout.getSegments().size() - 1; i++) {
                    transfer(newLayout, Collections.singleton(request.getEndpoint()),
                            runtime,
                            newLayout.getSegments().get(i));
                }

                final int stripeIndex = 0;
                runtime.getLayoutManagementView()
                        .addLogUnitReplica(
                                new Layout(newLayout), request.getEndpoint(), stripeIndex);
                runtime.invalidateLayout();
                newLayout = runtime.getLayoutView().getLayout();
            } else {
                throw new IllegalStateException("RestoreRedundancy: "
                        + "Node to be added marked unresponsive.");
            }
        }
    }
}
