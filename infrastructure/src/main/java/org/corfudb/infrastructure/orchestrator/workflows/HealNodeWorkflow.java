package org.corfudb.infrastructure.orchestrator.workflows;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.HEAL_NODE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Set;

import javax.annotation.Nonnull;

import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.HealNodeRequest;
import org.corfudb.runtime.CorfuRuntime;
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
        this.actions = ImmutableList.of(new HealNodeToLayout(),
                new RestoreRedundancyAndMergeSegments());
    }

    @Override
    public String getName() {
        return HEAL_NODE.toString();
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

    /**
     * This action attempts to restore redundancy for all servers across all segments
     * starting from the oldest segment. It then collapses the segments once the set of
     * servers in the 2 oldest subsequent segments are equal.
     */
    class RestoreRedundancyAndMergeSegments extends Action {
        @Nonnull
        @Override
        public String getName() {
            return "RestoreRedundancyAndMergeSegments";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            // Catchup all servers across all segments.
            while (newLayout.getSegments().size() > 1) {

                // Currently the state is transferred for the complete segment.
                // TODO: Add stripe specific transfer granularity for optimization.
                // Get the set of servers present in the second segment but not in the first
                // segment.
                Set<String> lowRedundancyServers = Sets.difference(
                        newLayout.getSegments().get(1).getAllLogServers(),
                        newLayout.getSegments().get(0).getAllLogServers());
                // Transfer the replicated segment to the difference set calculated above.
                stateTransfer(lowRedundancyServers, runtime, newLayout.getSegments().get(0));
                runtime.getLayoutManagementView().mergeSegments(new Layout(newLayout));
                runtime.invalidateLayout();
                newLayout = runtime.getLayoutView().getLayout();
            }
        }
    }
}
