package org.corfudb.infrastructure.orchestrator.workflows;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.LayoutRateLimitParams;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ProbeCalc;
import org.corfudb.infrastructure.management.failuredetector.LayoutRateLimit.ProbeStatus;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.HealNodeRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutProbe.LayoutStatus;

import javax.annotation.Nonnull;

import static org.corfudb.infrastructure.utils.ServerContextUtils.buildRateLimitParamsFromServerContext;
import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.HEAL_NODE;

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

    public HealNodeWorkflow(HealNodeRequest healNodeRequest, ServerContext serverContext) {
        super(new AddNodeRequest(healNodeRequest.getEndpoint()));
        this.request = healNodeRequest;

        RestoreRedundancyMergeSegments mergeSegmentsAction = RestoreRedundancyMergeSegments.builder()
                .currentNode(request.getEndpoint())
                .redundancyCalculator(new RedundancyCalculator(request.getEndpoint()))
                .build();

        this.actions = ImmutableList.of(new HealNodeToLayout(serverContext), mergeSegmentsAction);
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
        private final ServerContext serverContext;

        @Override
        public String getName() {
            return "HealNodeToLayout";
        }

        public HealNodeToLayout(ServerContext serverContext) {
            this.serverContext = serverContext;
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            Layout currentLayout = new Layout(runtime.getLayoutView().getLayout());

            LayoutRateLimitParams layoutRateLimitParams = buildRateLimitParamsFromServerContext(serverContext);

            ProbeCalc probeCalc = ProbeCalc.builder()
                    .localEndpoint(serverContext.getLocalEndpoint())
                    .layoutRateLimitParams(layoutRateLimitParams)
                    .build();
            probeCalc.updateFromLayout(currentLayout);

            ProbeStatus probeStatus = probeCalc.calcStatsForNewUpdate();
            if (probeStatus.isAllowed()) {
                LayoutStatus newLayoutStatus = new LayoutStatus(probeStatus.getStatus(), probeCalc.printToLayout());
                currentLayout.setStatus(newLayoutStatus);

                runtime.getLayoutManagementView().healNode(currentLayout, request.getEndpoint());
                runtime.invalidateLayout();
                newLayout = new Layout(runtime.getLayoutView().getLayout());
            } else {
                log.warn("Healing disabled (layout update limit reached): probCalc {}, status {}", probeCalc, probeStatus);
            }
        }
    }
}
