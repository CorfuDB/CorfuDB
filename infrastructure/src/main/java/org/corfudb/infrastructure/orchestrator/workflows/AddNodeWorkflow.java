package org.corfudb.infrastructure.orchestrator.workflows;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.ADD_NODE;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.IWorkflow;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

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

    protected Layout newLayout;

    @Getter
    final UUID id;

    @Getter
    protected List<Action> actions;

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

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            try {
                runtime.getLayoutManagementView().bootstrapNewNode(request.getEndpoint());
            } catch (Exception e) {
                if (e.getCause() instanceof AlreadyBootstrappedException) {
                    log.info("BootstrapNode: Node {} already bootstrapped, skipping.", request.getEndpoint());
                } else {
                    log.error("execute: Error during bootstrap", e);
                    throw e;
                }
            }
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
     * Fetch and propagate the trimMark to the new/healing nodes. Else, a FastLoader reading from
     * them will have to mark all the already trimmed entries as holes.
     * Transfer an address segment from a cluster to a set of specified nodes.
     * There are no cluster reconfigurations, hence no epoch change side effects.
     *
     * @param endpoints destination nodes
     * @param runtime   The runtime to read the segment from
     * @param segment   segment to transfer
     */
    protected void stateTransfer(Set<String> endpoints, CorfuRuntime runtime,
                                 Layout.LayoutSegment segment) throws Exception {

        int batchSize = runtime.getParameters().getBulkReadSize();

        long trimMark = runtime.getAddressSpaceView().getTrimMark();
        // Send the trimMark to the new/healing nodes.
        // If this times out or fails, the Action performing the stateTransfer fails and retries.
        for (String endpoint : endpoints) {
            // TrimMark is the first address present on the log unit server.
            // Perform the prefix trim on the preceding address = (trimMark - 1).
            CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(newLayout)
                    .getLogUnitClient(endpoint)
                    .prefixTrim(trimMark - 1));
        }

        if (trimMark > segment.getEnd()) {
            log.info("stateTransfer: Nothing to transfer, trimMark {} greater than end of segment {}",
                    trimMark, segment.getEnd());
            return;
        }

        // State transfer should start from segment start address or trim mark whichever is lower.
        long segmentStart = Math.max(trimMark, segment.getStart());

        for (long chunkStart = segmentStart; chunkStart < segment.getEnd()
                ; chunkStart = chunkStart + batchSize) {
            long chunkEnd = Math.min((chunkStart + batchSize - 1), segment.getEnd() - 1);

            long ts1 = System.currentTimeMillis();

            Map<Long, ILogData> dataMap = runtime.getAddressSpaceView()
                    .cacheFetch(ContiguousSet.create(
                            Range.closed(chunkStart, chunkEnd),
                            DiscreteDomain.longs()));

            long ts2 = System.currentTimeMillis();

            log.info("stateTransfer: read {}-{} in {} ms", chunkStart, chunkEnd, (ts2 - ts1));

            List<LogData> entries = new ArrayList<>();
            for (long x = chunkStart; x <= chunkEnd; x++) {
                if (dataMap.get(x) == null) {
                    log.error("Missing address {} in range {}-{}", x, chunkStart, chunkEnd);
                    throw new IllegalStateException("Missing address");
                }
                entries.add((LogData) dataMap.get(x));
            }

            for (String endpoint : endpoints) {
                // Write segment chunk to the new logunit
                ts1 = System.currentTimeMillis();
                boolean transferSuccess = runtime.getLayoutView().getRuntimeLayout(newLayout)
                        .getLogUnitClient(endpoint)
                        .writeRange(entries).get();
                ts2 = System.currentTimeMillis();

                if (!transferSuccess) {
                    log.error("stateTransfer: Failed to transfer {}-{} to {}", chunkStart,
                            chunkEnd, endpoint);
                    throw new IllegalStateException("Failed to transfer!");
                }

                log.info("stateTransfer: Transferred address chunk [{}, {}] to {} in {} ms",
                        chunkStart, chunkEnd, endpoint, (ts2 - ts1));
            }
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
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
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
                    stateTransfer(Collections.singleton(request.getEndpoint()),
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
                throw new RuntimeException("RestoreRedundancy: "
                        + "Node to be added marked unresponsive.");
            }
        }
    }
}
