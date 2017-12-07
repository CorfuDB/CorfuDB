package org.corfudb.infrastructure.orchestrator;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

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

    final AddNodeRequest request;

    private Layout newLayout;

    /**
     * The chunk size (i.e. number of address space entries) that
     * the state transfer operation uses.
     */
    public static final long CHUNK_SIZE = 2500;

    @Getter
    final UUID id;

    @Getter
    final List<Action> actions;

    /**
     * Creates a new add node workflow from a request.
     *
     * @param request request to add a node
     */
    public AddNodeWorkflow(Request request) {
        this.id = UUID.randomUUID();
        this.request = (AddNodeRequest) request;
        actions = ImmutableList.of(new BootstrapNode(),
                new AddNodeToLayout(),
                new StateTransfer(),
                new MergeSegments());
    }

    @Override
    public String getName() {
        return ADD_NODE.toString();
    }

    /**
     * Bootstrap the new node to be added to the cluster, or ignore
     * bootstrap if it's already bootstrapped.
     */
    private class BootstrapNode extends Action {
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
            return;

        }
    }

    /**
     * Transfer an address segment from a cluster to a new node. The epoch shouldn't change
     * during the segment transfer.
     *
     * @param endpoint destination node
     * @param runtime  The runtime to read the segment from
     * @param segment  segment to transfer
     */
    private void stateTransfer(String endpoint, CorfuRuntime runtime,
                       Layout.LayoutSegment segment) throws Exception {

        long trimMark = runtime.getAddressSpaceView().getTrimMark();
        if (trimMark > segment.getEnd()) {
            log.info("stateTransfer: Nothing to transfer, trimMark {} greater than end of segment {}",
                    trimMark, segment.getEnd());
            return;
        }

        for (long chunkStart = segment.getStart(); chunkStart < segment.getEnd()
                ; chunkStart = chunkStart + CHUNK_SIZE) {
            long chunkEnd = Math.min((chunkStart + CHUNK_SIZE - 1), segment.getEnd() - 1);

            Map<Long, ILogData> dataMap = runtime.getAddressSpaceView()
                    .cacheFetch(ContiguousSet.create(
                            Range.closed(chunkStart, chunkEnd),
                            DiscreteDomain.longs()));

            List<LogData> entries = new ArrayList<>();
            for (long x = chunkStart; x <= chunkEnd; x++) {
                if (dataMap.get(x) == null) {
                    log.error("Missing address {} in range {}-{}", x, chunkStart, chunkEnd);
                    throw new IllegalStateException("Missing address");
                }
                entries.add((LogData) dataMap.get(x));
            }

            // Write segment chunk to the new logunit
            boolean transferSuccess = runtime
                    .getRouter(endpoint)
                    .getClient(LogUnitClient.class)
                    .writeRange(entries).get();

            if (!transferSuccess) {
                log.error("stateTransfer: Failed to transfer {}-{} to {}", CHUNK_SIZE,
                        chunkEnd, endpoint);
                throw new IllegalStateException("Failed to transfer!");
            }

            log.info("stateTransfer: Transferred address chunk [{}, {}]",
                    chunkStart, chunkEnd);
        }
    }


    /**
     * Copies the split segment to the new node, if it
     * is the new node also participates as a logging unit.
     */
    private class StateTransfer extends Action {
        @Override
        public String getName() {
            return "StateTransfer";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            // Transfer the replicated segment to the new node
            stateTransfer(request.getEndpoint(), runtime, newLayout.getSegment(0));
        }
    }

    /**
     * Merges the fragmented segment if the AddNodeToLayout action caused any
     * segments to split
     */
    private class MergeSegments extends Action {
        @Override
        public String getName() {
            return "MergeSegments";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            runtime.getLayoutManagementView().mergeSegments(newLayout);
        }
    }
}
