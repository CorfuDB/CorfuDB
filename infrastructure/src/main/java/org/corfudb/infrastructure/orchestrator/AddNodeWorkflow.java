package org.corfudb.infrastructure.orchestrator;

import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import static org.corfudb.format.Types.OrchestratorRequestType.ADD_NODE;

/**
 * A definition of a workflow that adds a new node to the cluster.
 *
 * @author Maithem
 */
@NotThreadSafe
public class AddNodeWorkflow implements Workflow {

    final AddNodeRequest request;

    public AddNodeWorkflow(Request request) {
        this.request = (AddNodeRequest) request;
    }

    @Override
    public String getName() {
        return ADD_NODE.toString();
    }

    @Override
    public List<Action> getActions() {
        return null;
    }

    class BootstrapNode implements Action {
        @Override
        public String getName() {
            return "BootstrapNode";
        }

        @Override
        public Map<String, Object> execute(@Nonnull CorfuRuntime runtime) throws Exception {
            runtime.getLayoutManagementView().bootstrapNewNode(request.getEndpoint());
            return null;
        }
    }

    /**
     * This action adds a new node to the layout. If it is also
     * added as a logunit server, then in addition to adding
     * the node the address space segment is split at the
     * tail determined during the layout modification.
     */
    class AddNodeToLayout implements Action {
        @Override
        public String getName() {
            return "AddNodeToLayout";
        }

        @Override
        public Map<String, Object> execute(@Nonnull CorfuRuntime runtime) throws Exception {
            Layout currentLayout = runtime.getLayoutView().getLayout();
            runtime.getLayoutManagementView().addNode(currentLayout, request.getEndpoint(),
                    true, true,
                    true, true,
                    0);
            return null;
        }
    }

    /**
     * Copies the split segment to the new node, if it
     * is the new node also participates as a logging unit.
     */
    class StateTransfer implements Action {
        @Override
        public String getName() {
            return "StateTransfer";
        }

        @Override
        public Map<String, Object> execute(@Nonnull CorfuRuntime runtime) throws Exception {
            // Transfer the replicated segment to the new node
            return null;
        }
    }

    /**
     * Merges the fragmented segment if the AddNodeToLayout action caused any
     * segments to split
     */
    class MergeSegments implements Action {
        @Override
        public String getName() {
            return "MergeSegments";
        }

        @Override
        public Map<String, Object> execute(@Nonnull CorfuRuntime runtime) throws Exception {
            // Transfer the replicated segment to the new node
            //runtime.getLayoutManagementView().mergeSegment(lastSegment);
            return null;
        }
    }
}
