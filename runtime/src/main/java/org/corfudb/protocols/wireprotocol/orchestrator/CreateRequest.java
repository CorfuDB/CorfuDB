package org.corfudb.protocols.wireprotocol.orchestrator;

/**
 * All requests that create workflows should implement this interface.
 * @author Maithem
 */
public interface CreateRequest extends Request {

    /**
     * The endpoint of the node to operate on (i.e. endpoint of a new node
     * to be added)
     * @return
     */
    String getEndpoint();

    /**
     * Create a workflow instance from this request
     *
     * @return IWorkflow instance
     */
    default IWorkflow getWorkflow() {
        return OrchestratorRequestType.typeMap
                .get(getType().getType())
                .getWorkflowGenerator().apply(this);
    }
}
