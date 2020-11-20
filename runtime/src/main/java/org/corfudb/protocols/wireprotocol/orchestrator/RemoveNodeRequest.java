package org.corfudb.protocols.wireprotocol.orchestrator;

import javax.annotation.Nonnull;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.REMOVE_NODE;

/**
 *
 * An orchestrator request to remove a node to the cluster.
 *
 * @author Maithem
 */
public class RemoveNodeRequest extends AddNodeRequest {

    /**
     * Creates a request to remove a node from an endpoint.
     * @param endpoint the endpoint to remove
     */
    public RemoveNodeRequest(@Nonnull String endpoint) {
        super(endpoint);
    }

    @Override
    public OrchestratorRequestType getType() {
        return REMOVE_NODE;
    }
}
