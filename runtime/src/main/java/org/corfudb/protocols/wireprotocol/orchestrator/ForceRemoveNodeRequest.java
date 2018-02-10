package org.corfudb.protocols.wireprotocol.orchestrator;

import javax.annotation.Nonnull;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.FORCE_REMOVE_NODE;

/**
 * A request to force remove an endpoint from the cluster.
 *
 * @author Maithem
 */
public class ForceRemoveNodeRequest extends RemoveNodeRequest {

    /**
     * Create a force remove request.
     * @param endpoint the endpoint to force remove
     */
    public ForceRemoveNodeRequest(@Nonnull String endpoint) {
        super(endpoint);
    }

    /**
     * Create a force remove request from a byte array.
     * @param buf the serialized request
     */
    public ForceRemoveNodeRequest(@Nonnull byte[] buf) {
        super(buf);
    }

    @Override
    public OrchestratorRequestType getType() {
        return FORCE_REMOVE_NODE;
    }
}
