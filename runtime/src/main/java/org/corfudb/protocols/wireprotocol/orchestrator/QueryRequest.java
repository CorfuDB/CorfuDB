package org.corfudb.protocols.wireprotocol.orchestrator;

import java.util.UUID;
import lombok.Getter;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.QUERY;

/**
 * An orchestrator request that queries a specific workflow's ID.
 *
 * @author Maithem
 */
public class QueryRequest implements Request {

    @Getter
    public UUID id;

    public QueryRequest(UUID id) {
        this.id = id;
    }

    @Override
    public OrchestratorRequestType getType() {
        return QUERY;
    }

    @Override
    public String getEndpoint() {
        throw new UnsupportedOperationException("This operation is not supported for a QueryRequest");
    }
}
