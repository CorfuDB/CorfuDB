package org.corfudb.protocols.wireprotocol.orchestrator;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.QUERY;

/**
 * An orchestrator request that queries if a specific orchestrator is running
 * a workflow against an endpoint.
 *
 * @author Maithem
 */
public class QueryRequest extends AddNodeRequest {

    public QueryRequest(String address) {
        super(address);
    }

    public QueryRequest(byte[] buf) {
        super(buf);
    }

    @Override
    public OrchestratorRequestType getType() {
        return QUERY;
    }
}
