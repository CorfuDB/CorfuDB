package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponseType.WORKFLOW_STATUS;

/**
 * Return a boolean that indicates whether a particular workflow is being executed by
 * an orchestrator service.
 * @author Maithem
 */
public class QueryResponse implements Response {

    @Getter
    final boolean active;

    public QueryResponse(boolean active) {
        this.active = active;
    }

    @Override
    public OrchestratorResponseType getType() {
        return WORKFLOW_STATUS;
    }
}
