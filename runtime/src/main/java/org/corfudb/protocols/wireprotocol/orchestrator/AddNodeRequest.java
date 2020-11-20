package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.ADD_NODE;

/**
 *
 * An orchestrator request to add a new node to the cluster.
 *
 * @author Maithem
 */
public class AddNodeRequest implements Request {

    @Getter
    public String endpoint;

    public AddNodeRequest(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public OrchestratorRequestType getType() {
        return ADD_NODE;
    }
}
