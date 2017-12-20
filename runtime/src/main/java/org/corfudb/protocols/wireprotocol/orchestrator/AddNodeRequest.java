package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;

import java.nio.charset.StandardCharsets;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.ADD_NODE;

/**
 *
 * An orchestrator request to add a new node to the cluster.
 *
 * @author Maithem
 */
public class AddNodeRequest implements CreateRequest {

    @Getter
    public String endpoint;

    public AddNodeRequest(String endpoint) {
        this.endpoint = endpoint;
    }

    public AddNodeRequest(byte[] buf) {
        endpoint = new String(buf, StandardCharsets.UTF_8);
    }

    @Override
    public OrchestratorRequestType getType() {
        return ADD_NODE;
    }

    @Override
    public byte[] getSerialized() {
        return endpoint.getBytes(StandardCharsets.UTF_8);
    }
}
