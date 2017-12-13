package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;
import org.corfudb.format.Types.OrchestratorRequestType;

import java.nio.charset.StandardCharsets;

import static org.corfudb.format.Types.OrchestratorRequestType.ADD_NODE;

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
