package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;
import org.corfudb.format.Types.OrchestratorResponseType;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.corfudb.format.Types.OrchestratorResponseType.WORKFLOW_ID;

/**
 * AddNodeResponse returns the UUID of the add node workflow that was requested.
 * @author Maithem
 */
public class AddNodeResponse implements Response {

    @Getter
    public UUID workflowId;

    public AddNodeResponse(UUID workflowId) {
        this.workflowId = workflowId;
    }

    public AddNodeResponse(byte[] buf) {
        ByteBuffer bytes = ByteBuffer.wrap(buf);
        this.workflowId = new UUID(bytes.getLong(), bytes.getLong());
    }

    @Override
    public OrchestratorResponseType getType() {
        return WORKFLOW_ID;
    }

    @Override
    public byte[] getSerialized() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2);
        buf.putLong(workflowId.getMostSignificantBits());
        buf.putLong(workflowId.getLeastSignificantBits());
        return buf.array();
    }
}
