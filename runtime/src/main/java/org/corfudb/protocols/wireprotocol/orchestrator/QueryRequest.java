package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.UUID;

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

    public QueryRequest(byte[] buf) {
        ByteBuffer bytes = ByteBuffer.wrap(buf);
        this.id = new UUID(bytes.getLong(), bytes.getLong());
    }

    @Override
    public OrchestratorRequestType getType() {
        return QUERY;
    }

    @Override
    public byte[] getSerialized() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2);
        buf.putLong(id.getMostSignificantBits());
        buf.putLong(id.getLeastSignificantBits());
        return buf.array();
    }
}
