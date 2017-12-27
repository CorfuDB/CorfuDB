package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.UUID;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequestType.QUERY;

/**
 * An orchestrator request that queries a specific workflow's ID.
 *
 * @author Maithem
 */
public class QueryRequest implements Request {

    /**
     * The workflow id to query.
     */
    @Getter
    public UUID id;

    /**
     * Query workflow id
     * @param id the workflow id to query
     */
    public QueryRequest(@Nonnull UUID id) {
        this.id = id;
    }

    /**
     * Create a query request from a byte array
     * @param buf the serialized request
     */
    public QueryRequest(@Nonnull byte[] buf) {
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
