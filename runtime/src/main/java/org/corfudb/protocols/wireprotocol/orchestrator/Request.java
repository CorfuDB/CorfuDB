package org.corfudb.protocols.wireprotocol.orchestrator;

import io.netty.buffer.ByteBuf;
import org.corfudb.format.Types.OrchestratorRequestType;

/**
 *
 * An interface that should be implemented by all the orchestrator service requests.
 *
 * @author Maithem
 */
public interface Request {

    /**
     * Returns the type of the request.
     * @return type of request
     */
    OrchestratorRequestType getType();

    /**
     * Serialize this request into a ByteBuf
     * @return serialized bytes of the request
     */
    byte[] getSerialized();
}
