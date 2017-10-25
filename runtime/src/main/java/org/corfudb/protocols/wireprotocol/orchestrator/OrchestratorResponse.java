package org.corfudb.protocols.wireprotocol.orchestrator;

import io.netty.buffer.ByteBuf;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

/**
 * Created by Maithem on 10/25/17.
 */

public class OrchestratorResponse implements ICorfuPayload<OrchestratorResponse> {

    public OrchestratorResponse() {

    }

    public OrchestratorResponse(ByteBuf buf) {

    }

    @Override
    public void doSerialize(ByteBuf buf) {

    }
}
