package org.corfudb.protocols.wireprotocol.orchestrator;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

/**
 * A message container that encapsulates all the orchestrator's
 * responses.
 *
 * Created by Maithem on 10/25/17.
 */

public class OrchestratorResponse implements ICorfuPayload<OrchestratorResponse> {

    @Getter
    public final Response response;

    public OrchestratorResponse(Response response) {
        this.response = response;
    }

    public OrchestratorResponse(ByteBuf buf) {
        OrchestratorResponseType responseType = OrchestratorResponseType.typeMap.get(buf.readInt());
        byte[] bytes = new byte[buf.readInt()];
        buf.readBytes(bytes);
        response = responseType.getResponseGenerator().apply(bytes);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeInt(response.getType().getType());
        byte[] bytes = response.getSerialized();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
}
