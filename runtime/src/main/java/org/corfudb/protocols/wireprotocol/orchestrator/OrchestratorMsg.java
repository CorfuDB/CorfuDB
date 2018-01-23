package org.corfudb.protocols.wireprotocol.orchestrator;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

import org.corfudb.protocols.wireprotocol.ICorfuPayload;


/**
 * A message container that encapsulates all the orchestrator's
 * requests.
 *
 * Created by Maithem on 10/25/17.
 */

public class OrchestratorMsg implements ICorfuPayload<OrchestratorMsg> {

    @Getter
    public final Request request;

    public OrchestratorMsg(Request request) {
        this.request = request;
    }

    public OrchestratorMsg(ByteBuf buf) {
        OrchestratorRequestType requestType = OrchestratorRequestType.typeMap.get(buf.readInt());
        byte[] bytes = new byte[buf.readInt()];
        buf.readBytes(bytes);
        request = requestType.getRequestGenerator().apply(bytes);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeInt(request.getType().getType());
        byte[] bytes = request.getSerialized();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
}
