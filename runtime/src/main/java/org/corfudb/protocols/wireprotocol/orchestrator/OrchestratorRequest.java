package org.corfudb.protocols.wireprotocol.orchestrator;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

import org.corfudb.format.Types.OrchestratorRequestType;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

import static org.corfudb.format.Types.OrchestratorRequestType.ADD_NODE;

/**
 * A message container that encapsulates all the orchestrator's
 * requests.
 *
 * Created by Maithem on 10/25/17.
 */

public class OrchestratorRequest implements ICorfuPayload<OrchestratorRequest> {

    @Getter
    public final Request request;

    public OrchestratorRequest(Request request) {
        this.request = request;
    }

    public OrchestratorRequest(ByteBuf buf) {
        OrchestratorRequestType requestType = OrchestratorRequestType.forNumber(buf.readInt());
        byte[] bytes = new byte[buf.readInt()];
        buf.readBytes(bytes);
        request = mapRequest(requestType, bytes);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeInt(request.getType().getNumber());
        byte[] bytes = request.getSerialized();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    static Request mapRequest(OrchestratorRequestType type, byte[] payload) {
        switch (type) {
            case ADD_NODE:
                return new AddNodeRequest(payload);
            case QUERY:
                return new QueryRequest(payload);
            default:
                throw new IllegalStateException("mapRequest: Unknown Type");
        }
    }
}
