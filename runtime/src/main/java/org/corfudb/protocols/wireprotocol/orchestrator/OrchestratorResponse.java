package org.corfudb.protocols.wireprotocol.orchestrator;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.format.Types.OrchestratorResponseType;
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
        OrchestratorResponseType requestType = OrchestratorResponseType.forNumber(buf.readInt());
        byte[] bytes = new byte[buf.readInt()];
        buf.readBytes(bytes);
        response = mapResponse(requestType, bytes);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeInt(response.getType().getNumber());
        byte[] bytes = response.getSerialized();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }

    static Response mapResponse(OrchestratorResponseType type, byte[] payload) {
        switch (type) {
            case WORKFLOW_STATUS:
                return new QueryResponse(payload);
            case WORKFLOW_CREATED:
                return new CreateWorkflowResponse(payload);
            default:
                throw new IllegalStateException("mapResponse: Unknown Type");
        }
    }
}
