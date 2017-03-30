package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Set;

/**
 * Trigger sent to the management server with the failures detected.
 * Created by zlokhandwala on 11/8/16.
 */
@Data
@AllArgsConstructor
public class FailureDetectorMsg implements ICorfuPayload<FailureDetectorMsg> {
    private Set<String> nodes;

    public FailureDetectorMsg(ByteBuf buf) {
        nodes = ICorfuPayload.setFromBuffer(buf, String.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, nodes);
    }
}
