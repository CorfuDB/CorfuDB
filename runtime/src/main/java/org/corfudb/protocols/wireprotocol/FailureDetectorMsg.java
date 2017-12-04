package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Trigger sent to the management server with the failures detected.
 * Created by zlokhandwala on 11/8/16.
 */
@Data
@AllArgsConstructor
public class FailureDetectorMsg implements ICorfuPayload<FailureDetectorMsg> {
    private Set<String> failedNodes;
    private Set<String> healedNodes;

    public FailureDetectorMsg(ByteBuf buf) {
        failedNodes = ICorfuPayload.setFromBuffer(buf, String.class);
        healedNodes = ICorfuPayload.setFromBuffer(buf, String.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, failedNodes);
        ICorfuPayload.serialize(buf, healedNodes);
    }
}
