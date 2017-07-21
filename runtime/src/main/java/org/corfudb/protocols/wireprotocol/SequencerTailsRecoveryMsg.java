package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.UUID;

/**
 * Created by rmichoud on 6/20/17.
 */
@Data
@AllArgsConstructor
public class SequencerTailsRecoveryMsg implements ICorfuPayload<SequencerTailsRecoveryMsg> {

    private Long globalTail;
    private Map<UUID, Long> streamTails;
    private Long readyStateEpoch;

    public SequencerTailsRecoveryMsg(ByteBuf buf) {
        globalTail = ICorfuPayload.fromBuffer(buf, Long.class);
        streamTails = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        readyStateEpoch = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {

        ICorfuPayload.serialize(buf, globalTail);
        ICorfuPayload.serialize(buf, streamTails);
        ICorfuPayload.serialize(buf, readyStateEpoch);
    }
}

