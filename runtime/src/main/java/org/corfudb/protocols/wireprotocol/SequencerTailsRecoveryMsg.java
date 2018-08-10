package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by rmichoud on 6/20/17.
 */
@Data
@AllArgsConstructor
public class SequencerTailsRecoveryMsg implements ICorfuPayload<SequencerTailsRecoveryMsg> {

    private Long globalTail;
    private Map<UUID, Long> streamTails;
    private Long sequencerEpoch;

    /**
     * Boolean flag to denote whether this bootstrap message is just updating an existing primary
     * sequencer with the new epoch (if set to true) or bootstrapping a currently NOT_READY
     * sequencer.
     */
    private Boolean bootstrapWithoutTailsUpdate;

    public SequencerTailsRecoveryMsg(ByteBuf buf) {
        globalTail = ICorfuPayload.fromBuffer(buf, Long.class);
        streamTails = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        sequencerEpoch = ICorfuPayload.fromBuffer(buf, Long.class);
        bootstrapWithoutTailsUpdate = ICorfuPayload.fromBuffer(buf, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {

        ICorfuPayload.serialize(buf, globalTail);
        ICorfuPayload.serialize(buf, streamTails);
        ICorfuPayload.serialize(buf, sequencerEpoch);
        ICorfuPayload.serialize(buf, bootstrapWithoutTailsUpdate);
    }
}

