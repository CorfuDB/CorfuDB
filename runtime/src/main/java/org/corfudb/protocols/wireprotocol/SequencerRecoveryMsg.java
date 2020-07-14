package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

/** Created by rmichoud on 6/20/17. */
@Data
@AllArgsConstructor
public class SequencerRecoveryMsg implements ICorfuPayload<SequencerRecoveryMsg> {

  private final Long globalTail;
  private final Map<UUID, StreamAddressSpace> streamsAddressMap;
  private final Long sequencerEpoch;

  /**
   * Boolean flag to denote whether this bootstrap message is just updating an existing primary
   * sequencer with the new epoch (if set to true) or bootstrapping a currently NOT_READY sequencer.
   */
  private Boolean bootstrapWithoutTailsUpdate;

  public SequencerRecoveryMsg(ByteBuf buf) {
    globalTail = ICorfuPayload.fromBuffer(buf, Long.class);
    streamsAddressMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, StreamAddressSpace.class);
    sequencerEpoch = ICorfuPayload.fromBuffer(buf, Long.class);
    bootstrapWithoutTailsUpdate = ICorfuPayload.fromBuffer(buf, Boolean.class);
  }

  @Override
  public void doSerialize(ByteBuf buf) {

    ICorfuPayload.serialize(buf, globalTail);
    ICorfuPayload.serialize(buf, streamsAddressMap);
    ICorfuPayload.serialize(buf, sequencerEpoch);
    ICorfuPayload.serialize(buf, bootstrapWithoutTailsUpdate);
  }
}
