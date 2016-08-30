package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;

import java.util.*;

/**
 * Created by atai on 8/26/16.
 */
@Builder
@AllArgsConstructor
public class CommitRequest implements ICorfuPayload<CommitRequest> {

    @Getter
    final Map<UUID, Long> streams;
    @Getter
    final Long address;
    @Getter
    final Boolean commit;

    @SuppressWarnings("unchecked")
    public CommitRequest(ByteBuf buf) {
        if (ICorfuPayload.fromBuffer(buf, Boolean.class)) {
            streams = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        } else { streams = null; }
        address = ICorfuPayload.fromBuffer(buf, Long.class);
        commit = ICorfuPayload.fromBuffer(buf, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, streams != null);
        if (streams != null) {
            ICorfuPayload.serialize(buf, streams);
        }
        ICorfuPayload.serialize(buf, address);
        ICorfuPayload.serialize(buf, commit);
    }
}
