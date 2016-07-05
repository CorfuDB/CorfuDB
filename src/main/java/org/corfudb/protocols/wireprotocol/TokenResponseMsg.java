package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class TokenResponseMsg extends CorfuMsg {
    /**
     * The issued token
     */
    Long token;

    /**
     * A map of backpointers.
     */
    Map<UUID, Long> backpointerMap;
        /* The wire format of the NettyStreamingServerTokenResponse message is below:
            | client ID(16) | request ID(8) |  type(1)  |  token(8) |
            |  MSB  |  LSB  |               |           |           |
            0       7       15              23          24          32
         */

    public TokenResponseMsg(@NonNull Long token, @NonNull Map<UUID, Long> backpointerMap) {
        this.msgType = CorfuMsgType.TOKEN_RES;
        this.token = token;
        this.backpointerMap = backpointerMap;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(this.token);
        buffer.writeShort(backpointerMap.size());
        backpointerMap.entrySet().stream()
                .forEach(e -> {
                    buffer.writeLong(e.getKey().getMostSignificantBits());
                    buffer.writeLong(e.getKey().getLeastSignificantBits());
                    buffer.writeLong(e.getValue());
                });
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        this.token = buffer.readLong();
        short numEntries = buffer.readShort();
        ImmutableMap.Builder<UUID, Long> mb = ImmutableMap.builder();
        for (int i = 0; i < numEntries; i++) {
            UUID id = new UUID(buffer.readLong(), buffer.readLong());
            Long backPointer = buffer.readLong();
            mb.put(id, backPointer);
        }
        backpointerMap = mb.build();
    }
}
