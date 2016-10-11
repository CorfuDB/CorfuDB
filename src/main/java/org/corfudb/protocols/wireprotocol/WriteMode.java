package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 8/9/16.
 */
@RequiredArgsConstructor
public enum WriteMode implements ICorfuPayload<WriteMode> {
    NORMAL((byte) 0),
    REPLEX_GLOBAL((byte) 1),
    REPLEX_HYBRID((byte) 2),
    REPLEX_STREAM((byte) 3);

    final int val;

    byte asByte() {
        return (byte) val;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeByte(asByte());
    }

    static Map<Byte, WriteMode> typeMap =
            Arrays.stream(WriteMode.values())
                    .collect(Collectors.toMap(WriteMode::asByte, Function.identity()));
}
