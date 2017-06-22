package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;

/**
 * Created by mwei on 8/9/16.
 */
@RequiredArgsConstructor
public enum WriteMode implements ICorfuPayload<WriteMode> {
    NORMAL((byte) 0);

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
