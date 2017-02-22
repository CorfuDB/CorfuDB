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
public enum FillHoleMode implements ICorfuPayload<FillHoleMode> {
    NORMAL((byte)0),
    QUORUM_STEADY((byte)4),
    QUORUM_PHASE1((byte)5),
    QUORUM_PHASE2((byte)6),
    QUORUM_FORCE_OVERWRITE((byte)7);

    final int val;

    byte asByte() {
        return (byte) val;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeByte(asByte());
    }

    static Map<Byte, FillHoleMode> typeMap =
            Arrays.stream(FillHoleMode.values())
                    .collect(Collectors.toMap(FillHoleMode::asByte, Function.identity()));
}
