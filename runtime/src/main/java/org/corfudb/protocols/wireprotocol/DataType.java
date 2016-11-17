package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 8/16/16.
 */
@RequiredArgsConstructor
public enum DataType implements ICorfuPayload<DataType> {
    DATA(0),
    EMPTY(1),
    HOLE(2),
    TRIMMED(3);

    final int val;

    byte asByte() {
        return (byte) val;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeByte(asByte());
    }

    static Map<Byte, DataType> typeMap =
            Arrays.stream(DataType.values())
                    .collect(Collectors.toMap(DataType::asByte, Function.identity()));

}
