package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by mwei on 8/16/16.
 */
@AllArgsConstructor
public enum DataType implements ICorfuPayload<DataType> {
    DATA(0, true),
    EMPTY(1, true),
    HOLE(2, true),
    TRIMMED(3, true),
    RANK_ONLY(4, true);

    final int val;

    @Getter
    private boolean metadataAware;

    byte asByte() {
        return (byte) val;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeByte(asByte());
    }

    public static Map<Byte, DataType> typeMap =
            Arrays.stream(DataType.values())
                    .collect(Collectors.toMap(DataType::asByte, Function.identity()));

}
