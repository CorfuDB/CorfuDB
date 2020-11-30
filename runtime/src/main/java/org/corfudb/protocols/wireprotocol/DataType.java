package org.corfudb.protocols.wireprotocol;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;

/**
 * Created by mwei on 8/16/16.
 */
@AllArgsConstructor
public enum DataType {
    DATA(0),
    EMPTY(1),
    HOLE(2),
    TRIMMED(3),
    RANK_ONLY(4);

    final int val;

    public byte asByte() {
        return (byte) val;
    }

    public static final Map<Byte, DataType> typeMap =
            Arrays.stream(DataType.values())
                    .collect(Collectors.toMap(DataType::asByte, Function.identity()));
}
