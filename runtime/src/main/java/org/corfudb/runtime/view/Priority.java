package org.corfudb.runtime.view;

import lombok.AllArgsConstructor;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * Indicates the priority of LogUnit tasks.
 *
 * Created by Maithem on 6/24/19.
 */
@AllArgsConstructor
public enum Priority {
    NORMAL(0),
    HIGH(1);

    public final int type;

    public byte asByte() {
        return (byte) type;
    }

    public static final Map<Byte, Priority> typeMap =
            Arrays.stream(Priority.values()).collect(Collectors.toMap(Priority::asByte, Function.identity()));
}
