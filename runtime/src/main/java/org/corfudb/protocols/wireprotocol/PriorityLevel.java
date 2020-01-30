package org.corfudb.protocols.wireprotocol;

import lombok.AllArgsConstructor;
import org.corfudb.runtime.Messages.CorfuPriorityLevel;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * Indicates the priority of client requests.
 *
 * Created by Maithem on 6/24/19.
 */
@AllArgsConstructor
public enum PriorityLevel {
    // This is the default priority for all requests
    NORMAL(0, CorfuPriorityLevel.NORMAL),

    // The priority for clients that need to bypass quota checks (i.e. management clients, checkpointer)
    HIGH(1, CorfuPriorityLevel.HIGH);

    public final int type;

    public final CorfuPriorityLevel proto;

    public byte asByte() {
        return (byte) type;
    }

    public static PriorityLevel fromProtoType(CorfuPriorityLevel level) {
        for (PriorityLevel priorityLevel: values()) {
            if (priorityLevel.proto == level) {
                return priorityLevel;
            }
        }
        throw new IllegalArgumentException("wrong value " + level);
    }

    public static final Map<Byte, PriorityLevel> typeMap =
            Arrays.stream(PriorityLevel.values()).collect(Collectors.toMap(PriorityLevel::asByte, Function.identity()));
}