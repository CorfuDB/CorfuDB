package org.corfudb.runtime.collections;

import lombok.Getter;

import javax.annotation.Nullable;
import java.util.List;

public class DeltaMessage {
    @Getter
    private final DataLocator dataLocator;

    @Getter
    private final DeltaOperationType operationType;

    /**
     * Opaque payload for log replication.
     *
     */
    @Getter
    private final byte[] payload;

    @Getter
    private final byte[] metaValue;

    /**
     * List of destinations for a transaction.
     *
     */
    @Getter
    private final List<String> destinations;

    /**
     * Defines the enum for the type of the operation.
     *
     */
    public enum DeltaOperationType {
        PUT,
        REMOVE,
        DIFF
    }

    public DeltaMessage(DataLocator dataLocator, DeltaOperationType operationType, byte[] payload, @Nullable byte[] metaValue, List<String> destinations) {
        this.dataLocator = dataLocator;
        this.operationType = operationType;
        this.metaValue = metaValue;
        this.payload = payload;
        this.destinations = destinations;
    }
}
