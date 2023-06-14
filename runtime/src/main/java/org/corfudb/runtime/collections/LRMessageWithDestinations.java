package org.corfudb.runtime.collections;

import lombok.Getter;

import java.util.List;

public class LRMessageWithDestinations {

    /**
     * Opaque payload for log replication.
     */
    @Getter
    private final byte[] payload;

    /**
     * List of destinations for a transaction.
     */
    @Getter
    private final List<String> destinations;

    public LRMessageWithDestinations(byte[] payload, List<String> destinations) {
        this.payload = payload;
        this.destinations = destinations;
    }
}
