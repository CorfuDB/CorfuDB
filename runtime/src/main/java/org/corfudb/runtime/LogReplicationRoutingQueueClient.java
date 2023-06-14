package org.corfudb.runtime;

import org.corfudb.runtime.collections.LRMessageWithDestinations;

import java.util.List;

public interface LogReplicationRoutingQueueClient {
    /**
     * Transmits Delta Messages.
     * Must be invoked with the same transaction builder that was used for as actual data modification.
     * This method must be used if transaction has multiple messages. Repetitive calls in the same transaction to the
     * transmitDeltaMessages/transmitDeltaMessage won't be supported
     */
    void transmitDeltaMessage(byte[] payload, List<String> destinations);

    void transmitDeltaMessages(List<LRMessageWithDestinations> messages);
}