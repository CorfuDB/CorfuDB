package org.corfudb.runtime;

import org.corfudb.runtime.collections.DeltaMessage;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.List;

public interface LogReplicationRoutingQueueClient {
    /**
     * Transmits Delta Messages.
     * Must be invoked with the same transaction builder that was used for as actual data modification.
     * This method must be used if transaction has multiple messages. Repetitive calls in the same transaction to the
     * transmitDeltaMessages/transmitDeltaMessage won't be supported
     */
    void transmitDeltaMessage(TransactionalContext txn, DeltaMessage message);

    void transmitDeltaMessages(TransactionalContext txn, List<DeltaMessage> messages);
}
