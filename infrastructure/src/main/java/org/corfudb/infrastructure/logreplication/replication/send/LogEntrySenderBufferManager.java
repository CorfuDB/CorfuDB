package org.corfudb.infrastructure.logreplication.replication.send;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This manages log entry sync messages for buffering and processing ACKs.
 */
public class LogEntrySenderBufferManager extends SenderBufferManager {

    /**
     * Constructor
     * @param dataSender
     */
    public LogEntrySenderBufferManager(DataSender dataSender) {
        super(dataSender);
    }

    /**
     * maintains the ACKs. For log entry sync messages, use the message's timestamp as the key
     * to locate the corresponding CompletableFuture.
     * @param message
     * @param cf
     */
    @Override
    public void addCFToAcked(LogReplicationEntry message, CompletableFuture<LogReplicationEntry> cf) {
        pendingCompletableFutureForAcks.put(message.getMetadata().getTimestamp(), cf);
    }

    /**
     * Update the ack. Remove messages and the corresponding CompletableFutures.
     * @param newAck
     */
    @Override
    public void updateAck(Long newAck) {
        /*
         * If the newAck is not larger than the current maxAckForLogEntrySync, ignore it.
         */
        if (newAck <= maxAckForLogEntrySync) {
            return;
        }


        maxAckForLogEntrySync = newAck;

        //Remove pending messages has been ACKed.
        pendingMessages.evictAccordingToTimestamp(maxAckForLogEntrySync);

        //Remove CompletableFutures for Acks that has received.
        pendingCompletableFutureForAcks = pendingCompletableFutureForAcks.entrySet().stream()
                .filter(entry -> entry.getKey() > maxAckForLogEntrySync)
                .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
    }

    /**
     * For log entry sync, use the message's timestamp as the ACK
     * @param entry
     */
    @Override
    public void updateAck(LogReplicationEntry entry) {
        updateAck(entry.getMetadata().getTimestamp());
    }
}
