package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * This manages log entry sync messages for buffering and processing ACKs.
 */
@Slf4j
public class LogEntrySenderBufferManager extends SenderBufferManager {
    private LogReplicationAckReader ackReader;

    /**
     * Constructor
     * @param dataSender
     */
    public LogEntrySenderBufferManager(DataSender dataSender, LogReplicationAckReader ackReader) {
        super(dataSender);
        this.ackReader = ackReader;
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
         * If the newAck is not larger than the current maxAckTimestamp, ignore it.
         */
        if (newAck <= maxAckTimestamp) {
            return;
        }
        log.trace("Ack received for Log Entry Sync {}", newAck);
        maxAckTimestamp = newAck;

        // Remove pending messages that have been ACKed.
        pendingMessages.evictAccordingToTimestamp(maxAckTimestamp);

        // Remove CompletableFutures for Acks that has received.
        pendingCompletableFutureForAcks = pendingCompletableFutureForAcks.entrySet().stream()
                .filter(entry -> entry.getKey() > maxAckTimestamp)
                .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
        ackReader.setAckedTsAndSyncType(newAck, ReplicationStatusVal.SyncType.LOG_ENTRY);
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
