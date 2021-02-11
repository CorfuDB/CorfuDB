package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
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
        super(dataSender, configureAcksCounter());
        this.ackReader = ackReader;
    }

    /**
     * maintains the ACKs. For log entry sync messages, use the message's timestamp as the key
     * to locate the corresponding CompletableFuture.
     * @param message
     * @param cf
     */
    @Override
    public void addCFToAcked(LogReplicationEntryMsg message, CompletableFuture<LogReplicationEntryMsg> cf) {
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
    public void updateAck(LogReplicationEntryMsg entry) {
        updateAck(entry.getMetadata().getTimestamp());
    }

    private static Optional<AtomicLong> configureAcksCounter() {
        return MeterRegistryProvider.getInstance()
                .map(registry -> registry.gauge("logreplication.acks",
                        ImmutableList.of(Tag.of("replication.type", "logentry")),
                        new AtomicLong(0)));
    }
}
