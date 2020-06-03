package org.corfudb.logreplication.send;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class LogEntrySenderBufferManager extends SenderBufferManager {

    public LogEntrySenderBufferManager(DataSender dataSender) {
        super(dataSender);
    }

    @Override
    public void addCFToAcked(LogReplicationEntry message, CompletableFuture<LogReplicationEntry> cf) {
        pendingLogEntriesAcked.put(message.getMetadata().getTimestamp(), cf);
    }

    @Override
    public void updateAckTs(Long newAck) {
        if (newAck <= ackTs) {
            return;
        }

        ackTs = newAck;
        pendingEntries.evictAccordingToTimestamp(ackTs);
        pendingLogEntriesAcked = pendingLogEntriesAcked.entrySet().stream()
                .filter(entry -> entry.getKey() > ackTs)
                .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
    }

    @Override
    public void updateAckTs(LogReplicationEntry entry) {
        updateAckTs(entry.getMetadata().getTimestamp());
    }
}
