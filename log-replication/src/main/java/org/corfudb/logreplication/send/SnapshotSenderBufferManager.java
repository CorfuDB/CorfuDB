package org.corfudb.logreplication.send;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class SnapshotSenderBufferManager extends SenderBufferManager {
    public SnapshotSenderBufferManager(DataSender dataSender) {
        super(dataSender);
    }

    @Override
    public void updateAckTs(Long newAck) {
        if (ackTs > newAck)
            return;
        ackTs = newAck;
        pendingEntries.evictAccordingToSeqNum(ackTs);
        pendingLogEntriesAcked = pendingLogEntriesAcked.entrySet().stream()
                .filter(entry -> entry.getKey() > ackTs)
                .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
        System.out.print("\nSnapshot Sender received ack " + newAck);
    }

    @Override
    public void updateAckTs(LogReplicationEntry entry) {
        updateAckTs(entry.getMetadata().getSnapshotSyncSeqNum());
    }

    @Override
    public void addCFToAcked(LogReplicationEntry message, CompletableFuture<LogReplicationEntry> cf) {
        pendingLogEntriesAcked.put(message.getMetadata().getSnapshotSyncSeqNum(), cf);
    }
}
