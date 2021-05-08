package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryAckMsg;


/**
 * Test Implementation of Snapshot Data Sender which emulates sending messages by placing directly
 * in an entry queue, and sends ACKs right away.
 */
public class TestDataSender implements DataSender {

    @Getter
    private final Queue<LogReplicationEntryMsg> entryQueue = new LinkedList<>();

    private long snapshotSyncBaseSnapshot = 0;

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(LogReplicationEntryMsg message) {
        if (message != null && !message.getData().isEmpty() &&
                (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_MESSAGE) ||
                    message.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_MESSAGE))) {
            // Ignore, do not account Start and End Markers as messages
            entryQueue.add(message);
        }

        CompletableFuture<LogReplicationEntryMsg> cf = new CompletableFuture<>();
        LogReplicationEntryMetadataMsg.Builder ackMetadata =
                LogReplicationEntryMetadataMsg.newBuilder().mergeFrom(message.getMetadata());

        // Emulate behavior from Sink, send ACK per received message
        if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_END)) {
            snapshotSyncBaseSnapshot = message.getMetadata().getSnapshotTimestamp();
            ackMetadata.setEntryType(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE);
        } else if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_MESSAGE)) {
            ackMetadata.setEntryType(LogReplicationEntryType.SNAPSHOT_REPLICATED);
        } else if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_MESSAGE)) {
            ackMetadata.setEntryType(LogReplicationEntryType.LOG_ENTRY_REPLICATED);
        } else {
            // Do not send an ACK for start markers
            return cf;
        }

        LogReplicationEntryMsg ack = getLrEntryAckMsg(ackMetadata.build());
        cf.complete(ack);

        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(List<LogReplicationEntryMsg> messages) {

        CompletableFuture<LogReplicationEntryMsg> lastSentMessage = new CompletableFuture<>();

        if (messages != null && !messages.isEmpty()) {
            CompletableFuture<LogReplicationEntryMsg> tmp;

            for (LogReplicationEntryMsg message : messages) {
                tmp = send(message);
                if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_END) ||
                        message.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_MESSAGE)) {
                    lastSentMessage = tmp;
                }
            }
        }

        return lastSentMessage;
    }

    @Override
    public CompletableFuture<LogReplicationMetadataResponseMsg> sendMetadataRequest() {
        CompletableFuture<LogReplicationMetadataResponseMsg> completableFuture = new CompletableFuture<>();
        LogReplicationMetadataResponseMsg response =
                LogReplicationMetadataResponseMsg.newBuilder()
                .setTopologyConfigID(0)
                .setVersion("version")
                .setSnapshotStart(snapshotSyncBaseSnapshot)
                .setSnapshotTransferred(snapshotSyncBaseSnapshot)
                .setSnapshotApplied(snapshotSyncBaseSnapshot)
                .setLastLogEntryTimestamp(snapshotSyncBaseSnapshot)
                .build();
        completableFuture.complete(response);
        return completableFuture;
    }

    public void reset() {
        entryQueue.clear();
    }

    @Override
    public void onError(LogReplicationError error) {}
}
