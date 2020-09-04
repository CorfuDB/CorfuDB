package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;


/**
 * Test Implementation of Snapshot Data Sender which emulates sending messages by placing directly
 * in an entry queue, and sends ACKs right away.
 */
public class TestDataSender implements DataSender {

    @Getter
    private Queue<LogReplicationEntry> entryQueue = new LinkedList<>();

    private long snapshotSyncBaseSnapshot = 0;

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) {
        if (message != null && message.getPayload() != null) {
            if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_MESSAGE) ||
                    message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                // Ignore, do not account Start and End Markers as messages
                entryQueue.add(message);
            }
        }

        CompletableFuture<LogReplicationEntry> cf = new CompletableFuture<>();
        LogReplicationEntryMetadata ackMetadata = new LogReplicationEntryMetadata(message.getMetadata());

        // Emulate behavior from Sink, send ACK per received message
        if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_END)) {
            snapshotSyncBaseSnapshot = message.getMetadata().getSnapshotTimestamp();
            ackMetadata.setMessageMetadataType(MessageType.SNAPSHOT_TRANSFER_COMPLETE);
        } else if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_MESSAGE)) {
            ackMetadata.setMessageMetadataType(MessageType.SNAPSHOT_REPLICATED);
        } else if (message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
            ackMetadata.setMessageMetadataType(MessageType.LOG_ENTRY_REPLICATED);
        } else {
            // Do not send an ACK for start markers
            return cf;
        }

        LogReplicationEntry ack = LogReplicationEntry.generateAck(ackMetadata);
        cf.complete(ack);

        return cf;
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(List<LogReplicationEntry> messages) {

        CompletableFuture<LogReplicationEntry> lastSentMessage = new CompletableFuture<>();

        if (messages != null && !messages.isEmpty()) {
            CompletableFuture<LogReplicationEntry> tmp;

            for (LogReplicationEntry message : messages) {
                tmp = send(message);
                if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_END) ||
                        message.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_MESSAGE)) {
                    lastSentMessage = tmp;
                }
            }
        }

        return lastSentMessage;
    }

    @Override
    public CompletableFuture<LogReplicationMetadataResponse> sendMetadataRequest() {
        CompletableFuture<LogReplicationMetadataResponse> completableFuture = new CompletableFuture<>();
        LogReplicationMetadataResponse response =  new LogReplicationMetadataResponse(0, "version",
                snapshotSyncBaseSnapshot, snapshotSyncBaseSnapshot, snapshotSyncBaseSnapshot, snapshotSyncBaseSnapshot);
        completableFuture.complete(response);
        return completableFuture;
    }

    public void reset() {
        entryQueue.clear();
    }

    @Override
    public void onError(LogReplicationError error) {}
}
