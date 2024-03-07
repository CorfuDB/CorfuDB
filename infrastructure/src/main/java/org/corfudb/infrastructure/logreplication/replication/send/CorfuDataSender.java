package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.protobuf.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClient;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class CorfuDataSender implements DataSender {

    private final LogReplicationClient client;

    public CorfuDataSender(LogReplicationClient client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(LogReplicationEntryMsg message) {
        log.trace("Send single log entry for request {}", TextFormat.shortDebugString(message.getMetadata()));
        return client.sendLogEntry(message);
    }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(List<LogReplicationEntryMsg> messages) {
        log.trace("Send multiple log entries [{}] for request {}", messages.size(), messages.get(0).getMetadata().getSyncRequestId());
        CompletableFuture<LogReplicationEntryMsg> lastSentMessage = new CompletableFuture<>();
        CompletableFuture<LogReplicationEntryMsg> tmp;

        for (LogReplicationEntryMsg message :  messages) {
            tmp = send(message);
            if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_END) ||
                    message.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_MESSAGE)) {
                lastSentMessage = tmp;
            }
        }

        return lastSentMessage;
    }

    @Override
    public CompletableFuture<LogReplicationMetadataResponseMsg> sendMetadataRequest() {
        return client.sendMetadataRequest();
    }

    @Override
    public void onError(LogReplicationError error) {}
}
