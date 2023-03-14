package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.protobuf.TextFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter.REMOTE_LEADER;

@Slf4j
public class CorfuDataSender implements DataSender {

    private final IClientServerRouter router;

    @Getter
    private final LogReplication.LogReplicationSession session;

    public CorfuDataSender(IClientServerRouter router, LogReplication.LogReplicationSession session) {
        this.router = router;
        this.session = session;
    }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(LogReplicationEntryMsg message) {
        log.trace("Send single log entry for request {}", TextFormat.shortDebugString(message.getMetadata()));
        CorfuMessage.RequestPayloadMsg payload =
                CorfuMessage.RequestPayloadMsg.newBuilder()
                        .setLrEntry(message)
                        .build();
        return router.sendRequestAndGetCompletable(session, payload, REMOTE_LEADER);
    }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(List<LogReplicationEntryMsg> messages) {
        log.trace("Send multiple log entries [{}] for request {}", messages.size(),
                messages.get(0).getMetadata().getSyncRequestId());
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
        CorfuMessage.RequestPayloadMsg payload =
                CorfuMessage.RequestPayloadMsg.newBuilder()
                        .setLrMetadataRequest(LogReplication.LogReplicationMetadataRequestMsg.newBuilder().build())
                        .build();
        return router.sendRequestAndGetCompletable(session, payload, REMOTE_LEADER);
    }

    @Override
    public void onError(LogReplicationError error) {}
}
