package org.corfudb.logreplication.send;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.logreplication.runtime.LogReplicationClient;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class CorfuDataSender implements DataSender {

    private final LogReplicationClient client;

    public CorfuDataSender(LogReplicationClient client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) {
        log.info("Send single log entry");
        // Todo (hack): I believe we need to somehow keep these CF until any is completed...
        return client.sendLogEntry(message);
    }

    @Override
    public boolean send(List<LogReplicationEntry> messages) {
        log.info("Send multiple log entries");
        messages.forEach(message -> send(message));
        return true;
    }

    @Override
    public void onError(LogReplicationError error) {

    }
}
