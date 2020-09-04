package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *  Empty Implementation of Snapshot Listener - used for state machine transition testing (no logic)
 */
public class EmptyDataSender implements DataSender {

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) { return new CompletableFuture<>(); }

    @Override
    public CompletableFuture<LogReplicationEntry> send(List<LogReplicationEntry> messages) { return new CompletableFuture<>(); }

    @Override
    public CompletableFuture<LogReplicationMetadataResponse> sendMetadataRequest() { return new CompletableFuture<>(); }

    @Override
    public void onError(LogReplicationError error) {}
}
