package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *  Empty Implementation of Snapshot Listener - used for state machine transition testing (no logic)
 */
public class EmptyDataSender implements DataSender {

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(LogReplicationEntryMsg message) { return new CompletableFuture<>(); }

    @Override
    public CompletableFuture<LogReplicationEntryMsg> send(List<LogReplicationEntryMsg> messages) { return new CompletableFuture<>(); }

    @Override
    public CompletableFuture<LogReplication.LogReplicationMetadataResponseMsg> sendMetadataRequest() { return new CompletableFuture<>(); }

    @Override
    public void onError(LogReplicationError error) {}
}
