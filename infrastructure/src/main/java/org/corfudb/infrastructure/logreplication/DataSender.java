package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This Interface comprises Data Path send operations for both Source and Sink.
 *
 * Application is expected to transmit messages from source to sink, and ACKs
 * from sink to source.
 */
public interface DataSender {

    /**
     * Application callback on next available message for transmission to remote cluster.
     *
     * @param message LogReplicationEntry representing the data to send across sites.
     * @return
     */
    CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message);


    /**
     * Application callback on next available messages for transmission to remote cluster.
     *
     * @param messages list of LogReplicationEntry representing the data to send across sites.
     * @return
     */
    CompletableFuture<LogReplicationEntry> send(List<LogReplicationEntry> messages);

    /**
     * Used by Snapshot Full Sync while has done with transferring data and waiting for the receiver to finish applying.
     * The sender queries the receiver's status and will do the proper transition.
     * @return
     */
    public LogReplicationQueryMetadataResponse sendQueryMetadata() throws ExecutionException, InterruptedException;

    /**
     * Application callback on error.
     *
     * @param error log replication error
     */
    void onError(LogReplicationError error);
}
