package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationError;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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
     * @return {@link CompletableFuture} containing the acknowledgement.
     */
    CompletableFuture<LogReplicationEntryMsg> send(LogReplicationEntryMsg message);

    /**
     * Application callback on next available message for transmission to remote cluster,
     * using specified timeout time.
     *
     * @param message LogReplicationEntry representing the data to send across sites.
     * @param timeoutResponse Time allotted to receive ACK from client before completing exceptionally.
     * @return {@link CompletableFuture} containing the acknowledgement.
     */
    CompletableFuture<LogReplicationEntryMsg> sendWithTimeout(LogReplicationEntryMsg message, long timeoutResponse);

    /**
     * Application callback on next available messages for transmission to remote cluster.
     *
     * @param messages list of LogReplicationEntry representing the data to send across sites.
     * @return {@link CompletableFuture} containing the acknowledgement.
     */
    CompletableFuture<LogReplicationEntryMsg> send(List<LogReplicationEntryMsg> messages);


    /**
     * Send metadata request to remote cluster
     *
     * @return metadata response.
     */
    CompletableFuture<LogReplicationMetadataResponseMsg> sendMetadataRequest();

    /**
     * Application callback on error.
     *
     * @param error log replication error
     */
    void onError(LogReplicationError error);
}
