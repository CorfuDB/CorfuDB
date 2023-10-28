package org.corfudb.infrastructure.logreplication.replication.receive;

import org.corfudb.runtime.LogReplication;

import java.util.concurrent.ExecutorService;

/**
 * This Interface comprises Data Path receive operations for both Source and Sink.
 */
public interface DataReceiver {
    LogReplication.LogReplicationEntryMsg receive(LogReplication.LogReplicationEntryMsg message);
}
