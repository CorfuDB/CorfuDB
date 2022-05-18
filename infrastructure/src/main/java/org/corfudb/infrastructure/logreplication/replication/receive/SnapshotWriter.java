package org.corfudb.infrastructure.logreplication.replication.receive;

import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;

import java.util.List;

/**
 * The snapshot full sync engine will call the snapshot writer api apply to apply messages it has received.
 * It will guarantee the ordering of the message that pass to the snapshot writer.
 */
public interface SnapshotWriter {
    // The snapshot full sync engine will pass a message to the snapshot writer
    void apply(LogReplicationEntryMsg message, String id) throws Exception;

    void apply(List<LogReplicationEntryMsg> messages) throws Exception;
}
