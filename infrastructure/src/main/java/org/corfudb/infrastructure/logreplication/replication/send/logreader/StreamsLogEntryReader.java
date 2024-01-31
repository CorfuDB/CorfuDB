package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams. The set of streams to replicate
 * will be synced by the config at the start of a log entry sync and when a new stream to replicate is discovered.
 */
public class StreamsLogEntryReader extends BaseLogEntryReader {

    public StreamsLogEntryReader(LogReplicationSession replicationSession, LogReplicationContext replicationContext) {
        super(replicationSession, replicationContext);
    }
}
