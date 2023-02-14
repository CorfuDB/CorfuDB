package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.CorfuRuntime;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams. The set of streams to replicate
 * will be synced by the config at the start of a log entry sync and when a new stream to replicate is discovered.
 */
public class StreamsLogEntryReader extends BaseLogEntryReader {

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationSession replicationSession,
                                 LogReplicationContext replicationContext) {
        super(runtime, replicationSession, replicationContext);
    }

    @Override
    protected void refreshStreamUUIDs() {
        Set<String> streams = replicationContext.getConfig(session).getStreamsToReplicate();
        streamUUIDs = new HashSet<>();
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }
    }
}
