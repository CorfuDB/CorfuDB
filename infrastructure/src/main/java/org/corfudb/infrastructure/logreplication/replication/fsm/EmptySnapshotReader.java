package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Empty Implementation of Snapshot Reader - used for state machine transition testing (no logic)
 */
public class EmptySnapshotReader extends StreamsSnapshotReader {
    public SnapshotReadMessage read(UUID snapshotRequestId) {
        return new SnapshotReadMessage(new ArrayList<>(), true);
    }

    public void reset(long snapshotTimestamp) {

    }

    public void setTopologyConfigId(long topologyConfigId) {

    }
}
