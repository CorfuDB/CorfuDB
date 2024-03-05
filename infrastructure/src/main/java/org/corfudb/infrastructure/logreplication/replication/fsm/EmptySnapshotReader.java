package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Empty Implementation of Snapshot Reader - used for state machine transition testing (no logic)
 */
public class EmptySnapshotReader implements SnapshotReader {
    @Override
    public SnapshotReadMessage read(UUID snapshotRequestId) {
        return new SnapshotReadMessage(new ArrayList<>(), true);
    }

    @Override
    public void reset(long snapshotTimestamp) {

    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {

    }
}
