package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.send.logreader.SnapshotReadMessage;
import org.corfudb.logreplication.send.logreader.SnapshotReader;

import java.util.Collections;
import java.util.UUID;

/**
 * Empty Implementation of Snapshot Reader - used for state machine transition testing (no logic)
 */
public class EmptySnapshotReader implements SnapshotReader {
    @Override
    public SnapshotReadMessage read(UUID snapshotRequestId) {
        return new SnapshotReadMessage(Collections.EMPTY_LIST, true);
    }

    @Override
    public void reset(long snapshotTimestamp) {

    }

    @Override
    public void setSiteEpoch(long siteEpoch) {

    }
}
