package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.transmitter.SnapshotReadMessage;
import org.corfudb.logreplication.transmitter.SnapshotReader;

import java.util.Collections;

/**
 * Empty Implementation of Snapshot Reader - used for state machine transition testing (no logic)
 */
public class EmptySnapshotReader implements SnapshotReader {
    @Override
    public SnapshotReadMessage read() {
        return new SnapshotReadMessage(Collections.EMPTY_LIST, false);
    }

    @Override
    public void reset(long snapshotTimestamp) {

    }
}
