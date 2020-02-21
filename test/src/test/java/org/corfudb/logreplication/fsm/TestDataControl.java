package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.DataControl;

public class TestDataControl implements DataControl {

    @Override
    public void requestSnapshotSync() {
        // Empty - No-Op
    }
}
