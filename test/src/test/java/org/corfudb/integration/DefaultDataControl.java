package org.corfudb.integration;

import lombok.Setter;
import org.corfudb.logreplication.DataControl;
import org.corfudb.logreplication.SourceManager;

import static org.assertj.core.api.Assertions.fail;

/**
 * Test Default Data Control Implementation, used for source and sink (destination) nodes.
 */
public class DefaultDataControl implements DataControl {

    @Setter
    private SourceManager sourceManager;

    private boolean source;

    public DefaultDataControl(boolean source) {
        this.source = source;
    }

    @Override
    public void requestSnapshotSync() {
        if (!source && sourceManager != null) {
            // If it represents the destination data control, we should have an instance of the source manager
            //sourceManager.startSnapshotSync();
        } else if (source && sourceManager != null) {
            // If it represents the source data control, we assume the request went over the wire to the destination
            // application side and they re-triggered snapshot sync

            // Keep both paths as we might have different behaviors if it is the source or the destination control
            //sourceManager.startSnapshotSync();
        } else {
            fail("Data Control request Snapshot Sync is not processed.");
        }
    }
}
