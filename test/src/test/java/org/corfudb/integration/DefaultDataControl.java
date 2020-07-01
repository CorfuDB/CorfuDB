package org.corfudb.integration;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataControl;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;

import static org.assertj.core.api.Assertions.fail;

/**
 * Test Default Data Control Implementation, used for source and sink (destination) nodes.
 */
public class DefaultDataControl implements DataControl {

    @Setter
    private LogReplicationSourceManager sourceManager;

    private int controlCallsCount = 0;

    private DefaultDataControlConfig config;

    @Getter
    private ObservableValue controlCalls = new ObservableValue(controlCallsCount);

    public DefaultDataControl(DefaultDataControlConfig config) {
        this.config = config;
    }

    @Override
    public void requestSnapshotSync() {
        // Increase counter used for testing purposes
        controlCallsCount++;
        controlCalls.setValue(controlCallsCount);

        if (sourceManager != null && config.dropSnapshotSyncRequestMessage && config.dropCount >= controlCallsCount) {
            // Drop Snapshot Sync Request Message, because the request is not satisfied,
            // this should be periodically re-triggered
            // System.out.println("----- Drop snapshot sync request: " + controlCallsCount);
        } else if (sourceManager != null) {
            // Request/Start Snapshot Sync on Source
            System.out.println("----- Start Snapshot Sync on Source: " + controlCallsCount);
            sourceManager.startSnapshotSync();
        } else {
            fail("Source Manager has not been set for DataControl implementation.");
        }
    }

    public static class DefaultDataControlConfig {

        private boolean dropSnapshotSyncRequestMessage;
        private int dropCount;

        public DefaultDataControlConfig(boolean dropSnapshotSyncRequestMessage, int dropCount) {
            this.dropSnapshotSyncRequestMessage = dropSnapshotSyncRequestMessage;
            this.dropCount = dropCount;
        }
    }
}
