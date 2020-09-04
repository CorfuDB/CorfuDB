package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

/**
 * Default Snapshot Sync Plugin
 *
 * This implementation returns immediately for start and end of a snapshot sync.
 */
@Slf4j
public class DefaultSnapshotSyncPlugin implements ISnapshotSyncPlugin {

    @Override
    public void onSnapshotSyncStart(CorfuRuntime runtime) {
        log.debug("onSnapshotSyncStart :: OK");
    }

    @Override
    public void onSnapshotSyncEnd(CorfuRuntime runtime) {
        log.debug("onSnapshotSyncEnd :: OK");
    }
}
