package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;

/**
 * This class is created for LR rolling upgrade tests to serve as the plugin for Active cluster to
 * get the set of streams to replicate.
 */
public class DefaultAdapterForUpgradeActive extends DefaultAdapterForUpgrade {

    public DefaultAdapterForUpgradeActive(CorfuRuntime runtime) {
        super(runtime);
    }
}
