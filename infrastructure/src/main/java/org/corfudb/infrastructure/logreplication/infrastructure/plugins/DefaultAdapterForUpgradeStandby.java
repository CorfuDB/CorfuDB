package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;

/**
 * This class is created for LR rolling upgrade tests to serve as the plugin for Standby cluster to
 * get the set of streams to replicate.
 */
public class DefaultAdapterForUpgradeStandby extends DefaultAdapterForUpgrade {
    public DefaultAdapterForUpgradeStandby(CorfuRuntime runtime) {
        super(runtime);
    }
}
