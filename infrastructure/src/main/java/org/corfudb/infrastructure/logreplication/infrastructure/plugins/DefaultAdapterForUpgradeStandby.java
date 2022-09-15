package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;

/**
 * This class is created for LR rolling upgrade tests to serve as the plugin for Standby cluster to
 * get the set of streams to replicate.
 */
public class DefaultAdapterForUpgradeStandby extends DefaultAdapterForUpgrade {

    public DefaultAdapterForUpgradeStandby() {
        String ENDPOINT = "localhost:9001";
        CorfuRuntime runtime =
            CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                    .parseConfigurationString(ENDPOINT).connect();
        corfuStore = new CorfuStore(runtime);
        init();
    }
}
