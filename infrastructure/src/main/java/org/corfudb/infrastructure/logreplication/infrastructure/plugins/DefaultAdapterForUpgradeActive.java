package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;

/**
 * This class is created for LR rolling upgrade tests to serve as the plugin for Active cluster to
 * get the set of streams to replicate.
 */
public class DefaultAdapterForUpgradeActive extends DefaultAdapterForUpgrade {

    public DefaultAdapterForUpgradeActive() {
        String ENDPOINT = "localhost:9000";
        CorfuRuntime runtime =
            CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                    .parseConfigurationString(ENDPOINT).connect();
        this.corfuStore = new CorfuStore(runtime);
        init();
    }
}
