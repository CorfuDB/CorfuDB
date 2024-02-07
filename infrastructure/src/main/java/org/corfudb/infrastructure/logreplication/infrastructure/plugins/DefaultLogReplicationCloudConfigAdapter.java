package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.TxnContext;

/**
 * Default implementation of a Log Replication Config Provider for cloud testing deployments.
 */

public class DefaultLogReplicationCloudConfigAdapter implements ILogReplicationVersionAdapter {
    private static final String latestVer = "LATEST-VERSION";

    public void openVersionTable(CorfuRuntime runtime) {
        if (runtime == null) {
            throw new IllegalArgumentException("Null runtime passed in");
        }
    }

    @Override
    public String getNodeVersion() {
        return latestVer;
    }

    @Override
    public String getPinnedClusterVersion(TxnContext txnContext) {
        return latestVer;
    }

    @Override
    public boolean isSaasDeployment() {
        return true;
    }
}
