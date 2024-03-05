package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.TxnContext;

/**
 * Default testing implementation of a Log Replication Config Provider
 */
public class DefaultLogReplicationConfigAdapter implements ILogReplicationVersionAdapter {

    private static final String latestVer = "LATEST-VERSION";

    public DefaultLogReplicationConfigAdapter(CorfuRuntime runtime) {
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
}
