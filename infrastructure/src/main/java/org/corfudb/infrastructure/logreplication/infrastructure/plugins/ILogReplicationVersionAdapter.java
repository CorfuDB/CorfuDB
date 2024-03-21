package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.collections.TxnContext;

/**
 * This Interface must be implemented by any external
 * provider to give the System's version
 */
public interface ILogReplicationVersionAdapter {

    /**
     * Returns a version string that indicates the version of LR
     * As per the currently deployed codebase LR is executing under.
     *
     * @return Version string that indicates the current version of LR.
     */
    String getNodeVersion();

    /**
     * When cluster is in the rolling upgrade phase some nodes are
     * running the previous code base so until all the nodes are upgraded
     * the cluster's version is "pinned" to the prior version.
     *
     * @param txnContext - pass the transaction within which the determination needs to happen
     * @return the pinned cluster version which can be compared with the
     * getNodeVersion() to determine if rolling upgrade has completed.
     */
    String getPinnedClusterVersion(TxnContext txnContext);
}
