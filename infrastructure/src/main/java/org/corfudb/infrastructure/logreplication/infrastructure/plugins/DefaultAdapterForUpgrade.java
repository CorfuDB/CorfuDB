package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import com.google.common.annotations.VisibleForTesting;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.TxnContext;

public abstract class DefaultAdapterForUpgrade implements ILogReplicationVersionAdapter {

    public static final String NAMESPACE = "LR-Test";
    public static final String LATEST_VERSION = "LATEST-VERSION";
    public static final String OLDER_VERSION = "OLDER-VERSION";
    public static final String DEFAULT_VERSION = "version_latest";

    String versionString;

    String pinnedVersionString;

    public DefaultAdapterForUpgrade(CorfuRuntime runtime) {
        versionString = DEFAULT_VERSION;
        pinnedVersionString = DEFAULT_VERSION;
    }

    @Override
    public String getNodeVersion() {
        return versionString;
    }

    @Override
    public String getPinnedClusterVersion(TxnContext txnContext) {
        return pinnedVersionString;
    }

    /**
     * This method should be used only for facilitating validation of the upgrade process
     * in tests and ITs
     *
     * @param corfuStore - please supply the appropriate instance from the
     *                   cluster of interest (source / sink)
     */
    @VisibleForTesting
    public void startRollingUpgrade() {
        versionString = LATEST_VERSION;
        pinnedVersionString = OLDER_VERSION;
    }

    /**
     * This method should be used only for facilitating validation of the upgrade process
     * in tests and ITs and it simulates end of upgrade by setting pinned version as node version
     *
     * @param corfuStore - please supply the appropriate instance from the
     *                   cluster of interest (source / sink)
     */
    @VisibleForTesting
    public void endRollingUpgrade() {
        versionString = LATEST_VERSION;
        pinnedVersionString = LATEST_VERSION;
    }
}
