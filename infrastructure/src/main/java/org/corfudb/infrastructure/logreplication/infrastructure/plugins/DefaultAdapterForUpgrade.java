package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import com.google.common.annotations.VisibleForTesting;
import org.corfudb.runtime.ExampleSchemas.Uuid;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.utils.LogReplicationStreams.Version;
import org.corfudb.utils.LogReplicationStreams.VersionString;

import java.lang.reflect.InvocationTargetException;

import static org.corfudb.infrastructure.logreplication.utils.LogReplicationUpgradeManager.LOG_REPLICATION_PLUGIN_VERSION_TABLE;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public abstract class DefaultAdapterForUpgrade implements ILogReplicationVersionAdapter {

    public static final String NAMESPACE = "LR-Test";
    public static final String LATEST_VERSION = "LATEST-VERSION";
    public static final String OLDER_VERSION = "OLDER-VERSION";
    public static final String DEFAULT_VERSION = "version_latest";

    String versionString;

    String pinnedVersionString;

    public DefaultAdapterForUpgrade() {
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
     */
    @VisibleForTesting
    public void startRollingUpgrade(CorfuStore corfuStore)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // Open version table to simulate being on an older setup (V1)
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                VersionString.class, Version.class, Uuid.class, TableOptions.builder().build());
        versionString = LATEST_VERSION;
        pinnedVersionString = OLDER_VERSION;
    }

    /**
     * This method should be used only for facilitating validation of the upgrade process
     * in tests and ITs, and it simulates end of upgrade by setting pinned version as node version
     *
     */
    @VisibleForTesting
    public void endRollingUpgrade() {
        versionString = LATEST_VERSION;
        pinnedVersionString = LATEST_VERSION;
    }
}
