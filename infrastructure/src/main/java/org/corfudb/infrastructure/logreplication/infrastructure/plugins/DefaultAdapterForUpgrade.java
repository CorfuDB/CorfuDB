package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas.Uuid;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.utils.LogReplicationStreams.Version;
import org.corfudb.utils.LogReplicationStreams.VersionString;

public abstract class DefaultAdapterForUpgrade implements ILogReplicationVersionAdapter {

    public static final String VERSION_TEST_TABLE = "VersionTestTable";

@Slf4j
public class DefaultAdapterForUpgrade implements ILogReplicationVersionAdapter {

    private static final String LATEST_VERSION = "LATEST-VERSION";
    private static final String OLDER_VERSION = "OLDER-VERSION";

    private static String versionString = OLDER_VERSION;

    private static String pinnedVersionString = OLDER_VERSION;

    private CorfuStore corfuStore;

    @Override
    public void openVersionTable(CorfuRuntime runtime) {
        corfuStore = new CorfuStore(runtime);
    }

    @Override
    public String getNodeVersion() {
        return versionString;
    }

    @Override
    public String getPinnedClusterVersion(TxnContext txnContext) {
        return pinnedVersionString;
    }

    @Override
    public boolean isSaasDeployment() {
        return false;
    }

    /**
     * This method should be used only for facilitating validation of the upgrade process
     * in tests and ITs
     *
     */
    @VisibleForTesting
    public void startRollingUpgrade() throws IllegalAccessException, NoSuchMethodException,
        InvocationTargetException {
        // Open version table to simulate being on an older setup (V1)
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                VersionString.class, Version.class, Uuid.class, TableOptions.builder().build());
        } catch (Exception e) {
            log.error("Failed to open the version table", e);
            throw e;
        }
        versionString = LATEST_VERSION;
    }

    /**
     * This method should be used only for facilitating validation of the upgrade process
     * in tests and ITs, and it simulates end of upgrade by setting pinned version as node version
     *
     */
    @VisibleForTesting
    public void endRollingUpgrade() {
        pinnedVersionString = LATEST_VERSION;
    }

    @VisibleForTesting
    public void reset() {
        versionString = OLDER_VERSION;
        pinnedVersionString = OLDER_VERSION;
    }
}
