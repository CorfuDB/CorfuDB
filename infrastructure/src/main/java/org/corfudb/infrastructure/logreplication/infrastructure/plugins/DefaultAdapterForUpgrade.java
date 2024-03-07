package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import com.google.common.annotations.VisibleForTesting;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams;

public abstract class DefaultAdapterForUpgrade implements ILogReplicationVersionAdapter {

    public static final String VERSION_TEST_TABLE = "VersionTestTable";

    public static final String NAMESPACE = "LR-Test";

    public static final String PINNED_VERSION_KEY = "PINNED_VERSION_KEY";

    public static final String LATEST_VERSION = "LATEST-VERSION";

    /* Represents the current code's version
     */
    public static final String NODE_VERSION_KEY = "VERSION";

    String versionString = "version_latest";

    String pinnedVersionString = "version_latest";
    CorfuStore corfuStore;

    public DefaultAdapterForUpgrade(CorfuRuntime runtime) {
        this.corfuStore = new CorfuStore(runtime);
        openVersionTable(corfuStore);
    }

    @Override
    public String getNodeVersion() {
        // For local testing we could be either within a transaction..
        if (TransactionalContext.isInTransaction()) {
            getVersion(TransactionalContext.getRootContext().getTxnContext());
        } else { // or invoking this independently outside of a transaction..
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                getVersion(txn);
                txn.commit();
            } catch (Exception e) {
                // Just to wrap this up
            }
        }
        return versionString;
    }

    private void getVersion(TxnContext txn) {
        LogReplicationStreams.VersionString versionStringKey =
                LogReplicationStreams.VersionString.newBuilder()
                        .setName(NODE_VERSION_KEY).build();
        versionString =
                ((LogReplicationStreams.Version)txn.getRecord(VERSION_TEST_TABLE,
                        versionStringKey).getPayload()).getVersion();
    }

    private void getPinnedVersionString(TxnContext txn) {
        LogReplicationStreams.VersionString versionStringKey =
                LogReplicationStreams.VersionString.newBuilder()
                        .setName(PINNED_VERSION_KEY).build();
        pinnedVersionString =
                ((LogReplicationStreams.Version)txn.getRecord(VERSION_TEST_TABLE,
                        versionStringKey).getPayload()).getVersion();
    }

    @Override
    public String getPinnedClusterVersion(TxnContext txnContext) {
        getPinnedVersionString(txnContext);
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
    public void startRollingUpgrade(CorfuStore corfuStore) {
        Table<LogReplicationStreams.VersionString, LogReplicationStreams.Version, CommonTypes.Uuid>
                versionStringVersionUuidTable = openVersionTable(corfuStore);

        LogReplicationStreams.VersionString versionStringKey =
                LogReplicationStreams.VersionString.newBuilder()
                        .setName(NODE_VERSION_KEY).build();
        LogReplicationStreams.Version versionPayload =
                LogReplicationStreams.Version.newBuilder()
                        .setVersion(LATEST_VERSION)
                        .build();

        LogReplicationStreams.VersionString pinnedVersionKey =
                LogReplicationStreams.VersionString.newBuilder()
                        .setName(PINNED_VERSION_KEY).build();
        LogReplicationStreams.Version pinnedVersionPayload =
                LogReplicationStreams.Version.newBuilder()
                        .setVersion("OLDER-VERSION")
                        .build();
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(versionStringVersionUuidTable, versionStringKey, versionPayload, null);
            txn.putRecord(versionStringVersionUuidTable, pinnedVersionKey, pinnedVersionPayload, null);
            txn.commit();
        } catch (Exception e) {
            // Just for wrap this up
        }
    }

    private Table<LogReplicationStreams.VersionString, LogReplicationStreams.Version, CommonTypes.Uuid>
    openVersionTable(CorfuStore corfuStore) {
        Table<LogReplicationStreams.VersionString, LogReplicationStreams.Version, CommonTypes.Uuid>
                versionStringVersionUuidTable = null;
        try {
            versionStringVersionUuidTable = corfuStore.openTable(NAMESPACE, VERSION_TEST_TABLE,
                    LogReplicationStreams.VersionString.class,
                    LogReplicationStreams.Version.class, CommonTypes.Uuid.class,
                    TableOptions.builder().build());
        } catch (Exception e) {
            // Just for wrap this up
        }
        return versionStringVersionUuidTable;
    }

    /**
     * This method should be used only for facilitating validation of the upgrade process
     * in tests and ITs and it simulates end of upgrade by setting pinned version as node version
     *
     * @param corfuStore - please supply the appropriate instance from the
     *                   cluster of interest (source / sink)
     */
    @VisibleForTesting
    public void endRollingUpgrade(CorfuStore corfuStore) {
        Table<LogReplicationStreams.VersionString, LogReplicationStreams.Version, CommonTypes.Uuid>
                versionStringVersionUuidTable = openVersionTable(corfuStore);

        LogReplicationStreams.VersionString versionStringKey =
                LogReplicationStreams.VersionString.newBuilder()
                        .setName(NODE_VERSION_KEY).build();
        LogReplicationStreams.Version versionPayload =
                LogReplicationStreams.Version.newBuilder()
                        .setVersion(LATEST_VERSION)
                        .build();

        LogReplicationStreams.VersionString pinnedVersionKey =
                LogReplicationStreams.VersionString.newBuilder()
                        .setName(PINNED_VERSION_KEY).build();
        LogReplicationStreams.Version pinnedVersionPayload =
                LogReplicationStreams.Version.newBuilder()
                        .setVersion(LATEST_VERSION)
                        .build();
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(versionStringVersionUuidTable, versionStringKey, versionPayload, null);
            txn.putRecord(versionStringVersionUuidTable, pinnedVersionKey, pinnedVersionPayload, null);
            txn.commit();
        } catch (Exception e) {
            // Just for wrap this up
        }
    }
}
