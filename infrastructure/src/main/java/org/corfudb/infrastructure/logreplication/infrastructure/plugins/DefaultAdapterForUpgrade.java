package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams;

public abstract class DefaultAdapterForUpgrade implements ILogReplicationVersionAdapter {

    public static final String VERSION_TEST_TABLE = "VersionTestTable";

    public static final String NAMESPACE = "LR-Test";

    String versionString = "version_latest";
    CorfuStore corfuStore;

    @Override
    public String getVersion() {
        if (corfuStore != null) {
            try {
                corfuStore.openTable(NAMESPACE, VERSION_TEST_TABLE,
                        LogReplicationStreams.VersionString.class,
                        LogReplicationStreams.Version.class, CommonTypes.Uuid.class,
                        TableOptions.builder().build());
            } catch (Exception e) {
                // Just for wrap this up
            }

            LogReplicationStreams.VersionString versionStringKey =
                    LogReplicationStreams.VersionString.newBuilder()
                            .setName("VERSION").build();
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                versionString =
                        ((LogReplicationStreams.Version)txn.getRecord(VERSION_TEST_TABLE,
                                versionStringKey).getPayload()).getVersion();
                txn.commit();
            } catch (Exception e) {
                // Just for wrap this up
            }
        }
        return versionString;
    }
}
