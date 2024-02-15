package org.corfudb.infrastructure.logreplication;

import com.google.protobuf.Message;
import org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultAdapterForUpgrade;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler.V1_METADATA_TABLE_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public class RollingUpgradeHandlerTest extends AbstractViewTest {

    private CorfuRuntime corfuRuntime;
    private CorfuStore corfuStore;

    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();
        corfuStore = new CorfuStore(corfuRuntime);
    }

    /**
     * Test if we are able to successfully detect if rolling upgrade is in progress based on the node and cluster
     * versions returned from the plugin.
     * 1. Simulate an ongoing upgrade by invoking startRollingUpgrade() on the plugin
     * 2. verify that rolling upgrade is detected
     * 3. simulate the end of an upgrade by invoking endRollingUpgrade() on the plugin
     * 4. verify that rolling upgrade is no longer detected
     *
     * @throws Exception
     */
    @Test
    public void testIsUpgradeInProgress() throws Exception {
        DefaultAdapterForUpgrade defaultAdapterForUpgrade = new DefaultAdapterForUpgrade();
        defaultAdapterForUpgrade.openVersionTable(corfuRuntime);

        // Update the node version.  Cluster version is still not updated
        defaultAdapterForUpgrade.startRollingUpgrade();

        // Instantiate LRRollingUpgradeHandler in this state, i.e., upgrade is in progress
        LRRollingUpgradeHandler rollingUpgradeHandler =
                new LRRollingUpgradeHandler((ILogReplicationVersionAdapter)defaultAdapterForUpgrade, corfuStore);

        // Verify that an ongoing upgrade is detected because node version != cluster version
        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Assert.assertTrue(rollingUpgradeHandler.isLRUpgradeInProgress(corfuStore, txnContext));
            txnContext.commit();
        }

        // End rolling upgrade.  This will update the cluster version to the latest version
        defaultAdapterForUpgrade.endRollingUpgrade();

        // Verify that an ongoing upgrade is not detected because node version == cluster version
        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Assert.assertFalse(rollingUpgradeHandler.isLRUpgradeInProgress(corfuStore, txnContext));
            txnContext.commit();
        }
    }

    @Test
    public void testSessionCreationFromMetadataTables()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        DefaultAdapterForUpgrade defaultAdapterForUpgrade = new DefaultAdapterForUpgrade();

        List<String> sourceAndSinkClusterIds = Stream.concat(
                DefaultClusterConfig.getSourceClusterIds().stream(),
                DefaultClusterConfig.getSinkClusterIds().stream()
        ).collect(Collectors.toList());

        // Setup dummy V1 metadata for source clusters
        for (String clusterId : sourceAndSinkClusterIds) {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    String.join("", V1_METADATA_TABLE_PREFIX, clusterId),
                    LogReplicationMetadata.LogReplicationMetadataKey.class,
                    LogReplicationMetadata.LogReplicationMetadataVal.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.LogReplicationMetadataVal.class));
        }

        Table<ReplicationStatusKey, ReplicationStatusVal, Message> statusTable = corfuStore.openTable(
                CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationMetadata.ReplicationStatusKey.class,
                ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatusVal.class));

        // Setup dummy V1 metadata for sink clusters
        for (String sinkClusterId : DefaultClusterConfig.getSinkClusterIds()) {
            try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                ReplicationStatusKey key = ReplicationStatusKey.newBuilder()
                        .setClusterId(sinkClusterId)
                        .build();
                ReplicationStatusVal val = ReplicationStatusVal.newBuilder()
                        .build();

                txnContext.putRecord(statusTable, key, val, null);
                txnContext.commit();
            }
        }

        LRRollingUpgradeHandler rollingUpgradeHandler =
                new LRRollingUpgradeHandler((ILogReplicationVersionAdapter)defaultAdapterForUpgrade, corfuStore);
        List<LogReplicationSession> defaultSessions = DefaultClusterConfig.getSessions();
        List<LogReplicationSession> constructedSessions;

        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            constructedSessions = rollingUpgradeHandler.buildSessionsFromOldMetadata(corfuStore, txnContext);
            txnContext.commit();
        }

        // Check that the sessions constructed and the default sessions are equal
        Assert.assertEquals(new HashSet<>(defaultSessions), new HashSet<>(constructedSessions));
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }
}
