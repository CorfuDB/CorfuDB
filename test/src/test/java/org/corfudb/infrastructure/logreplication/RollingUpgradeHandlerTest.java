package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.LRRollingUpgradeHandler;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultAdapterForUpgrade;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        DefaultAdapterForUpgrade defaultAdapterForUpgrade = new DefaultAdapterForUpgrade(corfuRuntime);

        // Update the node version.  Cluster version is still not updated
        defaultAdapterForUpgrade.startRollingUpgrade();

        // Instantiate LRRollingUpgradeHandler in this state, i.e., upgrade is in progress
        LRRollingUpgradeHandler rollingUpgradeHandler =
                new LRRollingUpgradeHandler((ILogReplicationVersionAdapter)defaultAdapterForUpgrade, corfuStore);

        // Verify that an ongoing upgrade is detected because node version != cluster version
        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Assert.assertTrue(rollingUpgradeHandler.isLRUpgradeInProgress(txnContext));
            txnContext.commit();
        }

        // End rolling upgrade.  This will update the cluster version to the latest version
        defaultAdapterForUpgrade.endRollingUpgrade();

        // Verify that an ongoing upgrade is not detected because node version == cluster version
        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Assert.assertFalse(rollingUpgradeHandler.isLRUpgradeInProgress(txnContext));
            txnContext.commit();
        }
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }
}
