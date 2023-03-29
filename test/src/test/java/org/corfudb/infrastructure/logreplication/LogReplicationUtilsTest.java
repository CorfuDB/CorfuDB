package org.corfudb.infrastructure.logreplication;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplicationListener;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;

/**
 * Tests the apis in LogReplicationUtils.
 */
@Slf4j
public class LogReplicationUtilsTest extends AbstractViewTest {

    private CorfuRuntime corfuRuntime;
    private CorfuStore corfuStore;
    private LogReplicationListener lrListener;
    private Table<LogReplication.LogReplicationSession, LogReplication.ReplicationStatus, Message> replicationStatusTable;
    private String namespace = "test_namespace";

    @Before
    public void setUp() throws Exception {
        corfuRuntime = getDefaultRuntime();
        corfuStore = new CorfuStore(corfuRuntime);
        lrListener = new LogReplicationTestListener(corfuStore, namespace);
        replicationStatusTable = TestUtils.openReplicationStatusTable(corfuStore);
    }

    /**
     * Test the behavior of attemptClientFullSync() when LR Snapshot sync is ongoing.  The flags and variables on the
     * listener must be updated correctly.
     */
    @Test
    public void testAttemptClientFullSyncSnapshotSyncOngoing() {
        testAttemptClientFullSync(true);
    }

    /**
     * Test the behavior of attemptClientFullSync() when LR Snapshot sync is complete.  The flags and variables on
     * the listener must be updated correctly.
     */
    @Test
    public void testAttempClientFullSyncSnapshotSyncComplete() {
        testAttemptClientFullSync(false);
    }

    private void testAttemptClientFullSync(boolean ongoing) {
        TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, ongoing);
        LogReplicationUtils.attemptClientFullSync(corfuStore, lrListener, namespace);
        verifyListenerFlags(ongoing);
    }

    /**
     * Test the behavior of subscribe() when LR Snapshot sync is ongoing.  The flags and variables on the listener
     * must be updated correctly.
     */
    @Test
    public void testSubscribeSnapshotSyncOngoing() {
        testSubscribe(true);
    }

    /**
     * Test the behavior of subscribe() when LR Snapshot sync is complete.  The flags and variables on the listener
     * must be updated correctly.
     */
    @Test
    public void testSubscribeSnapshotSyncComplete() {
        testSubscribe(false);
    }

    private void testSubscribe(boolean ongoing) {
        TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, ongoing);

        String streamTag = "test_tag";
        LogReplicationUtils.subscribe(lrListener, namespace, streamTag, new ArrayList<>(), 5, corfuStore);
        verifyListenerFlags(ongoing);
    }

    private void verifyListenerFlags(boolean ongoing) {
        if (ongoing) {
            Assert.assertTrue(lrListener.getClientFullSyncPending().get());
            Assert.assertTrue(lrListener.getSnapshotSyncInProgress().get());
            Assert.assertEquals(Address.NON_ADDRESS, lrListener.getClientFullSyncTimestamp().get());
            Assert.assertFalse(((LogReplicationTestListener)lrListener).performFullSyncInvoked);
        } else {
            Assert.assertFalse(lrListener.getClientFullSyncPending().get());
            Assert.assertFalse(lrListener.getSnapshotSyncInProgress().get());
            Assert.assertNotEquals(Address.NON_ADDRESS, lrListener.getClientFullSyncTimestamp().get());
            Assert.assertTrue(((LogReplicationTestListener)lrListener).performFullSyncInvoked);
        }
    }

    @After
    public void cleanUp() {
        corfuRuntime.shutdown();
    }

    private class LogReplicationTestListener extends LogReplicationListener {

        private boolean performFullSyncInvoked = false;

        LogReplicationTestListener(CorfuStore corfuStore, String namespace) {
            super(corfuStore, namespace);
        }

        protected void onSnapshotSyncStart() {}

        protected void onSnapshotSyncComplete() {}

        protected void processUpdatesInSnapshotSync(CorfuStreamEntries results) {}

        protected void processUpdatesInLogEntrySync(CorfuStreamEntries results) {}

        protected void performFullSync(TxnContext txnContext) {
            performFullSyncInvoked = true;
        }

        public void onError(Throwable throwable) {
            log.error("Error in Test Listener", throwable);
        }
    }
}
