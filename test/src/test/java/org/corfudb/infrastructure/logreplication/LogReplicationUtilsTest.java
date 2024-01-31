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
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Tests the apis in LogReplicationUtils.
 * TODO (Shreay): Needs refactoring to properly test behavior of LR listener subscription
 *
 */
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class LogReplicationUtilsTest extends AbstractViewTest {

    private CorfuRuntime corfuRuntime;
    private CorfuStore corfuStore;
    private LogReplicationTestListener lrListener;
    private Table<LogReplication.LogReplicationSession, LogReplication.ReplicationStatus, Message> replicationStatusTable;
    private String namespace = "test_namespace";
    private String client = "test_client";
    private String streamTag = "test_tag";
    private CountDownLatch performedFullSync;

    public void setUp(boolean openStatusTable) throws Exception {
        corfuRuntime = getDefaultRuntime();
        corfuStore = new CorfuStore(corfuRuntime);
        performedFullSync = new CountDownLatch(1);
        lrListener = new LogReplicationTestListener(corfuStore, namespace, client, performedFullSync);

        if (openStatusTable) {
            replicationStatusTable = TestUtils.openReplicationStatusTable(corfuStore);
        }
    }

    /**
     * Test the behavior of subscribe() when LR Snapshot sync is ongoing.  The flags and variables on the listener
     * must be updated correctly.
     */
    @Test
    public void testSubscribeSnapshotSyncOngoing() throws Exception {
        testSubscribe(true, true);
    }


    /**
     * Test the behavior of subscribe() when LR Snapshot sync is complete.  The flags and variables on the listener
     * must be updated correctly.
     */
    @Test
    public void testSubscribeSnapshotSyncComplete() throws Exception {
        testSubscribe(true, false);
    }

    /**
     * Test the behavior of subscribe() when the LR Status table does not have any entry for the Logical
     * Group Replication Model.  The expected behavior is that no ongoing Snapshot Sync is detected and
     * subscription succeeds.
     */
    @Test
    public void testSubscribeSyncStatusNotFound() throws Exception {
        testSubscribe(false, false);
    }

    private void testSubscribe(boolean initializeTable, boolean ongoing) throws Exception {
        setUp(true);

        if (initializeTable) {
            TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, client, ongoing);
        }

        LogReplicationUtils.subscribe(lrListener, namespace, streamTag, new ArrayList<>(), 5, corfuStore);
        verifyListenerFlags(lrListener, ongoing);
    }

    /**
     * Verify that the client name is matched correctly.
     * 1) Set snapshot sync ongoing to false on the session corresponding to test_client
     * 2) Set snapshot sync ongoing to true on the session corresponding to new_client
     * 3) Invoke subscribe() on test_client's listener
     * 4) Verify that full sync finished and snapshot sync was not considered ongoing for test_client
     * 5) Verify that full sync was not attempted and snapshot sync was ongoing for new_client
     */
    @Test
    public void testMultipleClients() throws Exception {
        setUp(true);

        // Snapshot sync is not in progress on test_client
        TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, client, false);

        // Create the listener for new_client and set snapshot sync to be in progress
        String newClient = "new_client";
        CountDownLatch newClientFullSyncPerformed = new CountDownLatch(1);
        LogReplicationTestListener newListener = new LogReplicationTestListener(corfuStore,
                namespace, newClient, newClientFullSyncPerformed);
        TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, newClient, true);

        LogReplicationUtils.subscribe(lrListener, namespace, streamTag, new ArrayList<>(), 5, corfuStore);
        LogReplicationUtils.subscribe(newListener, namespace, streamTag, new ArrayList<>(), 5, corfuStore);

        // Verify that full sync finished and snapshot sync was not considered ongoing for test_client
        verifyListenerFlags(lrListener, false);

        // Verify that full sync was not attempted and snapshot sync was ongoing for new_client
        verifyListenerFlags(newListener, true);
    }

    /**
     * If the listener has been subscribed before the status table has been opened or hasn't yet been
     * opened by the client's runtime, the listener should open the table.
     */
    @Test
    public void testSubscriptionWithUnopenedStatusTable() throws Exception {
        setUp(false);
        LogReplicationUtils.subscribe(lrListener, namespace, streamTag, new ArrayList<>(), 5, corfuStore);
    }

    private void verifyListenerFlags(LogReplicationTestListener listener, boolean snapshotSyncOngoing) throws InterruptedException {
        if (snapshotSyncOngoing) {
            // We need to insert delay here to let listener workflow run before validating the latch.
            Thread.sleep(1000);

            // Validate that performFullSyncAndMerge() was not ran as snapshot sync was ongoing.
            Assert.assertEquals(1, listener.performedFullSync.getCount());
        } else {
            // performFullSyncAndMerge() should have ran.
            listener.performedFullSync.await();
        }
    }

    @After
    public void cleanUp() {
        corfuRuntime.shutdown();
    }

    private static class LogReplicationTestListener extends LogReplicationListener {

        private final CountDownLatch performedFullSync;
        private final String clientName;

        LogReplicationTestListener(CorfuStore corfuStore, String namespace, String clientName, CountDownLatch performedFullSync) {
            super(corfuStore, namespace);
            this.clientName = clientName;
            this.performedFullSync = performedFullSync;
        }

        @Override
        protected void onSnapshotSyncStart() {}

        @Override
        protected void onSnapshotSyncComplete() {}

        @Override
        protected void processUpdatesInSnapshotSync(CorfuStreamEntries results) {}

        @Override
        protected void processUpdatesInLogEntrySync(CorfuStreamEntries results) {}

        @Override
        protected void performFullSyncAndMerge(TxnContext txnContext) {
            performedFullSync.countDown();
        }

        @Override
        protected String getClientName() {
            return clientName;
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error in Test Listener", throwable);
        }
    }
}
