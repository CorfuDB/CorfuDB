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
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.LogReplicationRoutingQueueListener;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Tests the apis in LogReplicationUtils.
 */
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class LogReplicationUtilsTest extends AbstractViewTest {

    private CorfuRuntime corfuRuntime;
    private CorfuStore corfuStore;
    private LogReplicationListener lrListener;
    private LogReplicationRoutingQueueListener lrRqListener;
    private Table<LogReplication.LogReplicationSession, LogReplication.ReplicationStatus, Message> replicationStatusTable;
    private String namespace = "test_namespace";
    private String client = "test_client";

    @Before
    public void setUp() throws Exception {
        corfuRuntime = getDefaultRuntime();
        corfuStore = new CorfuStore(corfuRuntime);
        lrListener = new LogReplicationTestListener(corfuStore, namespace, client);
        lrRqListener = new LogReplicationTestRoutingQueueListener(corfuStore, namespace, client);
        replicationStatusTable = TestUtils.openReplicationStatusTable(corfuStore);
    }

    /**
     * Test the behavior of attemptClientFullSync() when LR Snapshot sync is ongoing.  The flags and variables on the
     * listener must be updated correctly.
     */
    @Test
    public void testAttemptClientFullSyncSnapshotSyncOngoing() {
        testAttemptClientFullSync(true,true);
    }

    /**
     * Test the behavior of attemptClientFullSync() when LR Snapshot sync is complete.  The flags and variables on
     * the listener must be updated correctly.
     */
    @Test
    public void testAttemptClientFullSyncSnapshotSyncComplete() {
        testAttemptClientFullSync(true,false);
    }

    /**
     * Test the behavior of attemptClientFullSync() when the LR Status table does not have any entry for the Logical
     * Group Replication Model.  The expected behavior is that no ongoing Snapshot Sync is detected and client full
     * sync succeeds.
     */
    @Test
    public void testAttemptClientFullSyncStatusNotFound() {
        testAttemptClientFullSync(false, false);
    }

    private void testAttemptClientFullSync(boolean initializeTable, boolean ongoing) {
        if (initializeTable) {
            TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, client, ongoing);
        }
        LogReplicationUtils.attemptClientFullSync(corfuStore, lrListener, namespace);
        verifyListenerFlags((LogReplicationTestListener)lrListener, ongoing);
    }

    /**
     * Test the behavior of subscribe() when LR Snapshot sync is ongoing.  The flags and variables on the listener
     * must be updated correctly.
     */
    @Test
    public void testSubscribeSnapshotSyncOngoing() {
        testSubscribe(true, true);
    }

    @Test
    public void testRoutingQueueSubscribeSnapshotSyncComplete() {
        testRoutingQueueSubscribe(true, false);
    }

    /**
     * Test the behavior of subscribe() when LR Snapshot sync is complete.  The flags and variables on the listener
     * must be updated correctly.
     */
    @Test
    public void testSubscribeSnapshotSyncComplete() {
        testSubscribe(true, false);
    }

    /**
     * Test the behavior of subscribe() when the LR Status table does not have any entry for the Logical
     * Group Replication Model.  The expected behavior is that no ongoing Snapshot Sync is detected and
     * subscription succeeds.
     */
    @Test
    public void testSubscribeSyncStatusNotFound() {
        testSubscribe(false, false);
    }

    private void testSubscribe(boolean initializeTable, boolean ongoing) {
        if (initializeTable) {
            TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, client, ongoing);
        }

        String streamTag = "test_tag";
        LogReplicationUtils.subscribe(lrListener, namespace, streamTag, new ArrayList<>(), 5, corfuStore);
        verifyListenerFlags((LogReplicationTestListener)lrListener, ongoing);
    }

    private void executeTxn(CorfuStore store, String namespace, Consumer<TxnContext> mutation) {
        try (TxnContext tx = store.txn(namespace)) {
            mutation.accept(tx);
            tx.commit();
        }
    }

    private void testRoutingQueueSubscribe(boolean initializeTable, boolean ongoing) {
        if (initializeTable) {
            TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, client, ongoing);
        }

        // Open routing queue before the subscribe call at the receiver.
        String recvQueueName = LogReplicationUtils.REPLICATED_QUEUE_NAME_PREFIX;
        Table<Queue.CorfuGuidMsg, Queue.RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> routingQueue;
        try {
            routingQueue =
                    corfuStore.openQueue(namespace, recvQueueName,
                            Queue.RoutingTableEntryMsg.class,
                            TableOptions.builder().schemaOptions(
                                            CorfuOptions.SchemaOptions.newBuilder()
                                                    .addStreamTag(LogReplicationUtils.REPLICATED_QUEUE_TAG_PREFIX)
                                                    .build())
                                    .build());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        // Subscribe to the routing queue.
        LogReplicationUtils.subscribeRqListener(lrRqListener, namespace, 5, corfuStore);

        // Update the queue to verify the listener works
        List<UUID> tags = new ArrayList<>();
        tags.add(CorfuRuntime.getStreamID(LogReplicationUtils.REPLICATED_QUEUE_TAG_PREFIX));
        // TODO: tx.enqueue and logUpdateEnqueue complains on the remote UT for unable to get guid. Removing it for now.
        // executeTxn(corfuStore, namespace, (TxnContext tx) -> tx.logUpdateEnqueue(routingQueue,
        //        Queue.RoutingTableEntryMsg.newBuilder().addDestinations(tx.getNamespace()).build(),
        //        tags, corfuStore));
        verifyRoutingQueueListenerFlags((LogReplicationTestRoutingQueueListener)lrRqListener, ongoing);
    }

    @Test
    public void testAttemptClientFullSyncMultipleClients() {
        testMultipleClients(false);
    }

    @Test
    public void testSubscribeMultipleClients() {
        testMultipleClients(true);
    }

    /**
     * Verify that the client name is matched correctly.
     * 1) Set snapshot sync ongoing to false on the session corresponding to test_client
     * 2) Set snapshot sync ongoing to true on the session corresponding to new_client
     * 3) Invoke subscribe() or attemptClientFullSync() on test_client's listener
     * 4) Verify that full sync finished and snapshot sync was not considered ongoing for test_client
     * 5) Verify that full sync was not attempted and snapshot sync was ongoing for new_client
     */
    private void testMultipleClients(boolean subscribe) {

        // Snapshot sync is not in progress on test_client
        TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, client, false);

        // Create the listener for new_client and set snapshot sync to be in progress
        String newClient = "new_client";
        LogReplicationListener newListener = new LogReplicationTestListener(corfuStore, namespace, newClient);
        TestUtils.setSnapshotSyncOngoing(corfuStore, replicationStatusTable, newClient, true);

        if (subscribe) {
            String streamTag = "test_tag";
            LogReplicationUtils.subscribe(lrListener, namespace, streamTag, new ArrayList<>(), 5, corfuStore);
            LogReplicationUtils.subscribe(newListener, namespace, streamTag, new ArrayList<>(), 5, corfuStore);
        } else {
            LogReplicationUtils.attemptClientFullSync(corfuStore, lrListener, namespace);
            LogReplicationUtils.attemptClientFullSync(corfuStore, newListener, namespace);
        }

        // Verify that full sync finished and snapshot sync was not considered ongoing for test_client
        verifyListenerFlags((LogReplicationTestListener)lrListener, false);

        // Verify that full sync was not attempted and snapshot sync was ongoing for new_client
        verifyListenerFlags((LogReplicationTestListener)newListener, true);
    }

    private void verifyListenerFlags(LogReplicationTestListener listener, boolean snapshotSyncOngoing) {
        if (snapshotSyncOngoing) {
            Assert.assertTrue(listener.getClientFullSyncPending().get());
            Assert.assertTrue(listener.getSnapshotSyncInProgress().get());
            Assert.assertEquals(Address.NON_ADDRESS, listener.getClientFullSyncTimestamp().get());
            Assert.assertFalse(listener.performFullSyncInvoked);
        } else {
            Assert.assertFalse(listener.getClientFullSyncPending().get());
            Assert.assertFalse(listener.getSnapshotSyncInProgress().get());
            Assert.assertNotEquals(Address.NON_ADDRESS, listener.getClientFullSyncTimestamp().get());
            Assert.assertTrue(listener.performFullSyncInvoked);
        }
    }

    private void verifyRoutingQueueListenerFlags(LogReplicationTestRoutingQueueListener listener, boolean snapshotSyncOngoing) {
        if (snapshotSyncOngoing) {
            Assert.assertTrue(listener.getClientFullSyncPending().get());
            Assert.assertTrue(listener.getSnapshotSyncInProgress().get());
            Assert.assertEquals(Address.NON_ADDRESS, listener.getClientFullSyncTimestamp().get());
            Assert.assertFalse(listener.performFullSyncInvoked);
        } else {
            Assert.assertFalse(listener.getClientFullSyncPending().get());
            Assert.assertFalse(listener.getSnapshotSyncInProgress().get());
            Assert.assertFalse(listener.getRoutingQRegistered().get());
            Assert.assertNotEquals(Address.NON_ADDRESS, listener.getClientFullSyncTimestamp().get());
            Assert.assertTrue(listener.performFullSyncInvoked);
        }
    }
    @After
    public void cleanUp() {
        corfuRuntime.shutdown();
    }

    private class LogReplicationTestListener extends LogReplicationListener {

        private boolean performFullSyncInvoked = false;

        private String clientName;

        LogReplicationTestListener(CorfuStore corfuStore, String namespace, String clientName) {
            super(corfuStore, namespace);
            this.clientName = clientName;
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
            performFullSyncInvoked = true;
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

    private class LogReplicationTestRoutingQueueListener extends LogReplicationRoutingQueueListener {

        private boolean performFullSyncInvoked = false;

        private String clientName;

        LogReplicationTestRoutingQueueListener(CorfuStore corfuStore, String namespace, String clientName) throws
                InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            super(corfuStore, namespace);
            this.clientName = clientName;
        }

        @Override
        protected void onSnapshotSyncStart() {}

        @Override
        protected void onSnapshotSyncComplete() {}

        @Override
        protected boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> results) {
            return true;
        }

        @Override
        protected boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> results) {
            return true;
        }

        @Override
        protected void performFullSyncAndMerge(TxnContext txnContext) {
            performFullSyncInvoked = true;
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
