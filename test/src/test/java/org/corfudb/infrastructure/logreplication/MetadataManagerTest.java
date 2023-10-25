package org.corfudb.infrastructure.logreplication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class MetadataManagerTest extends AbstractViewTest {

    private static final String LOCAL_SOURCE_CLUSTER_ID = DefaultClusterConfig.getSourceClusterIds().get(0);
    private CorfuRuntime corfuRuntime;
    private LogReplicationConfigManager configManager = Mockito.mock(LogReplicationConfigManager.class);

    private boolean success;
    private long topologyConfigId = 5L;
    private TestUtils utils;
    private List<LogReplicationSession> sessions = DefaultClusterConfig.getSessions();
    private LogReplicationSession defaultSession = sessions.get(0);
    private LogReplicationMetadataManager metadataManager;
    private LogReplicationContext replicationContext;

    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();
        Mockito.doReturn(corfuRuntime).when(configManager).getRuntime();
        utils = new TestUtils();
        replicationContext = new LogReplicationContext(new LogReplicationConfigManager(corfuRuntime,
                LOCAL_SOURCE_CLUSTER_ID), topologyConfigId, getEndpoint(SERVERS.PORT_0), true,
                Mockito.mock(LogReplicationPluginConfig.class), corfuRuntime, 2);
        metadataManager = new LogReplicationMetadataManager(corfuRuntime, replicationContext);
        metadataManager.addSession(defaultSession, topologyConfigId, true);
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }

    /**
     * This test verifies that the timestamp of the last log entry processed during LogEntry sync is correctly
     * updated in the metadata table on the Sink cluster
     */
    @Test
    public void testMetadataAfterLogEntrySync() {
        LogReplicationContext context = new LogReplicationContext(configManager, topologyConfigId,
                getEndpoint(SERVERS.PORT_0), Mockito.mock(LogReplicationPluginConfig.class), corfuRuntime, 2);
        LogEntryWriter writer = new LogEntryWriter(metadataManager, defaultSession, context);

        long numOpaqueEntries = 3L;
        LogReplication.LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries,
                Address.NON_ADDRESS, topologyConfigId, Address.NON_ADDRESS);

        // Verify that the data was successfully applied
        boolean success = writer.apply(lrEntryMsg);
        Assert.assertTrue(success);

        ReplicationMetadata metadata;
        try(TxnContext txnContext = metadataManager.getTxnContext()) {
            metadata = metadataManager.queryReplicationMetadata(txnContext, defaultSession);
            txnContext.commit();
        }

        // Verify that metadata was correctly updated
        Assert.assertEquals(topologyConfigId, metadata.getTopologyConfigId());
        Assert.assertEquals(numOpaqueEntries, metadata.getLastLogEntryBatchProcessed());
        Assert.assertEquals(numOpaqueEntries, metadata.getLastLogEntryApplied());
    }

    /**
     * This test verifies that the initial values for last applied snapshot timestamp and last processed log entry
     * timestamp are  Address.NON_ADDRESS.
     * LR code relies on these initial values so any change to this expectation should be caught by this test.
     */
    @Test
    public void testInitTsForSnapshotAndLogEntryProcessed() {

        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(corfuRuntime, replicationContext);
        metadataManager.addSession(defaultSession, topologyConfigId, true);

        long lastAppliedSnapshotTimestamp = metadataManager.getReplicationMetadata(defaultSession)
            .getLastSnapshotApplied();
        long lastProcessedLogEntryTimestamp = metadataManager.getReplicationMetadata(defaultSession)
            .getLastLogEntryBatchProcessed();

        Assert.assertEquals(Address.NON_ADDRESS, lastAppliedSnapshotTimestamp);
        Assert.assertEquals(Address.NON_ADDRESS, lastProcessedLogEntryTimestamp);
    }

    /**
     * This test simulates a log entry apply and topology config update concurrently.
     * It verifies that the topology update was successful.  Additionally, 1) if the topology update finished before all
     * opaque entries were applied, it verifies that LAST_LOG_ENTRY_BATCH_PROCESSED was not updated (because topology
     * config mismatch will be detected and no more entries applied) 2) if all opaque entries were applied before the
     * topology config update, LAST_LOG_ENTRY_BATCH_PROCESSED was updated.
     * @throws Exception
     */
    @Test
    public void testConcurrentTopologyChange() throws Exception {

        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(corfuRuntime, replicationContext);
        metadataManager.addSession(defaultSession, topologyConfigId, true);
        LogReplicationContext context = new LogReplicationContext(configManager, 0,
                defaultSession.getSourceClusterId(), Mockito.mock(LogReplicationPluginConfig.class), corfuRuntime, 2);
        LogEntryWriter writer = new LogEntryWriter(metadataManager, defaultSession, context);

        // Create a message with 50 opaque entries
        long numOpaqueEntries = 50L;
        LogReplication.LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries,
                Address.NON_ADDRESS, topologyConfigId, Address.NON_ADDRESS);

        // Thread 1: Log Entry apply
        scheduleConcurrently(f -> {
            success = writer.apply(lrEntryMsg);
        });

        // Thread 2: Topology config update
        scheduleConcurrently(f -> {
            try {
                IRetry.build(IntervalRetry.class, () -> {
                    CorfuStore corfuStore = new CorfuStore(corfuRuntime);
                    try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                        metadataManager.updateReplicationMetadataField(txnContext, defaultSession,
                                ReplicationMetadata.TOPOLOGYCONFIGID_FIELD_NUMBER, topologyConfigId + 1);
                        txnContext.commit();
                    } catch (TransactionAbortedException tae) {
                        log.error("Transaction Aborted", tae);
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } catch (Exception e) {
                log.error("Unexpected exception caught.  Giving up", e);
            }
        });

        executeScheduled(2, PARAMETERS.TIMEOUT_NORMAL);

        TxnContext txnContext = metadataManager.getTxnContext();
        ReplicationMetadata metadata = metadataManager
                .queryReplicationMetadata(txnContext, defaultSession);
        txnContext.commit();

        // Verify that the topology was successfully updated
        Assert.assertEquals(topologyConfigId + 1, metadata.getTopologyConfigId());

        if (success) {
            // Log entry apply finished before the topology changed.  Both LAST_LOG_ENTRY_PROCESSED and
            // LAST_LOG_ENTRY_APPLIED will be updated
            Assert.assertEquals(numOpaqueEntries, metadata.getLastLogEntryBatchProcessed());
            Assert.assertEquals(numOpaqueEntries, metadata.getLastLogEntryApplied());
        } else {
            // Log entry was partially applied because topology change was detected.  LAST_LOG_ENTRY_PROCESSED should
            // not have changed.
            Assert.assertEquals(Address.NON_ADDRESS, metadata.getLastLogEntryBatchProcessed());
            Assert.assertTrue(metadata.getLastLogEntryApplied() < numOpaqueEntries);
        }
    }

    /**
     * This test verifies that timestamp of the consistent cut for which
     * data is being replicated.
     *
     * If the current topologyConfigId is not the same as the persisted topologyConfigId, ignore the operation.
     * If the current ts is smaller than the persisted snapStart, it is an old operation,
     * ignore it.
     * Otherwise, update the base snapshot start timestamp. The update of topologyConfigId just fences off
     * any other metadata updates in other transactions.
     */
    @Test
    public void testSetBaseSnapshotStart() {
        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(corfuRuntime, replicationContext);
        metadataManager.addSession(defaultSession, topologyConfigId, true);

        ReplicationMetadata metadata = metadataManager.getReplicationMetadata(defaultSession);
        Assert.assertNotNull(metadata);

        boolean snapshotStartTsSetWithInvalidTopConfigId = metadataManager.setBaseSnapshotStart(defaultSession,
                topologyConfigId + 1, Address.NON_ADDRESS);
        boolean snapshotStartTsSetWithValidTopConfigId = metadataManager.setBaseSnapshotStart(defaultSession,
                topologyConfigId, 6L);

        Assert.assertFalse(snapshotStartTsSetWithInvalidTopConfigId);
        Assert.assertTrue(snapshotStartTsSetWithValidTopConfigId);
    }

    /**
     * This test verifies that the last snapshot transfer complete.
     */
    @Test
    public void testSetLastSnapshotTransferCompleteTimestamp() {
        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(corfuRuntime, replicationContext);
        metadataManager.addSession(defaultSession, topologyConfigId, true);

        ReplicationMetadata metadata = metadataManager.getReplicationMetadata(defaultSession);
        Assert.assertNotNull(metadata);
        long initialLastTransferredTs = metadata.getLastSnapshotTransferred();

        // Modify the cluster config, and verify that the update operation failed.
        metadataManager.setLastSnapshotTransferCompleteTimestamp(defaultSession,
                topologyConfigId + 1, Address.NON_ADDRESS);
        Assert.assertEquals(initialLastTransferredTs,
            metadataManager.getReplicationMetadata(defaultSession).getLastSnapshotTransferred());

        // Verify the update operation succeed.
        metadataManager.setLastSnapshotTransferCompleteTimestamp(defaultSession, topologyConfigId, 6L);
        Assert.assertEquals(6L,
            metadataManager.getReplicationMetadata(defaultSession).getLastSnapshotTransferred());
    }

    /**
     * This test verifies that the log entry snapshot applied is complete.
     */
    @Test
    public void testSetSnapshotAppliedComplete() {
        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(corfuRuntime, replicationContext);
        metadataManager.addSession(defaultSession, topologyConfigId, true);

        // Verify that an entry in the replication status table was created for the default session
        Assert.assertEquals(1L, metadataManager.getReplicationStatus().size());

        ReplicationMetadata metadata = metadataManager.getReplicationMetadata(defaultSession);
        Assert.assertNotNull(metadata);

        // Modify the topology config in log entry message, and verify that the update operation failed.
        long numOpaqueEntries = 3L;
        LogReplication.LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries,
                Address.NON_ADDRESS, topologyConfigId + 1, Address.NON_ADDRESS);
        metadataManager.setSnapshotAppliedComplete(lrEntryMsg, defaultSession);

        // Verify the update operation succeed.
        lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries,
                Address.NON_ADDRESS, topologyConfigId, Address.NON_ADDRESS);
        metadataManager.setSnapshotAppliedComplete(lrEntryMsg, defaultSession);
        Assert.assertTrue(metadataManager.getReplicationStatus().get(defaultSession).getSinkStatus().getDataConsistent());
    }

}
