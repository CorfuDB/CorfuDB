package org.corfudb.infrastructure.logreplication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
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

import java.util.Map;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class MetadataManagerTest extends AbstractViewTest {

    private CorfuRuntime corfuRuntime;
    private LogReplicationConfigManager configManager = Mockito.mock(LogReplicationConfig.class);

    private boolean success;
    private Long topologyConfigId = 5L;
    private String localClusterId = "Test Cluster";
    private TestUtils utils;

    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();
        utils = new TestUtils();
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

        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(corfuRuntime, topologyConfigId,
            localClusterId);
        LogEntryWriter writer = new LogEntryWriter(replicationConfig, metadataManager);

        Long numOpaqueEntries = 3L;
        LogReplication.LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries,
            Address.NON_ADDRESS, topologyConfigId, Address.NON_ADDRESS);

        // Verify that the data was successfully applied
        boolean success = writer.apply(lrEntryMsg);
        Assert.assertTrue(success);

        TxnContext txnContext = metadataManager.getTxnContext();
        Map<LogReplicationMetadataManager.LogReplicationMetadataType, Long> metadataMap = metadataManager.queryMetadata(
            txnContext,
            LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
            LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED,
            LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED);
        txnContext.commit();

        // Verify that metadata was correctly updated
        Assert.assertEquals(topologyConfigId,
            metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID));
        Assert.assertEquals(numOpaqueEntries,
            metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED));
        Assert.assertEquals(numOpaqueEntries,
            metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED));
    }

    /**
     * This test verifies that the initial values for last applied snapshot timestamp and last processed log entry
     * timestamp are  Address.NON_ADDRESS.
     * LR code relies on these initial values so any change to this expectation should be caught by this test.
     */
    @Test
    public void testInitTsForSnapshotAndLogEntryProcessed() {

        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(corfuRuntime,
            topologyConfigId, localClusterId);

        long lastApppliedSnapshotTimestamp = metadataManager.getLastAppliedSnapshotTimestamp();
        long lastProcessedLogEntryTimestamp = metadataManager.getLastProcessedLogEntryBatchTimestamp();

        Assert.assertEquals(Address.NON_ADDRESS, lastApppliedSnapshotTimestamp);
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

        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(corfuRuntime, topologyConfigId,
            localClusterId);
        LogEntryWriter writer = new LogEntryWriter(replicationConfig, metadataManager);

        // Create a message with 50 opaque entries
        Long numOpaqueEntries = 50L;
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
                        metadataManager.appendUpdate(txnContext,
                            LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
                            topologyConfigId+1);
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
        Map<LogReplicationMetadataManager.LogReplicationMetadataType, Long> metadataMap = metadataManager.queryMetadata(
            txnContext,
            LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
            LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED,
            LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED);
        txnContext.commit();

        // Verify that the topology was successfully updated
        Assert.assertEquals(topologyConfigId+1,
            (long)metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.TOPOLOGY_CONFIG_ID));

        if (success) {
            // Log entry apply finished before the topology changed.  Both LAST_LOG_ENTRY_PROCESSED and
            // LAST_LOG_ENTRY_APPLIED will be updated
            Assert.assertEquals(numOpaqueEntries,
                metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED));
            Assert.assertEquals(numOpaqueEntries,
                metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED));
        } else {
            // Log entry was partially applied because topology change was detected.  LAST_LOG_ENTRY_PROCESSED should
            // not have changed.
            Assert.assertEquals(Address.NON_ADDRESS,
                (long)metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED));
            Assert.assertTrue(
                metadataMap.get(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED)
                    < numOpaqueEntries);
        }
    }
}
