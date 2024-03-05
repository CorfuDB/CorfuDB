package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.times;

@SuppressWarnings("checkstyle:magicnumber")
public class LogEntryWriterTest extends AbstractViewTest {

    private CorfuRuntime corfuRuntime;
    private LogReplicationMetadataManager metadataManager = Mockito.mock(LogReplicationMetadataManager.class);
    private TxnContext txnContext = Mockito.mock(TxnContext.class);
    private LogEntryWriter logEntryWriter;
    private TxnContext txnContext;
    private TestUtils utils;
    private int numOpaqueEntries;
    private int topologyConfigId;
    private String remoteClusterId = "Remote Cluster";

    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();

        // Initialize TableRegistry and register ProtobufSerializer
        corfuRuntime.getTableRegistry();

        // Create the default replication session
        ReplicationSession replicationSession =
            ReplicationSession.getDefaultReplicationSessionForCluster(remoteClusterId);

        metadataManager = Mockito.mock(LogReplicationMetadataManager.class);
        initMocksForMetadataManager();

        // Mocking steps for initializing LogEntryWriter.
        /*LogReplicationConfigManager mockConfigManager = Mockito.mock(LogReplicationConfigManager.class);
        Mockito.doReturn(corfuRuntime).when(mockConfigManager).getRuntime();*/

        LogReplicationConfigManager configManager = new LogReplicationConfigManager(corfuRuntime);
        LogReplicationContext replicationContext = new LogReplicationContext(configManager, topologyConfigId,
                getEndpoint(SERVERS.PORT_0));
        logEntryWriter = new LogEntryWriter(replicationContext, metadataManager, replicationSession);
        numOpaqueEntries = 3;
        topologyConfigId = 5;
        utils = new TestUtils();
    }

    /**
     * This test verifies that Log Entry(delta) updates are applied on the Sink with transaction boundary preserved.
     * Each OpaqueEntry maps to a transaction on the Source.  Hence, each should be applied on the Sink as a separate
     * transaction.
     */
    @Test
    public void testLogEntryApplyWithExpectedTx() {
        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries, Address.NON_ADDRESS,
            topologyConfigId, Address.NON_ADDRESS);

        Map<LogReplicationMetadataType, Long> metadataMap = constructMetadataMgrMap(topologyConfigId,
            Address.NON_ADDRESS, Address.NON_ADDRESS, Address.NON_ADDRESS);
        updateMetadataManagerMap(metadataMap, false);

        // Verify that the message containing the list of opaque entries was applied successfully
        Assert.assertTrue(logEntryWriter.apply(lrEntryMsg));

        verifyNumberOfTx(numOpaqueEntries);

        // Construct the expected arguments and order with which the metadata updates will be written.  There will be 4
        // updates - 3 for LAST_LOG_ENTRY_APPLIED and 1 for LAST_LOG_ENTRY_BATCH_PROCESSED
        List<TxnContext> txnContexts = new ArrayList<>();
        List<LogReplicationMetadataType> metadataTypes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();

        for(int i = 0; i < numOpaqueEntries + 1; i++) {
            txnContexts.add(txnContext);
        }

        for(int i = 0; i < numOpaqueEntries + 1; i++) {
            // The first 3 metadata updates will be for LAST_LOG_ENTRY_APPLIED
            if (i < numOpaqueEntries) {
                metadataTypes.add(LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED);
                timestamps.add((long)i+1);
            } else {
                // The last one will be for LAST_LOG_ENTRY_BATCH_PROCESSED
                metadataTypes.add(LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED);
                timestamps.add((long)i);
            }
        }
        verifyMetadataAppliedAndOrder(numOpaqueEntries+1, txnContexts, metadataTypes, timestamps);
    }

    /**
     * This test simulates the case that the same payload is received by LogEntryWriter twice and verifies that it is
     * applied only once.
     */
    @Test
    public void testLogEntryRedundantApply() {
        testLogEntryApplyWithExpectedTx();

        // Send the same payload again
        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries, Address.NON_ADDRESS,
            topologyConfigId, Address.NON_ADDRESS);

        Map<LogReplicationMetadataType, Long> metadataMap = constructMetadataMgrMap(topologyConfigId, numOpaqueEntries,
            numOpaqueEntries, Address.NON_ADDRESS);
        updateMetadataManagerMap(metadataMap, true);

        // Verify that the redundant update was not applied
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));
        verifyNumberOfTx(0);

        List<TxnContext> txnContexts = new ArrayList<>();
        List<LogReplicationMetadataType> metadataTypes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();

        verifyMetadataAppliedAndOrder(0, txnContexts, metadataTypes, timestamps);
    }

    /**
     * This test gives 2 LogEntry messages to the Writer, first with sequence numbers(timestamps) [1-3] and then with
     * sequence numbers [2-4].  It then verifies that sequence numbers 2 and 3 were skipped during the second
     * apply phase.
     */
    @Test
    public void testLogEntryApplyWithSequenceNumOverlap() {
        testLogEntryApplyWithExpectedTx();

        // Construct the next batch with sequence numbers [2-4]
        int startTs = 2;
        int endTs = 4;
        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(startTs, endTs, numOpaqueEntries, topologyConfigId,
            Address.NON_ADDRESS);

        Map<LogReplicationMetadataType, Long> metadataMap = constructMetadataMgrMap(topologyConfigId,
            numOpaqueEntries, numOpaqueEntries, Address.NON_ADDRESS);
        updateMetadataManagerMap(metadataMap, true);

        Assert.assertTrue(logEntryWriter.apply(lrEntryMsg));

        // As sequence numbers [1-3] were already applied, only the transaction corresponding to sequence number 4
        // will be applied.  So the total number of times commit is invoked should be 1
        verifyNumberOfTx(1);

        // Verify that the right metadata was applied - there will be 2 updates, 1 to LAST_LOG_ENTRY_APPLIED and
        // the other to LAST_LOG_ENTRY_BATCH_PROCESSED
        List<TxnContext> txnContexts = Arrays.asList(txnContext, txnContext);
        List<LogReplicationMetadataType> metadataTypes =
            Arrays.asList(LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED,
                LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED);
        List<Long> timestamps = Arrays.asList((long)endTs, (long)endTs);
        verifyMetadataAppliedAndOrder(2, txnContexts, metadataTypes, timestamps);
    }

    /**
     * This test injects a failure after a subset of opaque entries are applied and verifies the expected number of
     * transactions.  The test first gives a LogReplicationEntryMsg with sequence numbers(timestamps) [1-3] to
     * LogEntryWriter and injects a failure when sequence number 3 is being applied.  Then another message with
     * sequence numbers [1-4] is given as an input.  The test then verifies that the right number of transactions were
     * committed in each case.
     */
    @Test
    public void testLogEntrySyncWithPartialApply() {

        // Construct the message with sequence numbers [1-3]
        int startTs = 1;
        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(startTs, numOpaqueEntries, Address.NON_ADDRESS,
            topologyConfigId, Address.NON_ADDRESS);

        // Mock the MetadataManager to return metadata matching the one constructed in the message
        Map<LogReplicationMetadataType, Long> metadataMap = constructMetadataMgrMap(topologyConfigId,
            Address.NON_ADDRESS, Address.NON_ADDRESS, Address.NON_ADDRESS);
        updateMetadataManagerMap(metadataMap, false);

        // Throw an exception when sequence number = 3 is being applied.  This will simulate a partial apply.
        Mockito.doAnswer(invocation -> {
            throw new InterruptedException();
        }).when(metadataManager).appendUpdate(txnContext,
                LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED, numOpaqueEntries);

        // Verify that the message containing the list of opaque entries was not applied
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // Verify that 2 transactions were made, 1 each for opaque entries 1 and 2
        // The number of times commit() is invoked should be equal to 2
        verifyNumberOfTx(numOpaqueEntries-1);

        // Construct the next batch with sequence numbers [1-4]
        int endTs = numOpaqueEntries + 1;
        lrEntryMsg = utils.generateLogEntryMsg(startTs, endTs, Address.NON_ADDRESS, topologyConfigId, Address.NON_ADDRESS);

        // Construct metadata to reflect the previous partial apply
        metadataMap = constructMetadataMgrMap(topologyConfigId, Address.NON_ADDRESS, numOpaqueEntries-1,
            Address.NON_ADDRESS);
        updateMetadataManagerMap(metadataMap, true);

        // Verify that the message was applied successfully.
        Assert.assertTrue(logEntryWriter.apply(lrEntryMsg));

        // Only 2 entries from the new message will be applied - 3 and 4.
        verifyNumberOfTx(2);

        // 3 Metadata update are expected - 2 to LAST_LOG_ENTRY_APPLIED and 1 to LAST_LOG_ENTRY_BATCH_PROCESSED
        List<TxnContext> txnContexts = Arrays.asList(txnContext, txnContext, txnContext);
        List<LogReplicationMetadataType> metadataTypes =
            Arrays.asList(LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED,
                LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED,
                LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED);
        List<Long> timestamps = Arrays.asList((long)numOpaqueEntries, (long)endTs, (long)endTs);

        verifyMetadataAppliedAndOrder(3, txnContexts, metadataTypes, timestamps);
    }

    /**
     * This test verifies that the log entry is not applied when the timestamps of the last LogEntryMsg applied
     * differ on the Source and Sink.  It also tests the assumption/requirement that MetadataManager returns
     * Address.NON_ADDRESS as the init/default value of LAST_LOG_ENTRY_BATCH_PROCESSED.
     *
     * The test sets this timestamp to Address.NON_ADDRESS in the Metadata sent by the Source.  Metadata manager on
     * the Sink has set this value to 0.
     */
    @Test
    public void testApplyWithPrevTsMismatch() {

        int startTs = 1;
        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(startTs, numOpaqueEntries, Address.NON_ADDRESS,
            topologyConfigId, Address.NON_ADDRESS);

        // Construct metadata where the sequence number of the last log entry applied does not match the incoming
        // message
        int lastTsInMetadataTable = 0;
        Map<LogReplicationMetadataType, Long> metadataMap = constructMetadataMgrMap(topologyConfigId, lastTsInMetadataTable,
            lastTsInMetadataTable, Address.NON_ADDRESS);
        updateMetadataManagerMap(metadataMap, false);

        // Verify that the data was not applied due to mismatch in the last log entry batch processed timestamps
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // The number of times commit() is invoked should be 0 as validation failed
        verifyNumberOfTx(0);

        List<TxnContext> txnContexts = new ArrayList<>();
        List<LogReplicationMetadataType> metadataTypes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();

        verifyMetadataAppliedAndOrder(0, txnContexts, metadataTypes, timestamps);
    }

    /**
     * This test verifies that the log entry is not applied when the snapshot timestamp on the Source is lower than
     * the one on Sink.  It also tests the assumption/requirement that MetadataManager returns
     * Address.NON_ADDRESS as the init/default value of LAST_SNAPSHOT_APPLIED.
     *
     * The test sets this timestamp to Address.NON_ADDRESS in the Metadata sent by the Source.  Metadata manager on
     *  the Sink has set this value to 0.
     */
    @Test
    public void testApplyWithLowerSnapshotTsOnSource() {
        int startTs = 1;
        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(startTs, numOpaqueEntries, Address.NON_ADDRESS,
            topologyConfigId, Address.NON_ADDRESS);

        // Construct metadata where the snapshot timestamp from Source(Address.NON_ADDRESS) is lower than the one on
        // Sink(0)
        int snapshotTsInMetadataTable = 0;
        Map<LogReplicationMetadataType, Long> metadataMap = constructMetadataMgrMap(topologyConfigId,
            Address.NON_ADDRESS, Address.NON_ADDRESS, snapshotTsInMetadataTable);
        updateMetadataManagerMap(metadataMap, false);

        // Verify that the data was not applied because the snapshot timestamp from Source was lower
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // The number of times commit() is invoked should be 0 as validation failed
        verifyNumberOfTx(0);

        List<TxnContext> txnContexts = new ArrayList<>();
        List<LogReplicationMetadataType> metadataTypes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();

        verifyMetadataAppliedAndOrder(0, txnContexts, metadataTypes, timestamps);
    }

    /**
     * This test verifies that the log entry is not applied when the snapshot timestamp on the Source is higher than the
     * one on Sink.
     * The test sends a message with snapshot ts = 50 and opaque entries with ts [51, 52, 53] and verifies that it
     * was not applied.  snapshot ts on the Sink = Address.NON_ADDRESS
     */
    @Test
    public void testApplyWithHigherSnapshotTsOnSource() {

        long snapshotTsOnSource = 50;
        long startTs = 51;

        // Create a message with numOpaqueEntries starting from ts=51
        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(startTs, startTs + numOpaqueEntries, snapshotTsOnSource,
            topologyConfigId, snapshotTsOnSource);

        // Construct metadata on the Sink with default values(Address.NON_Address)
        Map<LogReplicationMetadataType, Long> metadataMap = constructMetadataMgrMap(topologyConfigId,
            Address.NON_ADDRESS, Address.NON_ADDRESS, Address.NON_ADDRESS);
        updateMetadataManagerMap(metadataMap, false);

        // Verify that the log entry message was applied as the incoming snapshot ts is higher
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // The number of times commit() was invoked should be 0 as validation failed
        verifyNumberOfTx(0);

        List<TxnContext> txnContexts = new ArrayList<>();
        List<LogReplicationMetadataType> metadataTypes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();

        verifyMetadataAppliedAndOrder(0, txnContexts, metadataTypes, timestamps);
    }

    /**
     * This test verifies that mismatch in topology config id on the Source and Sink is handled correctly.  The
     * incoming message with a different topology config id must not be applied on the Sink.
     */
    @Test
    public void testApplyWithTopologyMismatch() {

        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries, Address.NON_ADDRESS,
            topologyConfigId, Address.NON_ADDRESS);

        // Construct Sink metadata where the topology config id does not match the value received
        Map<LogReplicationMetadataType, Long> metadataMap = constructMetadataMgrMap(topologyConfigId+1,
            Address.NON_ADDRESS, Address.NON_ADDRESS, Address.NON_ADDRESS);
        updateMetadataManagerMap(metadataMap, false);

        // Verify that the data was not applied due to mismatch in the topology config ids
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // The number of times commit() is invoked should be 0 as validation failed
        verifyNumberOfTx(0);

        List<TxnContext> txnContexts = new ArrayList<>();
        List<LogReplicationMetadataType> metadataTypes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();

        verifyMetadataAppliedAndOrder(0, txnContexts, metadataTypes, timestamps);
    }

    private void verifyNumberOfTx(int numExpectedTx) {
        // Verify that each opaque entry was applied in a separate transaction
        // The number of times commit() is invoked should be equal to numOpaqueEntries
        Mockito.verify(txnContext, times(numExpectedTx)).commit();
    }

    private void verifyMetadataAppliedAndOrder(int numExpectedInvocations, List<TxnContext> expectedTxnContexts,
                                               List<LogReplicationMetadataType> expectedMetadataTypes,
                                               List<Long> expectedTimestamps) {

        ArgumentCaptor<TxnContext> txnContextCaptor = ArgumentCaptor.forClass(TxnContext.class);
        ArgumentCaptor<LogReplicationMetadataType> metadataTypeCaptor = ArgumentCaptor.forClass(LogReplicationMetadataType.class);
        ArgumentCaptor<Long> timestampCaptor = ArgumentCaptor.forClass(Long.class);

        Mockito.verify(metadataManager, times(numExpectedInvocations)).appendUpdate(
            txnContextCaptor.capture(), metadataTypeCaptor.capture(), timestampCaptor.capture());

        List<TxnContext> txnContexts = txnContextCaptor.getAllValues();
        List<LogReplicationMetadataType> metadataTypes = metadataTypeCaptor.getAllValues();
        List<Long> timestamps = timestampCaptor.getAllValues();

        for (int i = 0; i < numExpectedInvocations; i++) {
            Assert.assertEquals(expectedTxnContexts.get(i), txnContexts.get(i));
            Assert.assertEquals(expectedMetadataTypes.get(i), metadataTypes.get(i));
            Assert.assertEquals(expectedTimestamps.get(i), timestamps.get(i));
        }
    }

    private void updateMetadataManagerMap(Map<LogReplicationMetadataType, Long> metadataMap, boolean reset) {
        if (reset) {
            Mockito.reset(metadataManager);
            initMocksForMetadataManager();
        }
        Mockito.doReturn(metadataMap).when(metadataManager).queryMetadata(txnContext,
            LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED,
            LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED,
            LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED);
    }

    private void initMocksForMetadataManager() {
        Mockito.doReturn(Address.NON_ADDRESS).when(metadataManager).getLastAppliedSnapshotTimestamp();
        Mockito.doReturn(Address.NON_ADDRESS).when(metadataManager).getLastProcessedLogEntryBatchTimestamp();

        txnContext = Mockito.mock(TxnContext.class);
        Mockito.doReturn(txnContext).when(metadataManager).getTxnContext();
    }

    private Map<LogReplicationMetadataType, Long> constructMetadataMgrMap(long topologyConfigId,
        long lastTsInBatch, long lastOpaqueEntryTs, long lastSnapshotAppliedTs) {
        Map<LogReplicationMetadataType, Long> metadataMap = new HashMap<>();
        metadataMap.put(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        metadataMap.put(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, Address.NON_ADDRESS);
        metadataMap.put(LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, lastSnapshotAppliedTs);
        metadataMap.put(LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED, lastTsInBatch);
        metadataMap.put(LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED, lastOpaqueEntryTs);

        return metadataMap;
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }
}
