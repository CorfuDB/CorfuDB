package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test suite for Log Entry Writer and Metadata Manager updates as part of apply of replicated data on Sink
 */
@SuppressWarnings("checkstyle:magicnumber")
public class LogEntryWriterTest extends AbstractViewTest {

    private static final String LOCAL_SINK_CLUSTER_ID = DefaultClusterConfig.getSinkClusterIds().get(0);
    private CorfuRuntime corfuRuntime;
    private LogReplicationMetadataManager metadataManager = Mockito.mock(LogReplicationMetadataManager.class);
    private TxnContext txnContext = Mockito.mock(TxnContext.class);
    private LogEntryWriter logEntryWriter;

    private TestUtils utils = new TestUtils();
    private final int numOpaqueEntries = 3;
    private final int topologyConfigId = 5;

    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();

        Mockito.doReturn(txnContext).when(metadataManager).getTxnContext();
        Mockito.doReturn(getDefaultMetadata()).when(metadataManager).queryReplicationMetadata(txnContext,
            getDefaultSession());
        Mockito.doReturn(getDefaultMetadata()).when(metadataManager).getReplicationMetadata(getDefaultSession());

        logEntryWriter = new LogEntryWriter(metadataManager, getDefaultSession(),
                new LogReplicationContext(new LogReplicationConfigManager(corfuRuntime, LOCAL_SINK_CLUSTER_ID), topologyConfigId,
                        getEndpoint(SERVERS.PORT_0), Mockito.mock(LogReplicationPluginConfig.class)));
    }

    @After
    public void tearDown() {
        corfuRuntime.shutdown();
    }

    /**
     * Verify log entry (delta) updates are applied on the Sink respecting source transaction boundaries.
     *
     * This test relies on the premise that each OpaqueEntry maps to a transaction on the Source.
     * Hence, each should be applied on the Sink as a separate transaction.
     */
    @Test
    public void testLogEntryApplyWithExpectedTx() {

        // (1) Generate log entry message for 'numOpaqueEntries'
        LogReplicationEntryMsg lrEntryMsg = utils.generateLogEntryMsg(1, numOpaqueEntries, Address.NON_ADDRESS,
            topologyConfigId, Address.NON_ADDRESS);

        // (2) Apply opaque entries and verify it has been successful, also verify they were applied in separate Txs
        Assert.assertTrue(logEntryWriter.apply(lrEntryMsg));
        verifyNumberOfTx(numOpaqueEntries);

        // (3) Verify metadata updates during 'apply' phase
        // Construct the expected arguments and order with which the metadata updates will be written.
        // There will be 1 update per opaque entry
        List<TxnContext> txnContexts = new ArrayList<>();
        List<LogReplicationSession> sessions = new ArrayList<>();
        List<ReplicationMetadata> metadataList = new ArrayList<>();

        for (int i = 0; i < numOpaqueEntries; i++) {
            txnContexts.add(txnContext);
            sessions.add(getDefaultSession());
            long batchTs = (i < (numOpaqueEntries - 1)) ? getDefaultMetadata().getLastLogEntryBatchProcessed() : (i + 1);
            metadataList.add(getDefaultMetadata().toBuilder()
                    .setTopologyConfigId(topologyConfigId)
                    .setLastLogEntryApplied(i + 1)
                    .setLastLogEntryBatchProcessed(batchTs)
                    .build());
        }

        verifyMetadataAppliedAndOrder(numOpaqueEntries, txnContexts, sessions, metadataList);
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

        resetMock();

        // Verify that the redundant update was not applied
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));
        verifyNumberOfTx(0);

        verifyMetadataAppliedAndOrder(0, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
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

        ReplicationMetadata mismatchMetadata = getDefaultMetadata().toBuilder()
                .setLastLogEntryBatchProcessed(numOpaqueEntries)
                .setLastLogEntryApplied(numOpaqueEntries)
                .setLastSnapshotApplied(Address.NON_ADDRESS)
                .build();
        resetMock();
        Mockito.doReturn(mismatchMetadata).when(metadataManager).queryReplicationMetadata(txnContext, getDefaultSession());

        Assert.assertTrue(logEntryWriter.apply(lrEntryMsg));

        // As sequence numbers [1-3] were already applied, only the transaction corresponding to sequence number 4
        // will be applied.  So the total number of times commit is invoked should be 1
        verifyNumberOfTx(1);

        // Verify that the right metadata was applied - there will be 1 write, with the updated values of
        // LAST_LOG_ENTRY_APPLIED and LAST_LOG_ENTRY_BATCH_PROCESSED
        List<TxnContext> txnContexts = Arrays.asList(txnContext);
        List<LogReplicationSession> sessions = Arrays.asList(getDefaultSession());
        List<ReplicationMetadata> metadataList = Arrays.asList(getDefaultMetadata()
                .toBuilder()
                .setLastLogEntryApplied(endTs)
                .setLastLogEntryBatchProcessed(endTs)
                .build());

        verifyMetadataAppliedAndOrder(1, txnContexts, sessions, metadataList);
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

        ReplicationMetadata expectedMetadata = getDefaultMetadata().toBuilder()
                .setTopologyConfigId(topologyConfigId)
                .setLastLogEntryBatchProcessed(numOpaqueEntries)
                .setLastLogEntryApplied(numOpaqueEntries)
                .setLastSnapshotApplied(Address.NON_ADDRESS)
                .build();

        // Throw an exception when sequence number = 3 is being applied. This will simulate a partial apply.
        Mockito.doThrow(InterruptedException.class).when(metadataManager).updateReplicationMetadata(txnContext,
                getDefaultSession(), expectedMetadata);

        // Verify that the message containing the list of opaque entries was not applied
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // Verify that 2 transactions were made, 1 for opaque entries 1 and 2
        // The number of times commit() is invoked should be equal to 2
        verifyNumberOfTx(numOpaqueEntries - 1);

        // Construct the next batch with sequence numbers [1-4]
        int endTs = numOpaqueEntries + 1;
        lrEntryMsg = utils.generateLogEntryMsg(startTs, endTs, Address.NON_ADDRESS, topologyConfigId, Address.NON_ADDRESS);

        // Construct metadata to reflect the previous partial apply
        ReplicationMetadata metadataPartialApply = getDefaultMetadata().toBuilder()
                .setLastLogEntryBatchProcessed(Address.NON_ADDRESS)
                .setLastLogEntryApplied(numOpaqueEntries - 1)
                .setLastSnapshotApplied(Address.NON_ADDRESS)
                .build();
        resetMock();
        Mockito.doReturn(metadataPartialApply).when(metadataManager).queryReplicationMetadata(txnContext,
            getDefaultSession());

        // Verify that the message was applied successfully.
        Assert.assertTrue(logEntryWriter.apply(lrEntryMsg));

        // Only 2 entries from the new message will be applied - 3 and 4.
        verifyNumberOfTx(2);

        List<TxnContext> txnContexts = Arrays.asList(txnContext, txnContext);
        List<LogReplicationSession> sessions = Arrays.asList(getDefaultSession(), getDefaultSession());
        List<ReplicationMetadata> metadataList = new ArrayList<>();

        for (int i = numOpaqueEntries; i <= endTs; i++) {
            long batchTs = (i == endTs) ? endTs : Address.NON_ADDRESS;
            metadataList.add(getDefaultMetadata().toBuilder()
                    .setTopologyConfigId(topologyConfigId)
                    .setLastLogEntryApplied(i)
                    .setLastLogEntryBatchProcessed(batchTs)
                    .build());
        }

        verifyMetadataAppliedAndOrder(2, txnContexts, sessions, metadataList);
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
        ReplicationMetadata mismatchMetadata = getDefaultMetadata().toBuilder()
                .setLastLogEntryBatchProcessed(0)
                .setLastLogEntryApplied(0)
                .build();
        resetMock();
        Mockito.doReturn(mismatchMetadata).when(metadataManager).queryReplicationMetadata(txnContext,
            getDefaultSession());

        // Verify that the data was not applied due to mismatch in the last log entry batch processed timestamps
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // The number of times commit() is invoked should be 0 as validation failed
        verifyNumberOfTx(0);

        List<TxnContext> txnContexts = new ArrayList<>();

        verifyMetadataAppliedAndOrder(0, txnContexts, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
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
        ReplicationMetadata invalidMetadata = getDefaultMetadata().toBuilder()
                .setLastSnapshotApplied(0)
                .build();
        resetMock();
        Mockito.doReturn(invalidMetadata).when(metadataManager).queryReplicationMetadata(txnContext,
            getDefaultSession());

        // Verify that the data was not applied because the snapshot timestamp from Source was lower
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // The number of times commit() is invoked should be 0 as validation failed
        verifyNumberOfTx(0);

        verifyMetadataAppliedAndOrder(0, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
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

        // Metadata on the Sink is already initialized with default(init) values, i.e., Address.NON_ADDRESS.  Verify
        // that the log entry message was not applied as the incoming snapshot ts is higher
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // The number of times commit() was invoked should be 0 as validation failed
        verifyNumberOfTx(0);

        verifyMetadataAppliedAndOrder(0, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
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
        ReplicationMetadata invalidMetadata = getDefaultMetadata().toBuilder()
                .setTopologyConfigId(topologyConfigId + 1)
                .build();
        resetMock();
        Mockito.doReturn(invalidMetadata).when(metadataManager).queryReplicationMetadata(txnContext,
            getDefaultSession());

        // Verify that the data was not applied due to mismatch in the topology config ids
        Assert.assertFalse(logEntryWriter.apply(lrEntryMsg));

        // The number of times commit() is invoked should be 0 as validation failed
        verifyNumberOfTx(0);

        verifyMetadataAppliedAndOrder(0, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }

    private void verifyNumberOfTx(int numExpectedTx) {
        // Verify that each opaque entry was applied in a separate transaction
        // The number of times commit() is invoked should be equal to numOpaqueEntries
        Mockito.verify(txnContext, Mockito.times(numExpectedTx)).commit();
    }

    private void resetMock() {
        Mockito.reset(metadataManager);
        Mockito.reset(txnContext);
        Mockito.doReturn(txnContext).when(metadataManager).getTxnContext();
    }

    private void verifyMetadataAppliedAndOrder(int numExpectedInvocations, List<TxnContext> expectedTxnContexts,
                                               List<LogReplicationSession> expectedSessions,
                                               List<ReplicationMetadata> expectedReplicationMetadata) {

        ArgumentCaptor<TxnContext> txnContextCaptor = ArgumentCaptor.forClass(TxnContext.class);
        ArgumentCaptor<LogReplicationSession> sessionCaptor = ArgumentCaptor.forClass(LogReplicationSession.class);
        ArgumentCaptor<ReplicationMetadata> metadataCaptor = ArgumentCaptor.forClass(ReplicationMetadata.class);

        Mockito.verify(metadataManager, Mockito.times(numExpectedInvocations))
                .updateReplicationMetadata(txnContextCaptor.capture(), sessionCaptor.capture(), metadataCaptor.capture());

        List<TxnContext> txnContexts = txnContextCaptor.getAllValues();
        List<LogReplicationSession> sessions = sessionCaptor.getAllValues();
        List<ReplicationMetadata> metadata = metadataCaptor.getAllValues();

        for (int i = 0; i < numExpectedInvocations; i++) {
            Assert.assertEquals(expectedTxnContexts.get(i), txnContexts.get(i));
            Assert.assertEquals(expectedSessions.get(i), sessions.get(i));
            Assert.assertEquals(expectedReplicationMetadata.get(i), metadata.get(i));
        }
    }

    private LogReplicationSession getDefaultSession() {
        return DefaultClusterConfig.getSessions().get(0);
    }

    /**
     * Retrieve default metadata (initial metadata)
     *
     */
    private ReplicationMetadata getDefaultMetadata() {
        return ReplicationMetadata.newBuilder()
                .setTopologyConfigId(topologyConfigId)
                .setLastLogEntryApplied(Address.NON_ADDRESS)
                .setLastLogEntryBatchProcessed(Address.NON_ADDRESS)
                .setLastSnapshotTransferredSeqNumber(Address.NON_ADDRESS)
                .setLastSnapshotApplied(Address.NON_ADDRESS)
                .setLastSnapshotTransferred(Address.NON_ADDRESS)
                .setLastSnapshotStarted(Address.NON_ADDRESS)
                .setCurrentCycleMinShadowStreamTs(Address.NON_ADDRESS)
                .build();
    }
}
