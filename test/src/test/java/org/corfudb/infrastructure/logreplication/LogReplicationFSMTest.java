package org.corfudb.infrastructure.logreplication;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_DATA_MSG_SIZE_SUPPORTED;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_CACHE_NUM_ENTRIES;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LR_STATUS_STREAM_TAG;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.NAMESPACE;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_STATUS_TABLE;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.compression.Codec;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationClusterInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.EmptyDataSender;
import org.corfudb.infrastructure.logreplication.replication.fsm.EmptySnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.InSnapshotSyncState;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationState;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationStateType;
import org.corfudb.infrastructure.logreplication.replication.fsm.TestDataSender;
import org.corfudb.infrastructure.logreplication.replication.fsm.TestLogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.TestReaderConfiguration;
import org.corfudb.infrastructure.logreplication.replication.fsm.TestSnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.DefaultReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal.SyncType;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.TableRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@Slf4j
/**
 * Test Log Replication FSM.
 */
public class LogReplicationFSMTest extends AbstractViewTest implements Observer {

    // Parameters for writes
    private static final int NUM_ENTRIES = 10;
    private static final String PAYLOAD_FORMAT = "%s hello world";
    private static final String TEST_NAMESPACE = "LR-Test";
    private static final String TEST_STREAM_NAME = "StreamA";
    private static final int BATCH_SIZE = 2;
    private static final int WAIT_TIME = 100;
    private static final int CORFU_PORT = 9000;
    private static final int TEST_TOPOLOGY_CONFIG_ID = 1;
    private static final String TEST_LOCAL_CLUSTER_ID = "local_cluster";
    private static final int MAX_SNAPSHOT_ENTRIES_APPLIED = 50;

    // This semaphore is used to block until the triggering event causes the transition to a new state
    private final Semaphore transitionAvailable = new Semaphore(1, true);
    // We observe the transition counter to know that a transition occurred.
    private ObservableValue transitionObservable;

    // Flag indicating if we should observer a snapshot sync, this is to interrupt it at any given stage
    private boolean observeSnapshotSync = false;
    private int limitSnapshotMessages = 0;
    private ObservableValue<Integer> snapshotMessageCounterObservable;

    private LogReplicationFSM fsm;
    private CorfuRuntime runtime;
    private CorfuStore corfuStore;
    private DataSender dataSender;
    private SnapshotReader snapshotReader;
    private LogReplicationAckReader ackReader;

    @Before
    public void setRuntime() {
        runtime = getDefaultRuntime();
        runtime.getParameters().setCodecType(Codec.Type.NONE);
        corfuStore = new CorfuStore(runtime);
    }

    @After
    public void stopAckReader() {
        ackReader.shutdown();
    }

    /**
     * Verify state machine behavior in the most simple (no error) path.
     *
     * This is the sequence of events triggered and expected state change:
     *
     * (1) None -> verify FSM initial state is INITIALIZED
     * (2) Replication Stop -> verify it stays in the INITIALIZED state as replication has not been started
     * (3) Replication Start -> IN_LOG_ENTRY_SYNC state
     * (4) Snapshot Sync Request -> IN_SNAPSHOT_SYNC state
     * (5) Snapshot Sync Complete -> IN_LOG_ENTRY_SYNC state
     * (6) Replication Stop -> back to INITIALIZED state
     *
     */
    @Test
    public void testLogReplicationFSMTransitions() throws Exception {

        initLogReplicationFSM(ReaderImplementation.EMPTY);

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        transitionAvailable.acquire();

        // Transition #1: Replication Stop (without any replication having started)
        transition(LogReplicationEventType.REPLICATION_STOP, LogReplicationStateType.INITIALIZED);

        // Transition #2: Log Entry Sync Start
        transition(LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST, LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // Transition #3: Snapshot Sync Request
        UUID snapshotSyncId = transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC, true);

        // Transition #4: Snapshot Sync Continue
        transition(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE, LogReplicationStateType.IN_SNAPSHOT_SYNC, snapshotSyncId, true);

        // Transition #5: Snapshot Sync Transfer Complete
        transition(LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE, LogReplicationStateType.WAIT_SNAPSHOT_APPLY, snapshotSyncId, false);

        // Transition #6: Snapshot Sync Apply still in progress
        transition(LogReplicationEventType.SNAPSHOT_APPLY_IN_PROGRESS, LogReplicationStateType.WAIT_SNAPSHOT_APPLY, false);

        // Transition #7: Snapshot Sync Apply Complete
        transition(LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE, LogReplicationStateType.IN_LOG_ENTRY_SYNC, snapshotSyncId, true);

        // Transition #8: Stop Replication
        // Next transition might not be to INITIALIZED, as IN_LOG_ENTRY_SYNC state might have enqueued
        // a continuation before the stop is enqueued.
        transition(LogReplicationEventType.REPLICATION_STOP, LogReplicationStateType.INITIALIZED, true);
    }

    /**
     * Verify the lastSyncType flag is being initialized properly and SnapshotSyncInfo
     * reflects accurate sync type for snapshot sync.
     *
     */
    @Test
    public void testSyncStatusUpdatesForSnapshotOnInit() throws Exception {
        final int updateToStatusTableFromOnEntry = 1;
        initLogReplicationFSM(ReaderImplementation.EMPTY);

        final Table<ReplicationStatusKey, ReplicationStatusVal, Message> statusTable =
                this.corfuStore.getTable(NAMESPACE, REPLICATION_STATUS_TABLE);

        CountDownLatch statusTableLatch = new CountDownLatch(updateToStatusTableFromOnEntry);
        TestStatusTableStreamListener streamListener = new TestStatusTableStreamListener(statusTableLatch);
        corfuStore.subscribeListener(streamListener, NAMESPACE, LR_STATUS_STREAM_TAG);

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        transitionAvailable.acquire();

        ReplicationStatusKey currentReplicationKey = ReplicationStatusKey.newBuilder().setClusterId(TEST_LOCAL_CLUSTER_ID).build();
        ReplicationStatusVal currentReplicationVal;
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            currentReplicationVal = txn.getRecord(statusTable, currentReplicationKey).getPayload();
        }

        // Default sync value is null so current syncType should not be set and default to SNAPSHOT.
        Assert.assertFalse(currentReplicationVal.hasField(ReplicationStatusVal.getDescriptor().findFieldByName("syncType")));

        // Current SyncStatus for ReplicationInfo and SnapshotSyncInfo should be NOT_STARTED
        Assert.assertEquals(SyncStatus.NOT_STARTED, currentReplicationVal.getStatus());
        Assert.assertEquals(SyncStatus.NOT_STARTED, currentReplicationVal.getSnapshotSyncInfo().getStatus());

        // Transition #1: SNAPSHOT Sync Start
        transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC);

        // Await for update to status table from entry of InSnapshotSyncState
        statusTableLatch.await();
        corfuStore.unsubscribeListener(streamListener);

        Set<SyncType> expectedSyncTypes = new HashSet<>(Collections.singletonList(SyncType.SNAPSHOT));
        Set<SyncType> actualSyncTypes = new HashSet<>();
        SyncStatus actualSyncStatus;
        SyncStatus actualSnapshotInfoSyncStatus;

        ArrayList<CorfuStreamEntries> streamEntries = streamListener.getEntries();
        ReplicationStatusVal replicationStatusVal = (ReplicationStatusVal)
                streamEntries.stream().findAny().get().getEntries().values().stream().findAny().get().get(0).getPayload();
        actualSyncTypes.add(replicationStatusVal.getSyncType());
        actualSyncStatus = replicationStatusVal.getStatus();
        actualSnapshotInfoSyncStatus = replicationStatusVal.getSnapshotSyncInfo().getStatus();

        // replicationSyncStatus (outer) starts as NOT_STARTED by initializeReplicationStatusTable,
        // and then gets updated to ONGOING in entry of InSnapshotSyncState
        SyncStatus replicationSyncStatusAfterEntry = SyncStatus.ONGOING;
        // snapshotInfoSyncStatus (inner) starts as NOT_STARTED by initializeReplicationStatusTable,
        // and then gets updated to ONGOING in entry of InSnapshotSyncState
        SyncStatus snapshotInfoSyncStatusAfterEntry = SyncStatus.ONGOING;

        Assert.assertEquals(expectedSyncTypes, actualSyncTypes);
        Assert.assertEquals(replicationSyncStatusAfterEntry, actualSyncStatus);
        Assert.assertEquals(snapshotInfoSyncStatusAfterEntry, actualSnapshotInfoSyncStatus);
    }

    /**
     * Verify the lastSyncType flag is being initialized properly and SnapshotSyncInfo
     * reflects accurate sync type for log entry sync.
     *
     */
    @Test
    public void testSyncStatusUpdatesForLogEntryOnInit() throws Exception {
        final int updateToStatusTableFromOnEntry = 1;
        initLogReplicationFSM(ReaderImplementation.EMPTY);

        final Table<ReplicationStatusKey, ReplicationStatusVal, Message> statusTable =
                this.corfuStore.getTable(NAMESPACE, REPLICATION_STATUS_TABLE);

        CountDownLatch statusTableLatch = new CountDownLatch(updateToStatusTableFromOnEntry);
        TestStatusTableStreamListener streamListener = new TestStatusTableStreamListener(statusTableLatch);
        corfuStore.subscribeListener(streamListener, NAMESPACE, LR_STATUS_STREAM_TAG);

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        transitionAvailable.acquire();

        ReplicationStatusKey currentReplicationKey = ReplicationStatusKey.newBuilder().setClusterId(TEST_LOCAL_CLUSTER_ID).build();
        ReplicationStatusVal currentReplicationVal;
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            currentReplicationVal = txn.getRecord(statusTable, currentReplicationKey).getPayload();
        }

        // Default sync value is null so current syncType should not be set and default to SNAPSHOT.
        Assert.assertFalse(currentReplicationVal.hasField(ReplicationStatusVal.getDescriptor().findFieldByName("syncType")));

        // Current SyncStatus for ReplicationInfo and SnapshotSyncInfo should be NOT_STARTED
        Assert.assertEquals(SyncStatus.NOT_STARTED, currentReplicationVal.getStatus());
        Assert.assertEquals(SyncStatus.NOT_STARTED, currentReplicationVal.getSnapshotSyncInfo().getStatus());

        // Transition #1: Log Entry Sync Start
        transition(LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST, LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // Await for update to status table from entry of InLogEntrySync
        statusTableLatch.await();
        corfuStore.unsubscribeListener(streamListener);

        Set<SyncType> expectedSyncTypes = new HashSet<>(Collections.singletonList(SyncType.LOG_ENTRY));
        Set<SyncType> actualSyncTypes = new HashSet<>();
        SyncStatus actualSyncStatus;
        SyncStatus actualSnapshotInfoSyncStatus;

        ArrayList<CorfuStreamEntries> streamEntries = streamListener.getEntries();
        ReplicationStatusVal replicationStatusVal = (ReplicationStatusVal)
                streamEntries.stream().findAny().get().getEntries().values().stream().findAny().get().get(0).getPayload();
        actualSyncTypes.add(replicationStatusVal.getSyncType());
        actualSyncStatus = replicationStatusVal.getStatus();
        actualSnapshotInfoSyncStatus = replicationStatusVal.getSnapshotSyncInfo().getStatus();

        // replicationSyncStatus (outer) starts as NOT_STARTED by initializeReplicationStatusTable,
        // and then gets updated to ONGOING in entry of InLogEntrySyncState
        SyncStatus replicationSyncStatusAfterEntry = SyncStatus.ONGOING;
        // snapshotInfoSyncStatus (inner) starts as NOT_STARTED by initializeReplicationStatusTable,
        // and then gets updated to COMPLETED in entry of InLogEntrySyncState
        SyncStatus snapshotInfoSyncStatusAfterEntry = SyncStatus.COMPLETED;

        Assert.assertEquals(expectedSyncTypes, actualSyncTypes);
        Assert.assertEquals(replicationSyncStatusAfterEntry, actualSyncStatus);
        Assert.assertEquals(snapshotInfoSyncStatusAfterEntry, actualSnapshotInfoSyncStatus);
    }

    /**
     * Verify the updates to the status table accurately reflect the sync type and sync status
     * for a transition from initialized to snapshot sync state.
     *
     */
    @Test
    public void testSyncStatusUpdatesForSnapshot() throws Exception {
        final int updateToStatusTableFromOnEntry = 3;
        initLogReplicationFSM(ReaderImplementation.EMPTY);

        CountDownLatch statusTableLatch = new CountDownLatch(updateToStatusTableFromOnEntry);
        TestStatusTableStreamListener streamListener = new TestStatusTableStreamListener(statusTableLatch);
        corfuStore.subscribeListener(streamListener, NAMESPACE, LR_STATUS_STREAM_TAG);

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        transitionAvailable.acquire();

        // Transition #1: SNAPSHOT Sync Start
        transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC);

        // Await for update to status table from entry of InSnapshotSyncState, an update stemming
        // from the SYNC_CANCEL request (since we are unable to send the snapshot data in a UT),
        // and an additional update from the TsPollingTask
        statusTableLatch.await();
        corfuStore.unsubscribeListener(streamListener);

        // Sync type should be snapshot for all three updates
        Set<SyncType> expectedSyncTypes = new HashSet<>(Collections.singletonList(SyncType.SNAPSHOT));
        // Sync status for both outer and inner should be ongoing for all three updates
        Set<SyncStatus> expectedSyncStatus = new HashSet<>(Collections.singletonList(SyncStatus.ONGOING));
        Set<SyncType> actualSyncTypes = new HashSet<>();
        Set<SyncStatus> actualSyncStatus = new HashSet<>();
        Set<SyncStatus> actualSnapshotInfoSyncStatus = new HashSet<>();

        Iterator<CorfuStreamEntries> entriesIterator = streamListener.getEntries().iterator();
        while (entriesIterator.hasNext()) {
            for (List<CorfuStreamEntry> entry : entriesIterator.next().getEntries().values()) {
                ReplicationStatusVal replicationStatusVal = (ReplicationStatusVal) entry.get(0).getPayload();
                actualSyncTypes.add(replicationStatusVal.getSyncType());
                actualSyncStatus.add(replicationStatusVal.getStatus());
                actualSnapshotInfoSyncStatus.add(replicationStatusVal.getSnapshotSyncInfo().getStatus());
            }
        }

        Assert.assertEquals(expectedSyncTypes, actualSyncTypes);
        Assert.assertEquals(expectedSyncStatus, actualSyncStatus);
        Assert.assertEquals(expectedSyncStatus, actualSnapshotInfoSyncStatus);
    }

    /**
     * Verify the updates to the status table accurately reflect the sync type and sync status
     * for a transition from initialized to log entry state.
     *
     */
    @Test
    public void testSyncStatusUpdatesForLogEntry() throws Exception {
        final int updateToStatusTableFromOnEntry = 2;
        initLogReplicationFSM(ReaderImplementation.EMPTY);

        CountDownLatch statusTableLatch = new CountDownLatch(updateToStatusTableFromOnEntry);
        TestStatusTableStreamListener streamListener = new TestStatusTableStreamListener(statusTableLatch);
        corfuStore.subscribeListener(streamListener, NAMESPACE, LR_STATUS_STREAM_TAG);

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        transitionAvailable.acquire();

        // Transition #1: Log Entry Sync Start
        transition(LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST, LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // Await for update to status table from entry of InLogEntrySyncState,
        // and an additional update from the TsPollingTask
        statusTableLatch.await();
        corfuStore.unsubscribeListener(streamListener);

        // Sync type should be log entry for both updates
        Set<SyncType> expectedSyncTypes = new HashSet<>(Collections.singletonList(SyncType.LOG_ENTRY));
        // Outer sync status should be ongoing for both updates
        Set<SyncStatus> expectedSyncStatus = new HashSet<>(Collections.singletonList(SyncStatus.ONGOING));
        // Inner sync status should be completed for both updates
        Set<SyncStatus> expectedSnapshotInfoSyncStatus = new HashSet<>(Collections.singletonList(SyncStatus.COMPLETED));
        Set<SyncType> actualSyncTypes = new HashSet<>();
        Set<SyncStatus> actualSyncStatus = new HashSet<>();
        Set<SyncStatus> actualSnapshotInfoSyncStatus = new HashSet<>();

        Iterator<CorfuStreamEntries> entriesIterator = streamListener.getEntries().iterator();
        while (entriesIterator.hasNext()) {
            for (List<CorfuStreamEntry> entry : entriesIterator.next().getEntries().values()) {
                ReplicationStatusVal replicationStatusVal = (ReplicationStatusVal) entry.get(0).getPayload();
                actualSyncTypes.add(replicationStatusVal.getSyncType());
                actualSyncStatus.add(replicationStatusVal.getStatus());
                actualSnapshotInfoSyncStatus.add(replicationStatusVal.getSnapshotSyncInfo().getStatus());
            }
        }

        Assert.assertEquals(expectedSyncTypes, actualSyncTypes);
        Assert.assertEquals(expectedSyncStatus, actualSyncStatus);
        Assert.assertEquals(expectedSnapshotInfoSyncStatus, actualSnapshotInfoSyncStatus);
    }

    /**
     * Verify the status table accurately reflects the sync type and sync status
     * for a transition from initialized -> snapshot sync state -> log entry state.
     *
     */
    @Test
    public void testSyncStatusUpdatesForSnapshotToLogEntryTransition() throws Exception {
        initLogReplicationFSM(ReaderImplementation.EMPTY);

        final Table<ReplicationStatusKey, ReplicationStatusVal, Message> statusTable =
                this.corfuStore.getTable(NAMESPACE, REPLICATION_STATUS_TABLE);

        ReplicationStatusKey currentReplicationKey = ReplicationStatusKey.newBuilder().setClusterId(TEST_LOCAL_CLUSTER_ID).build();
        ReplicationStatusVal currentReplicationVal;

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        transitionAvailable.acquire();

        // Transition #1: Snapshot Sync Request
        UUID snapshotSyncId = transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC);

        // Get replication status after snapshot sync start
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            currentReplicationVal = txn.getRecord(statusTable, currentReplicationKey).getPayload();
        }

        // Current SyncType should be SNAPSHOT
        Assert.assertEquals(SyncType.SNAPSHOT, currentReplicationVal.getSyncType());

        // Current SyncStatus for ReplicationInfo and SnapshotSyncInfo should be ONGOING
        Assert.assertEquals(SyncStatus.ONGOING, currentReplicationVal.getStatus());
        Assert.assertEquals(SyncStatus.ONGOING, currentReplicationVal.getSnapshotSyncInfo().getStatus());

        // Transition #2: Wait Snapshot Apply
        transition(LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE, LogReplicationStateType.WAIT_SNAPSHOT_APPLY, snapshotSyncId, false);

        // Transition #3: Log Entry Sync Start
        transition(LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE, LogReplicationStateType.IN_LOG_ENTRY_SYNC, snapshotSyncId, true);

        // Get replication status after log entry sync start
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            currentReplicationVal = txn.getRecord(statusTable, currentReplicationKey).getPayload();
        }

        // Current SyncType should be LOG_ENTRY
        Assert.assertEquals(SyncType.LOG_ENTRY, currentReplicationVal.getSyncType());

        // Current SyncStatus for ReplicationInfo should be ONGOING, and SyncStatus
        // for SnapshotSyncInfo should be COMPLETED
        Assert.assertEquals(SyncStatus.ONGOING, currentReplicationVal.getStatus());
        Assert.assertEquals(SyncStatus.COMPLETED, currentReplicationVal.getSnapshotSyncInfo().getStatus());
    }

    /**
     * Verify the status table accurately reflects the sync type and sync status
     * for a transition from initialized -> log entry state -> snapshot sync state.
     *
     */
    @Test
    public void testSyncStatusUpdatesForLogEntryToSnapshotTransition() throws Exception {
        initLogReplicationFSM(ReaderImplementation.EMPTY);

        final Table<ReplicationStatusKey, ReplicationStatusVal, Message> statusTable =
                this.corfuStore.getTable(NAMESPACE, REPLICATION_STATUS_TABLE);

        ReplicationStatusKey currentReplicationKey = ReplicationStatusKey.newBuilder().setClusterId(TEST_LOCAL_CLUSTER_ID).build();
        ReplicationStatusVal currentReplicationVal;

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        transitionAvailable.acquire();

        // Transition #1: Log Entry Sync Start
        transition(LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST, LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // Get replication status after log entry sync start
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            currentReplicationVal = txn.getRecord(statusTable, currentReplicationKey).getPayload();
        }

        // Current SyncType should be LOG_ENTRY
        Assert.assertEquals(SyncType.LOG_ENTRY, currentReplicationVal.getSyncType());

        // Current SyncStatus for ReplicationInfo should be ONGOING, and SyncStatus
        // for SnapshotSyncInfo should be COMPLETED
        Assert.assertEquals(SyncStatus.ONGOING, currentReplicationVal.getStatus());
        Assert.assertEquals(SyncStatus.COMPLETED, currentReplicationVal.getSnapshotSyncInfo().getStatus());

        // Transition #2: Snapshot Sync Request
        transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC, true);

        // Get replication status after snapshot sync start
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            currentReplicationVal = txn.getRecord(statusTable, currentReplicationKey).getPayload();
        }

        // Current SyncType should be SNAPSHOT
        Assert.assertEquals(SyncType.SNAPSHOT, currentReplicationVal.getSyncType());

        // Current SyncStatus for ReplicationInfo and SnapshotSyncInfo should be ONGOING
        Assert.assertEquals(SyncStatus.ONGOING, currentReplicationVal.getStatus());
        Assert.assertEquals(SyncStatus.ONGOING, currentReplicationVal.getSnapshotSyncInfo().getStatus());
    }

    /**
     * Test Trim Exception Events for Log Replication FSM
     *
     * This is the sequence of events triggered and expected state change:
     *
     * (1) Snapshot Sync Request => IN_SNAPSHOT_SYNC state
     * (2) Trimmed Exception (incorrect id) => IN_LOG_ENTRY_SYNC
     * (3) Trimmed Exception (for state in (5)) => IN_REQUIRE_SNAPSHOT_SYNC
     *
     * @throws Exception
     */
    @Test
    public void testTrimExceptionFSM() throws Exception {
        initLogReplicationFSM(ReaderImplementation.EMPTY);

        // Initial acquire of the semaphore, so the occurrence of the transition releases it for the transition itself.
        transitionAvailable.acquire();

        // Transition #1: Snapshot Sync Request
        transition(LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST, LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // A SYNC_CANCEL due to a trimmed exception, is an internal event generated during read in the log entry or
        // snapshot sync state, to ensure it is triggered during the state, and not before the task is
        // actually started on the worker thread, let's insert a delay.
        insertDelay(WAIT_TIME);

        // Transition #2: Trimmed Exception on Log Entry Sync for an invalid event id, this should not be taken as
        // a valid trimmed exception for the current state, hence it remains in the same state
        transition(LogReplicationEventType.SYNC_CANCEL, LogReplicationStateType.IN_LOG_ENTRY_SYNC, UUID.randomUUID(), false);

        // Transition #3: Trimmed Exception
        // Because this is an internal state, we need to capture the actual event id internally generated
        UUID logEntrySyncID = fsm.getStates().get(LogReplicationStateType.IN_LOG_ENTRY_SYNC).getTransitionEventId();
        transition(LogReplicationEventType.SYNC_CANCEL, LogReplicationStateType.IN_SNAPSHOT_SYNC, logEntrySyncID, true);
    }

    private void insertDelay(int timeMilliseconds) throws InterruptedException {
        sleep(timeMilliseconds);
    }

    /**
     * Test SnapshotSender through dummy implementations of the SnapshotReader and DataSender.
     *
     * (1) Initiate the Log Replication State Machine (which defaults to the INITIALIZED State)
     * (2) Write NUM_ENTRIES to the database in a consecutive address space for a given stream.
     * (3) Enforce event to initialize SNAPSHOT_SYNC.
     * (4) When SNAPSHOT_SYNC is completed the FSM should transition to a new state IN_LOG_ENTRY_SYNC. Block until
     * this transition occurs.
     * (5) Once this transition occurs verify that the Listener has received the same data written in (2).
     *
     * In this test we assume the SnapshotReader reads all NUM_ENTRIES in a single call (no batching)
     *
     * @throws Exception
     */
    @Test
    public void testLogReplicationSnapshotTransmitterNoBatch() throws Exception {
        testSnapshotSender(NUM_ENTRIES);
    }

    /**
     * Test SnapshotSender through dummy implementations of the SnapshotReader and DataSender.
     *
     * (1) Initiate the Log Replication State Machine (which defaults to the INITIALIZED State)
     * (2) Write NUM_ENTRIES to the database in a consecutive address space for a given stream.
     * (3) Enforce event to initialize SNAPSHOT_SYNC.
     * (4) When SNAPSHOT_SYNC is completed the FSM should transition to a new state IN_LOG_ENTRY_SYNC. Block until
     * this transition occurs.
     * (5) Once this transition occurs verify that the Listener has received the same data written in (2).
     *
     * In this test we assume the SnapshotReader reads NUM_ENTRIES in batches, and confirm all NUM_ENTRIES are
     * received by the listener.
     *
     * @throws Exception
     */
    @Test
    public void testLogReplicationSnapshotTransmitterBatch() throws Exception {
        testSnapshotSender(BATCH_SIZE);
    }

    private void testSnapshotSender(int batchSize) throws Exception {

        // Initialize State Machine
        initLogReplicationFSM(ReaderImplementation.TEST);

        // Modify test configuration to the specified batch size
        ((TestSnapshotReader)snapshotReader).setBatchSize(batchSize);

        // Write NUM_ENTRIES to streamA
        List<TokenResponse> writeTokens = writeToStream();

        List<Long> seqNums = new ArrayList<>();
        writeTokens.forEach(token -> seqNums.add(token.getSequence()));

        // Write to Stream will write to some addresses.  SnapshotReader should only read from those addresses
        ((TestSnapshotReader) snapshotReader).setSeqNumsToRead(seqNums);

        // Initial acquire of semaphore, the transition method will block until a transition occurs
        transitionAvailable.acquire();

        // Transition #1: Snapshot Sync Request
        UUID snapshotSyncRequestId = transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC);

        // Block until the snapshot sync completes and next transition occurs.
        // The transition should happen to IN_LOG_ENTRY_SYNC state.
        Queue<LogReplicationEntryMsg> listenerQueue = ((TestDataSender) dataSender).getEntryQueue();

        while(!fsm.getState().getType().equals(LogReplicationStateType.WAIT_SNAPSHOT_APPLY)) {
            transitionAvailable.acquire();
        }

        assertThat(fsm.getState().getType()).isEqualTo(LogReplicationStateType.WAIT_SNAPSHOT_APPLY);

        transition(LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE, LogReplicationStateType.IN_LOG_ENTRY_SYNC, snapshotSyncRequestId, true);
        assertThat(fsm.getState().getType()).isEqualTo(LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        assertThat(listenerQueue.size()).isEqualTo(NUM_ENTRIES);

        for (int i = 0; i < NUM_ENTRIES; i++) {
            assertThat(listenerQueue.poll().getData())
                    .isEqualTo(ByteString.copyFrom(String.format(PAYLOAD_FORMAT, i).getBytes()));
        }
    }

    /**
     * This test verifies that canceling a snapshot sync which is in progress, takes log replication
     * back to the INITIALIZED state and that snapshot sync can be retried and completed successfully.
     *
     * (1) Write NUM_ENTRIES to StreamA
     * (2) Trigger start of SNAPSHOT_SYNC
     * (3) Interrupt/Stop snapshot sync when 2 messages have been sent to the remote site.
     * (4) Re-trigger SNAPSHOT_SYNC
     * (5) Check for completeness, i.e., that state has changed to IN_LOG_ENTRY_SYNC
     *
     * @throws Exception
     */
    @Test
    public void cancelSnapshotSyncInProgressAndRetry() throws Exception {
        // This test needs to observe the number of messages generated during snapshot sync to interrupt/stop it,
        // before it completes.
        observeSnapshotSync = true;

        // Initialize State Machine
        initLogReplicationFSM(ReaderImplementation.TEST);

        // Modify test configuration to the specified batch size (since we write NUM_ENTRIES = 10) and we send in
        // batches of BATCH_SIZE = 2, we will stop snapshot sync at 2 sent messages.
        ((TestSnapshotReader)snapshotReader).setBatchSize(BATCH_SIZE);
        limitSnapshotMessages = 2;

        // Write NUM_ENTRIES to streamA
        List<TokenResponse> writeTokens = writeToStream();

        List<Long> seqNums = new ArrayList<>();
        writeTokens.forEach(token -> seqNums.add(token.getSequence()));

        // Write to Stream will write to some addresses.  SnapshotReader should only read from those addresses
        ((TestSnapshotReader) snapshotReader).setSeqNumsToRead(seqNums);

        // Initial acquire of semaphore, the transition method will block until a transition occurs
        transitionAvailable.acquire();

        log.debug("**** Transition to Snapshot Sync");
        // Transition #1: Snapshot Sync Request
        transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC);

        // We observe the number of transmitted messages and force a REPLICATION_STOP, when 2 messages have been sent
        // so we verify the state moves to INITIALIZED again.
        transitionAvailable.acquire();
        log.debug("**** Stop Replication");
        fsm.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_STOP));

        transitionAvailable.acquire();

        while (fsm.getState().getType() != LogReplicationStateType.INITIALIZED) {
            // Wait on a FSM transition to occur
            transitionAvailable.acquire();
        }

        assertThat(fsm.getState().getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        ((TestDataSender) dataSender).reset();

        // Stop observing number of messages in snapshot sync, so this time it completes
        observeSnapshotSync = false;

        // Transition #2: This time the snapshot sync completes
        UUID snapshotSyncId = transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC, true);

        while (fsm.getState().getType() != LogReplicationStateType.WAIT_SNAPSHOT_APPLY) {
            // Block until FSM moves back to in log entry (delta) sync state
            transitionAvailable.acquire();
        }

        assertThat(fsm.getState().getType()).isEqualTo(LogReplicationStateType.WAIT_SNAPSHOT_APPLY);

        Queue<LogReplicationEntryMsg> listenerQueue = ((TestDataSender) dataSender).getEntryQueue();

        assertThat(listenerQueue.size()).isEqualTo(NUM_ENTRIES);

        log.debug("**** Snapshot Sync Complete");
        transition(LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE, LogReplicationStateType.IN_LOG_ENTRY_SYNC, snapshotSyncId, true);

        assertThat(fsm.getState().getType()).isEqualTo(LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        for (int i=0; i<NUM_ENTRIES; i++) {
            assertThat(listenerQueue.poll().getData())
                    .isEqualTo(ByteString.copyFrom(String.format(PAYLOAD_FORMAT, i).getBytes()));
        }
    }


    /**
     * Test Snapshot Sync for Default Stream-based implementations.
     *
     * @throws Exception
     */
    @Test
    public void testSnapshotSyncStreamImplementation() throws Exception {

        // Initialize State Machine
        initLogReplicationFSM(ReaderImplementation.STREAMS);

        // Write LARGE_NUM_ENTRIES to streamA
        writeToMap();

        // Initial acquire of semaphore, the transition method will block until a transition occurs
        transitionAvailable.acquire();

        // Transition #1: Replication Start
        // transition(LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST, LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // Transition #2: Snapshot Sync Request
        UUID snapshotSyncRequestId = transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST,
                LogReplicationStateType.IN_SNAPSHOT_SYNC, true);

        // Block until the snapshot sync completes and next transition occurs.
        // The transition should happen to IN_LOG_ENTRY_SYNC state.
        log.debug("**** Wait for snapshot sync to complete");

        // Block until the snapshot sync completes and next transition occurs.
        while (fsm.getState().getType() != LogReplicationStateType.WAIT_SNAPSHOT_APPLY) {
            log.trace("Current state={}, expected={}", fsm.getState().getType(), LogReplicationStateType.WAIT_SNAPSHOT_APPLY);
            // Acquire the semaphore again in case transition is blocked with SNAPSHOT_SYNC_CONTINUE event
            transitionAvailable.acquire();
        }

        assertThat(fsm.getState().getType()).isEqualTo(LogReplicationStateType.WAIT_SNAPSHOT_APPLY);

        transition(LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE, LogReplicationStateType.IN_LOG_ENTRY_SYNC, snapshotSyncRequestId, true);

        assertThat(fsm.getState().getType()).isEqualTo(LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // Transactional puts into the stream (incremental updates)
        writeToMap();

        int incrementalUpdates = 0;

        while(incrementalUpdates < NUM_ENTRIES) {
           ((TestDataSender)dataSender).getEntryQueue().poll();
           incrementalUpdates++;
        }

        assertThat(incrementalUpdates).isEqualTo(NUM_ENTRIES);
    }

    private void writeToMap() throws Exception {
        Table<Sample.StringKey, Sample.StringKey, Sample.Metadata> map = corfuStore.openTable(
                TEST_NAMESPACE,
                TEST_STREAM_NAME,
                Sample.StringKey.class,
                Sample.StringKey.class,
                Sample.Metadata.class,
                TableOptions.fromProtoSchema(Sample.IntValueTag.class)
        );

        for (int i = 0; i < NUM_ENTRIES; i++) {
            try (TxnContext txn = corfuStore.txn(TEST_NAMESPACE)) {
                txn.putRecord(map, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build(), null);
                txn.commit();
            }
        }
    }

    private List<TokenResponse> writeToStream() {
        UUID streamA = UUID.nameUUIDFromBytes(TEST_STREAM_NAME.getBytes());
        List<TokenResponse> writeTokens = new ArrayList<>();
        // Write
        for (int i=0; i < NUM_ENTRIES; i++) {
            TokenResponse response = runtime.getSequencerView().next(streamA);
            writeTokens.add(response);
            runtime.getAddressSpaceView().write(response, String.format(PAYLOAD_FORMAT, i).getBytes());
        }

        // Read to verify data is there
        int index = 0;
        for (TokenResponse token : writeTokens) {
            assertThat(runtime.getAddressSpaceView().read((long)token.getSequence()).getPayload(getRuntime()))
                    .isEqualTo( String.format(PAYLOAD_FORMAT, index).getBytes());
            index++;
        }
        return writeTokens;
    }

    /**
     * Initialize Log Replication FSM
     *
     * Use empty implementations for those cases where you want to verify the behavior of the state machine.
     *
     * @param readerImpl implementation to use for readers.
     */
    private void initLogReplicationFSM(ReaderImplementation readerImpl) {

        String fullyQualifiedStreamName = TableRegistry.getFullyQualifiedTableName(TEST_NAMESPACE, TEST_STREAM_NAME);
        LogEntryReader logEntryReader = new TestLogEntryReader();
        LogReplicationMetadataManager metadataManager = new LogReplicationMetadataManager(runtime, TEST_TOPOLOGY_CONFIG_ID,
                TEST_LOCAL_CLUSTER_ID);
        CorfuRuntime newRT = getNewRuntime(getDefaultNode()).connect();
        LogReplicationConfigManager tableManagerPlugin = new LogReplicationConfigManager(newRT);
        LogReplicationConfig config = new LogReplicationConfig(tableManagerPlugin, DEFAULT_MAX_NUM_MSG_PER_BATCH,
                MAX_DATA_MSG_SIZE_SUPPORTED, MAX_CACHE_NUM_ENTRIES, DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED);

        switch(readerImpl) {
            case EMPTY:
                // Empty implementations of log reader and listener - used for testing transitions
                snapshotReader = new EmptySnapshotReader();
                dataSender = new EmptyDataSender();
                break;
            case TEST:
                // Dummy implementations of log reader and listener for testing
                // The log reader queries the log for the config provided (stream name, number of entries)
                // The listener inserts what it receives into a queue
                TestReaderConfiguration testConfig = TestReaderConfiguration.builder()
                        .endpoint(getDefaultEndpoint())
                        .numEntries(NUM_ENTRIES)
                        .payloadFormat(PAYLOAD_FORMAT)
                        .streamName(fullyQualifiedStreamName)
                        .batchSize(BATCH_SIZE).build();

                snapshotReader = new TestSnapshotReader(testConfig);
                dataSender = new TestDataSender();
                break;
            case STREAMS:
                // Default implementation used for Log Replication (stream-based)
                snapshotReader = new StreamsSnapshotReader(newRT, config);
                dataSender = new TestDataSender();
                break;
            default:
                break;
        }

        // Manually initialize the replication status table
        metadataManager.initializeReplicationStatusTable(TEST_LOCAL_CLUSTER_ID);

        ackReader = new LogReplicationAckReader(metadataManager, config, runtime, TEST_LOCAL_CLUSTER_ID);
        fsm = new LogReplicationFSM(runtime, snapshotReader, dataSender, logEntryReader,
                new DefaultReadProcessor(runtime), config, new ClusterDescriptor("Cluster-Local",
                LogReplicationClusterInfo.ClusterRole.ACTIVE, CORFU_PORT),
                Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("fsm-worker").build()),
                ackReader, tableManagerPlugin);
        ackReader.setLogEntryReader(fsm.getLogEntryReader());
        transitionObservable = fsm.getNumTransitions();
        transitionObservable.addObserver(this);

        if (observeSnapshotSync) {
            log.debug("Observe snapshot sync");
            snapshotMessageCounterObservable = ((InSnapshotSyncState) fsm.getStates()
                    .get(LogReplicationStateType.IN_SNAPSHOT_SYNC)).getSnapshotSender().getObservedCounter();
            snapshotMessageCounterObservable.addObserver(this);
        }
    }

    /**
     * It performs a transition, based on the given event type and asserts the FSM has moved to the expected state
     *
     * @param eventType log replication event.
     * @param expectedState expected state after transition is completed.
     * @param eventId identifier of the event.
     */
    private UUID transition(LogReplicationEventType eventType,
                            LogReplicationStateType expectedState,
                            UUID eventId, boolean waitUntilExpected)
            throws InterruptedException {

        log.debug("Insert event {}", eventType);

        LogReplicationEvent event;

        if (eventId != null) {
            // For testing we are enforcing internal events (like Trimmed Exception or Snapshot Complete),
            // for this reason we must set the id of the event that preceded it, so it corresponds to the same state.
            event = new LogReplicationEvent(eventType, new LogReplicationEventMetadata(eventId));
        } else {
            event = new LogReplicationEvent(eventType);
        }

        fsm.input(event);

        transitionAvailable.acquire();

        // Wait until the expected state
        while (waitUntilExpected) {
            if (fsm.getState().getType() == expectedState) {
                return event.getEventId();
            } else {
                transitionAvailable.acquire();
            }
        }

        assertThat(fsm.getState().getType()).isEqualTo(expectedState);

        return event.getEventId();
    }

    /**
     * Insert an event of eventType into the Log Replication FSM and assert on the expected state.
     *
     * This method blocks until an actual transition occurs in the FSM.
     *
     * @param eventType the type of the event to input into the state machine
     * @param expectedState the expected state to transition to
     *
     * @return UUID of the input event
     *
     * @throws InterruptedException
     */
    private UUID transition(LogReplicationEventType eventType,
                            LogReplicationStateType expectedState, boolean waitUntilExpected) throws InterruptedException {
        return transition(eventType, expectedState, null, waitUntilExpected);
    }

    private UUID transition(LogReplicationEventType eventType,
                            LogReplicationStateType expectedState) throws InterruptedException {
        return transition(eventType, expectedState, null, false);
    }

    /**
     * Observer callback, will be called on every transition of the log replication FSM.
     */
    @Override
    public void update(Observable obs, Object arg) {
        if (obs.equals(transitionObservable)) {
            while (!transitionAvailable.hasQueuedThreads()) {
                // Wait until some thread is waiting to acquire...
            }
            transitionAvailable.release();
            // log.debug("Transition::#"  + transitionObservable.getValue() + "::" + fsm.getState().getType());
        } else if (obs.equals(snapshotMessageCounterObservable) &&
                limitSnapshotMessages == snapshotMessageCounterObservable.getValue() && observeSnapshotSync) {
            // If number of messages in snapshot reaches the expected value force termination of SNAPSHOT_SYNC
            // log.debug("Insert event: " + LogReplicationEventType.REPLICATION_STOP);
            fsm.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_STOP));
        }
    }

    /**
     * StreamListener for updates to statusTable, utilizes countdown latch to count entries coming
     * from markSnapshotSyncInfoOngoing and markSnapshotSyncInfoCompleted.
     *
     */
    class TestStatusTableStreamListener implements StreamListener {
        @Getter
        ArrayList<CorfuStreamEntries> entries = new ArrayList<>();

        @Getter
        Throwable throwable;

        CountDownLatch countDownLatch;

        public TestStatusTableStreamListener(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public void onNext(CorfuStreamEntries results) {
            if (countDownLatch.getCount() > 0) {
                entries.add(results);
                for (int i = 0; i < results.getEntries().size(); i++) {
                    countDownLatch.countDown();
                }
            }
        }

        public void onError(Throwable throwable) {
            this.throwable = throwable;
        }
    }

    public enum ReaderImplementation {
        EMPTY,
        TEST,
        STREAMS
    }
}
