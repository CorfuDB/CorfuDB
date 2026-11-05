package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.health.Component;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.SnapshotSyncLeaseStore;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class CompactorLeaderServicesUnitTest {
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);
    private final CorfuStore corfuStore = mock(CorfuStore.class);


    private static final String NAMESPACE = "TestNamespace";
    private static final String TABLE_NAME = "TestTableName";
    private final CorfuStoreMetadata.TableName tableName = CorfuStoreMetadata.TableName.newBuilder()
            .setNamespace(NAMESPACE).setTableName(TABLE_NAME + "0").build();
    private final CorfuStoreMetadata.TableName tableName2 = CorfuStoreMetadata.TableName.newBuilder()
            .setNamespace(NAMESPACE).setTableName(TABLE_NAME + "1").build();

    private CompactorLeaderServices compactorLeaderServices;
    private final LivenessValidator livenessValidator = mock(LivenessValidator.class);

    @Before
    public void setup() throws Exception {

        this.compactorLeaderServices = new CompactorLeaderServices(corfuRuntime, "NodeEndpoint", corfuStore, livenessValidator);

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        when(txn.getRecord(SnapshotSyncLeaseStore.TABLE_NAME, SnapshotSyncLeaseStore.DOMAIN)).thenReturn(null);
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.MIN_CHECKPOINT))
                .thenReturn(new CorfuStoreEntry<>(CompactorMetadataTables.MIN_CHECKPOINT, null, null));
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        doNothing().when(txn).delete(anyString(), any(Message.class));
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());
        when(corfuStore.openTable(any(), any(), any(), any(), any(), any())).thenReturn(mock(Table.class));

        // Mirror production startup (ManagementAgent reports the COMPACTOR init issue,
        // CompactorService resolves it once started) so COMPACTOR reaches INITIALIZED and
        // runtime issues (e.g. CHECKPOINT_STALLED) can be reported/resolved in tests below.
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.COMPACTOR));
    }

    @After
    public void tearDown() {
        HealthMonitor.shutdown();
    }

    @Test
    public void initCompactionCycleTest() {
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, compactorLeaderServices.initCompactionCycle());

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        when(corfuStore.listTables(null)).thenReturn(Collections.singletonList(tableName));
        when(corfuRuntime.getAddressSpaceView()).thenReturn(mock(AddressSpaceView.class));
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS, compactorLeaderServices.initCompactionCycle());
    }

    /**
     * A new cycle yields to snapshot protection, but only while that protection is in force: once it
     * is older than its deadline plus the grace, the cycle starts and reclaims what the attempt left.
     */
    @Test
    public void initCompactionCycleYieldsOnlyToUnexpiredSnapshotProtection() {
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        when(corfuStore.listTables(null)).thenReturn(Collections.singletonList(tableName));
        when(corfuRuntime.getAddressSpaceView()).thenReturn(mock(AddressSpaceView.class));
        long now = System.currentTimeMillis();
        long grace = SnapshotSyncLeaseStore.expiryGraceMs();

        holdLease(now + Duration.ofMinutes(10).toMillis());
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, compactorLeaderServices.initCompactionCycle());

        holdLease(now - grace + Duration.ofMinutes(1).toMillis());
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, compactorLeaderServices.initCompactionCycle());

        holdLease(now - grace - 1);
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS, compactorLeaderServices.initCompactionCycle());
    }

    @Test
    public void protectionHeldPastItsDeadlineIsReportedWithItsSource() {
        long now = System.currentTimeMillis();
        long grace = SnapshotSyncLeaseStore.expiryGraceMs();
        freezeToken(null);

        holdLease(now + Duration.ofMinutes(10).toMillis());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNull("A running attempt inside its budget is not an issue", frozenIssue());

        holdLease(now - Duration.ofMinutes(10).toMillis());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNotNull(frozenIssue());
        Assert.assertTrue(frozenIssue().getDescription().contains("10 minutes past its deadline"));
        Assert.assertTrue(frozenIssue().getDescription().contains("stays frozen"));
        Assert.assertNull(issue(Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED));

        // The health status keeps the description an issue was first reported with: it must follow.
        holdLease(now - Duration.ofMinutes(20).toMillis());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertTrue(frozenIssue().getDescription().contains("20 minutes past its deadline"));

        // Past the grace checkpointing is not frozen any more. What remains wrong is that log
        // replication never settled its record: its sink has no leader, or the leader is hung.
        holdLease(now - grace - Duration.ofMinutes(10).toMillis());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNull(frozenIssue());
        Assert.assertNotNull(issue(Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED));
        Assert.assertTrue(issue(Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED).getDescription().contains("Checkpointing ignores it"));

        when(txn.getRecord(SnapshotSyncLeaseStore.TABLE_NAME, SnapshotSyncLeaseStore.DOMAIN)).thenReturn(null);
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNull("Released protection resolves the issue", frozenIssue());
        Assert.assertNull(issue(Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED));
    }

    /**
     * The log replication process has no health monitor, so what it detects it can only log. The
     * lease is durable and the compactor leader reads it on every pass anyway.
     */
    @Test
    public void conditionsOfLogReplicationAreReportedFromTheLease() {
        long now = System.currentTimeMillis();
        freezeToken(null);
        SnapshotSyncLeaseRecord.Builder lease = SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1)
                .setGeneration(9).setRecoveryCut(500).setFailure("Snapshot resource deadline expired");

        // Recovery that has only just begun is normal, as are two abandoned attempts.
        lease(lease.setPhase(SnapshotSyncLeaseRecord.Phase.RECOVERING).setConsecutiveAborts(2)
                .setReleasedAtMs(now - Duration.ofMinutes(5).toMillis()).build());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNull(issue(Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED));
        Assert.assertNull(issue(Issue.IssueId.SNAPSHOT_SYNC_FAILING));

        lease(lease.setConsecutiveAborts(3).setReleasedAtMs(now - Duration.ofMinutes(31).toMillis()).build());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertTrue(issue(Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED).getDescription().contains("recoveryCut=500"));
        Assert.assertTrue(issue(Issue.IssueId.SNAPSHOT_SYNC_FAILING).getDescription()
                .contains("3 log replication snapshot sync attempts"));
        Assert.assertTrue(issue(Issue.IssueId.SNAPSHOT_SYNC_FAILING).getDescription().contains("deadline expired"));

        lease(lease.setPhase(SnapshotSyncLeaseRecord.Phase.FAULTED).build());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertTrue(issue(Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED).getDescription().contains("cannot clean up"));

        // A completed snapshot sync and an open admission clear both.
        lease(lease.setPhase(SnapshotSyncLeaseRecord.Phase.READY).setConsecutiveAborts(0).build());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNull(issue(Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED));
        Assert.assertNull(issue(Issue.IssueId.SNAPSHOT_SYNC_FAILING));
    }

    @Test
    public void aNodeThatNoLongerLeadsStopsReportingWhatItSawAsTheLeader() {
        long now = System.currentTimeMillis();
        freezeToken(null);
        holdLease(now - Duration.ofMinutes(10).toMillis());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNotNull(frozenIssue());

        compactorLeaderServices.resolveLeaderIssues();
        Assert.assertNull(frozenIssue());
        Assert.assertTrue(HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR).isRuntimeHealthy());
    }

    /**
     * A sink upgraded to a later version may write a record this checkpointer does not fully know.
     * It still knows what must not be trimmed and until when; refusing the record would stop
     * compaction altogether, silently.
     */
    @Test
    public void aRecordOfALaterSchemaStillBoundsAndProtects() {
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        when(corfuStore.listTables(null)).thenReturn(Collections.singletonList(tableName));
        when(corfuRuntime.getAddressSpaceView()).thenReturn(mock(AddressSpaceView.class));
        long now = System.currentTimeMillis();
        SnapshotSyncLeaseRecord.Builder later = SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(2)
                .setProtectionHeld(true).setProtectedAfter(20);

        lease(later.setDeadlineMs(now + Duration.ofMinutes(10).toMillis()).build());
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, compactorLeaderServices.initCompactionCycle());

        lease(later.setDeadlineMs(now - SnapshotSyncLeaseStore.expiryGraceMs() - 1).build());
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS, compactorLeaderServices.initCompactionCycle());
    }

    @Test
    public void aLongLivedFreezeTokenIsReported() {
        long now = System.currentTimeMillis();
        freezeToken(RpcCommon.TokenMsg.newBuilder().setSequence(now - Duration.ofMinutes(30).toMillis()).build());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNull(frozenIssue());

        freezeToken(RpcCommon.TokenMsg.newBuilder().setSequence(now - Duration.ofMinutes(61).toMillis()).build());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNotNull(frozenIssue());
        Assert.assertTrue(frozenIssue().getDescription().contains("freeze token"));

        freezeToken(null);
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNull(frozenIssue());
    }

    @Test
    public void aFailureToReadTheFreezeStateLeavesTheReportedIssueAlone() {
        long now = System.currentTimeMillis();
        freezeToken(null);
        holdLease(now - Duration.ofMinutes(10).toMillis());
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNotNull(frozenIssue());

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenThrow(new IllegalStateException("store unavailable"));
        compactorLeaderServices.checkForProlongedFreeze(now);
        Assert.assertNotNull(frozenIssue());
    }

    private void holdLease(long deadlineMs) {
        SnapshotSyncLeaseRecord lease = SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1)
                .setProtectionHeld(true).setProtectedAfter(20).setGeneration(7)
                .setPhase(SnapshotSyncLeaseRecord.Phase.TRANSFERRING).setFailure("")
                .setDeadlineMs(deadlineMs).build();
        when(txn.getRecord(SnapshotSyncLeaseStore.TABLE_NAME, SnapshotSyncLeaseStore.DOMAIN))
                .thenReturn(new CorfuStoreEntry<>(SnapshotSyncLeaseStore.DOMAIN, lease, null));
    }

    private void lease(SnapshotSyncLeaseRecord lease) {
        when(txn.getRecord(SnapshotSyncLeaseStore.TABLE_NAME, SnapshotSyncLeaseStore.DOMAIN))
                .thenReturn(new CorfuStoreEntry<>(SnapshotSyncLeaseStore.DOMAIN, lease, null));
    }

    private Issue issue(Issue.IssueId id) {
        return HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR)
                .getRuntimeHealthIssues().stream()
                .filter(issue -> issue.getIssueId() == id)
                .findFirst().orElse(null);
    }

    private void freezeToken(RpcCommon.TokenMsg token) {
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN))
                .thenReturn(new CorfuStoreEntry<>(CompactorMetadataTables.FREEZE_TOKEN, token, null));
    }

    private Issue frozenIssue() {
        return HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR)
                .getRuntimeHealthIssues().stream()
                .filter(issue -> issue.getIssueId() == Issue.IssueId.CHECKPOINT_FROZEN)
                .findFirst().orElse(null);
    }

    @Test
    public void validateLivenessTest() {
        doNothing().when(livenessValidator).clearLivenessValidator();
        doNothing().when(livenessValidator).clearLivenessMap();

        //When there's no checkpoint activity
        when(livenessValidator.shouldChangeManagerStatus(any(Duration.class))).thenReturn(LivenessValidator.Status.FINISH);
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        compactorLeaderServices.validateLiveness();

        //When there's some checkpoint activity going on
        Set<CorfuStoreMetadata.TableName> set = new HashSet<>();
        set.add(tableName);
        when(txn.keySet(nullable(Table.class))).thenReturn(set);
        when(livenessValidator.isTableCheckpointActive(any(CorfuStoreMetadata.TableName.class), any(Duration.class))).thenReturn(false);
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        compactorLeaderServices.validateLiveness();

        ArgumentCaptor<CheckpointingStatus> captor = ArgumentCaptor.forClass(CheckpointingStatus.class);
        final int numTimesPutCalled = 3;
        verify(txn, times(numTimesPutCalled)).putRecord(any(), any(), captor.capture(), any());

        Assert.assertEquals(StatusType.COMPLETED, captor.getAllValues().get(0).getStatus());
        Assert.assertEquals(StatusType.FAILED, captor.getAllValues().get(1).getStatus());
        Assert.assertEquals(StatusType.FAILED, captor.getValue().getStatus());
    }

    private CheckpointingStatus runningCycle() {
        return CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).setCycleCount(4)
                .setTimeTaken(System.currentTimeMillis()).build();
    }

    private CheckpointingStatus table(StatusType status) {
        return CheckpointingStatus.newBuilder().setStatus(status).build();
    }

    private Issue stalledIssue() {
        return HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR)
                .getRuntimeHealthIssues().stream()
                .filter(issue -> issue.getIssueId() == Issue.IssueId.CHECKPOINT_STALLED)
                .findFirst().orElse(null);
    }

    /**
     * Tables that wait while nothing is being checkpointed and nothing completes: e.g. checkpointers
     * that were told checkpointing is frozen after the cycle had started.
     */
    @Test
    public void aCycleInWhichNothingMovesIsReportedAsStalled() {
        final long start = System.currentTimeMillis();
        when(txn.keySet(nullable(Table.class))).thenReturn(Collections.singleton(tableName));
        // Per pass: the manager record, then the status of the one table.
        when(corfuStoreEntry.getPayload())
                .thenReturn(runningCycle()).thenReturn(table(StatusType.IDLE))
                .thenReturn(runningCycle()).thenReturn(table(StatusType.IDLE))
                .thenReturn(runningCycle()).thenReturn(table(StatusType.IDLE))
                .thenReturn(runningCycle()).thenReturn(table(StatusType.COMPLETED));

        compactorLeaderServices.checkForStalledCheckpoints(start, false);
        Assert.assertNull("The cycle has only just been observed", stalledIssue());

        compactorLeaderServices.checkForStalledCheckpoints(start + Duration.ofMinutes(4).toMillis(), false);
        Assert.assertNull(stalledIssue());

        compactorLeaderServices.checkForStalledCheckpoints(start + Duration.ofMinutes(6).toMillis(), false);
        Assert.assertNotNull("Expected a CHECKPOINT_STALLED issue to be reported", stalledIssue());
        Assert.assertTrue(stalledIssue().getDescription().contains(tableName.getTableName()));

        compactorLeaderServices.checkForStalledCheckpoints(start + Duration.ofMinutes(7).toMillis(), false);
        Assert.assertNull("Issue should be resolved once no table remains IDLE", stalledIssue());
    }

    /**
     * Checkpointers work through the tables one after another, so a healthy cycle has waiting tables
     * for most of its duration, however long it takes. Time since the cycle started says nothing.
     */
    @Test
    public void aLongCycleThatKeepsMakingProgressIsNotStalled() {
        final long start = System.currentTimeMillis();
        Set<CorfuStoreMetadata.TableName> tables = new HashSet<>(Arrays.asList(tableName, tableName2));
        when(txn.keySet(nullable(Table.class))).thenReturn(tables);
        when(corfuStoreEntry.getPayload())
                // Both tables wait.
                .thenReturn(runningCycle()).thenReturn(table(StatusType.IDLE)).thenReturn(table(StatusType.IDLE))
                // Four minutes later still, but one is being checkpointed right now.
                .thenReturn(runningCycle()).thenReturn(table(StatusType.IDLE)).thenReturn(table(StatusType.IDLE))
                // Another four minutes later one is done.
                .thenReturn(runningCycle()).thenReturn(table(StatusType.COMPLETED)).thenReturn(table(StatusType.IDLE))
                // And four more minutes later the other still waits: twelve minutes into the cycle.
                .thenReturn(runningCycle()).thenReturn(table(StatusType.COMPLETED)).thenReturn(table(StatusType.IDLE));

        compactorLeaderServices.checkForStalledCheckpoints(start, false);
        compactorLeaderServices.checkForStalledCheckpoints(start + Duration.ofMinutes(4).toMillis(), true);
        compactorLeaderServices.checkForStalledCheckpoints(start + Duration.ofMinutes(8).toMillis(), false);
        compactorLeaderServices.checkForStalledCheckpoints(start + Duration.ofMinutes(12).toMillis(), false);

        Assert.assertNull(stalledIssue());
        Assert.assertTrue(HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR).isRuntimeHealthy());
    }

    @Test
    public void nothingIsStalledWhenNoCycleRuns() {
        when(corfuStoreEntry.getPayload()).thenReturn(table(StatusType.COMPLETED));
        compactorLeaderServices.checkForStalledCheckpoints(System.currentTimeMillis(), false);
        Assert.assertNull(stalledIssue());
    }

    @Test
    public void finishCompactionCycleTest() {
        Set<CorfuStoreMetadata.TableName> set = new HashSet<>(Arrays.asList(tableName, tableName2));
        when(txn.keySet(nullable(Table.class))).thenReturn(set);

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build());
        compactorLeaderServices.finishCompactionCycle();

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        compactorLeaderServices.finishCompactionCycle();

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build())
                .thenReturn(RpcCommon.TokenMsg.getDefaultInstance());
        compactorLeaderServices.finishCompactionCycle();

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build())
                .thenReturn(null)
                .thenReturn(RpcCommon.TokenMsg.getDefaultInstance());
        compactorLeaderServices.finishCompactionCycle();

        final int numTimePutInvoked = 4;
        ArgumentCaptor<CheckpointingStatus> putCaptor = ArgumentCaptor.forClass(CheckpointingStatus.class);
        verify(txn, times(numTimePutInvoked)).putRecord(any(), any(),
                putCaptor.capture(), any());

        ArgumentCaptor<StringKey> deleteCaptor = ArgumentCaptor.forClass(StringKey.class);
        verify(txn, times(2)).delete(anyString(), deleteCaptor.capture());

        Assert.assertEquals(StatusType.COMPLETED, putCaptor.getAllValues().get(0).getStatus());
        Assert.assertEquals(StatusType.FAILED, putCaptor.getAllValues().get(1).getStatus());
        Assert.assertEquals(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, deleteCaptor.getAllValues().get(0));
        Assert.assertEquals(CompactorMetadataTables.INSTANT_TIGGER, deleteCaptor.getAllValues().get(1));
    }
}
