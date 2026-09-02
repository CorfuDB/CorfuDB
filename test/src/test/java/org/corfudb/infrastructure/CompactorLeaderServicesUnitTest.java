package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.health.Component;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
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

    @Test
    public void checkForStalledCheckpointsTest() {
        doNothing().when(livenessValidator).clearLivenessValidator();
        doNothing().when(livenessValidator).clearLivenessMap();
        // No tables are "active" (started), so validateLiveness() takes the isEmpty() branch
        // and falls through to the new stalled-checkpoint check.
        when(livenessValidator.shouldChangeManagerStatus(any(Duration.class))).thenReturn(LivenessValidator.Status.NONE);

        final long staleCycleStartTime = System.currentTimeMillis() - Duration.ofMinutes(6).toMillis();

        // 1st keySet() call is getAllActiveCheckpointsTables() (none active),
        // 2nd is checkForStalledCheckpoints() reading CheckpointStatusTable (one table, still IDLE).
        when(txn.keySet(nullable(Table.class)))
                .thenReturn(Collections.emptySet())
                .thenReturn(Collections.singleton(tableName));
        // 1st getRecord() is the CompactionManagerTable (cycle STARTED 6 minutes ago, i.e. beyond
        // the 5 minute STALLED_CHECKPOINT_THRESHOLD), 2nd is the table's own status (still IDLE).
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED)
                        .setTimeTaken(staleCycleStartTime).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build());

        compactorLeaderServices.validateLiveness();

        Issue stalledIssue = HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR)
                .getRuntimeHealthIssues().stream()
                .filter(issue -> issue.getIssueId() == Issue.IssueId.CHECKPOINT_STALLED)
                .findFirst().orElse(null);
        Assert.assertNotNull("Expected a CHECKPOINT_STALLED issue to be reported", stalledIssue);
    }

    @Test
    public void checkForStalledCheckpointsResolvesOnceTableStartsTest() {
        doNothing().when(livenessValidator).clearLivenessValidator();
        doNothing().when(livenessValidator).clearLivenessMap();
        when(livenessValidator.shouldChangeManagerStatus(any(Duration.class))).thenReturn(LivenessValidator.Status.NONE);

        // First pass: report the issue exactly as above.
        final long staleCycleStartTime = System.currentTimeMillis() - Duration.ofMinutes(6).toMillis();
        when(txn.keySet(nullable(Table.class)))
                .thenReturn(Collections.emptySet())
                .thenReturn(Collections.singleton(tableName))
                // Second pass: the table has moved on (no longer IDLE), nothing left stalled.
                .thenReturn(Collections.emptySet())
                .thenReturn(Collections.singleton(tableName));
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED)
                        .setTimeTaken(staleCycleStartTime).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED)
                        .setTimeTaken(staleCycleStartTime).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build());

        compactorLeaderServices.validateLiveness();
        Assert.assertFalse(HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR).isRuntimeHealthy());

        compactorLeaderServices.validateLiveness();
        Assert.assertTrue("Issue should be resolved once no table remains IDLE",
                HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR).isRuntimeHealthy());
    }

    @Test
    public void checkForStalledCheckpointsNotYetPastThresholdTest() {
        doNothing().when(livenessValidator).clearLivenessValidator();
        doNothing().when(livenessValidator).clearLivenessMap();
        when(livenessValidator.shouldChangeManagerStatus(any(Duration.class))).thenReturn(LivenessValidator.Status.NONE);

        // Cycle started only 1 minute ago: well within the 5 minute threshold, so even though a
        // table is IDLE, it should not (yet) be reported as stalled.
        final long recentCycleStartTime = System.currentTimeMillis() - Duration.ofMinutes(1).toMillis();
        when(txn.keySet(nullable(Table.class))).thenReturn(Collections.emptySet());
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED)
                        .setTimeTaken(recentCycleStartTime).build());

        compactorLeaderServices.validateLiveness();

        Assert.assertTrue(HealthMonitor.getHealthStatusSnapshot().get(Component.COMPACTOR).isRuntimeHealthy());
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
