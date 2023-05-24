package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@Slf4j
public class CompactorServiceUnitTest {
    private final ServerContext serverContext = mock(ServerContext.class);
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final InvokeCheckpointingJvm invokeCheckpointingJvm = mock(InvokeCheckpointingJvm.class);
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final TxnContext txn = mock(TxnContext.class);
    private CompactorService compactorServiceSpy;
    private final CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message> corfuStoreCompactionManagerEntry =
            (CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message>) mock(CorfuStoreEntry.class);
    private final CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message> corfuStoreCompactionControlsEntry =
            (CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message>) mock(CorfuStoreEntry.class);
    private final DynamicTriggerPolicy dynamicTriggerPolicy = mock(DynamicTriggerPolicy.class);
    private final CompactorLeaderServices leaderServices = mock(CompactorLeaderServices.class);

    private static final int SCHEDULER_INTERVAL = 1;
    private static final String NODE_ENDPOINT = "NodeEndpoint";
    private static final Duration TIMEOUT = Duration.ofSeconds(8);
    private static final String NODE_0 = "0";

    @Before
    public void setup() throws Exception {

        CorfuRuntime.CorfuRuntimeParameters mockParams = mock(CorfuRuntime.CorfuRuntimeParameters.class);
        when(corfuRuntime.getParameters()).thenReturn(mockParams);
        when(mockParams.getCheckpointTriggerFreqMillis()).thenReturn(1L);

        Map<String, Object> map = new HashMap<>();
        map.put("<port>", "port");
        when(serverContext.getManagementRuntimeParameters()).thenReturn(mockParams);
        when(serverContext.getLocalEndpoint()).thenReturn(NODE_ENDPOINT);
        when(serverContext.getServerConfig()).thenReturn(map);


        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME, CompactorMetadataTables.COMPACTION_MANAGER_KEY)).thenReturn(corfuStoreCompactionManagerEntry);
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.DISABLE_COMPACTION)).thenReturn(corfuStoreCompactionControlsEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());

        compactorServiceSpy = spy(new CompactorService(serverContext,
                Duration.ofSeconds(SCHEDULER_INTERVAL), invokeCheckpointingJvm, dynamicTriggerPolicy));
        doReturn(corfuRuntime).when(compactorServiceSpy).getNewCorfuRuntime();
        doReturn(leaderServices).when(compactorServiceSpy).getCompactorLeaderServices();
        doReturn(corfuStore).when(compactorServiceSpy).getCorfuStore();
        //Compaction enabled
        when((RpcCommon.TokenMsg) corfuStoreCompactionControlsEntry.getPayload()).thenReturn(null);
    }

    @Test
    public void runOrchestratorNonLeaderTest() {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        //isLeader becomes false
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT + NODE_0);

        when((CheckpointingStatus) corfuStoreCompactionManagerEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));

        verify(invokeCheckpointingJvm, timeout(TIMEOUT.toMillis())).shutdown();
        verify(invokeCheckpointingJvm, timeout(TIMEOUT.toMillis())).invokeCheckpointing();
    }

    @Test
    public void runOrchestratorNonLeaderOnExceptionTest() throws Exception {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        //isLeader becomes false
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT + NODE_0);

        when(compactorServiceSpy.getCompactorLeaderServices())
                .thenThrow(new Exception("CompactorLeaderServices not initialized")).thenReturn(leaderServices);
        when((CheckpointingStatus) corfuStoreCompactionManagerEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));

        verify(invokeCheckpointingJvm, timeout(TIMEOUT.toMillis())).shutdown();
        verify(invokeCheckpointingJvm, timeout(TIMEOUT.toMillis())).invokeCheckpointing();
    }

    @Test
    public void runOrchestratorLeaderTest() throws Exception {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        //isLeader becomes true
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT)
                .thenReturn(NODE_ENDPOINT)
                .thenReturn(NODE_ENDPOINT + NODE_0);

        when((CheckpointingStatus) corfuStoreCompactionManagerEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(dynamicTriggerPolicy.shouldTrigger(Matchers.anyLong(), Matchers.any(CorfuStore.class))).thenReturn(true).thenReturn(false);
        doNothing().when(leaderServices).validateLiveness();
        doReturn(CompactorLeaderServices.LeaderInitStatus.SUCCESS).when(leaderServices).initCompactionCycle();
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));

        verify(leaderServices, timeout(TIMEOUT.toMillis())).validateLiveness();
        verify(leaderServices, timeout(TIMEOUT.toMillis())).initCompactionCycle();
        verify(invokeCheckpointingJvm, timeout(TIMEOUT.toMillis())).shutdown();
    }

    @Test
    public void failOnAcquireManagerStatusTest() throws Exception {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT);

        when((CheckpointingStatus) corfuStoreCompactionManagerEntry.getPayload())
                .thenReturn(null);
        when(txn.commit()).thenThrow(new TransactionAbortedException(
                        new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)),
                        AbortCause.CONFLICT, new Throwable(), null));
        when(dynamicTriggerPolicy.shouldTrigger(Matchers.anyLong(), Matchers.any(CorfuStore.class))).thenReturn(true);
        doReturn(CompactorLeaderServices.LeaderInitStatus.SUCCESS).when(leaderServices).initCompactionCycle();

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));

        verify(leaderServices, after((int) TIMEOUT.toMillis()).never()).initCompactionCycle();
    }

    @Test
    public void runOrchestratorSchedulerTest() throws Exception {
        Layout mockLayout = mock(Layout.class);
        CompletableFuture invalidateLayoutFuture = mock(CompletableFuture.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(invalidateLayoutFuture);
        when(invalidateLayoutFuture.join())
                .thenThrow(new UnrecoverableCorfuInterruptedError(new InterruptedException()))
                .thenReturn(mockLayout);
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT);

        when((CheckpointingStatus) corfuStoreCompactionManagerEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(dynamicTriggerPolicy.shouldTrigger(Matchers.anyLong(), Matchers.any(CorfuStore.class))).thenReturn(true).thenReturn(false);
        doNothing().when(leaderServices).validateLiveness();
        doReturn(CompactorLeaderServices.LeaderInitStatus.SUCCESS).when(leaderServices).initCompactionCycle();
        when(invokeCheckpointingJvm.isRunning())
                .thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));

        verify(leaderServices, timeout(TIMEOUT.toMillis())).initCompactionCycle();
        verify(invokeCheckpointingJvm, timeout(TIMEOUT.toMillis())).shutdown();
    }

    @Test
    public void disableCompactionAfterStartedTest() {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        //isLeader becomes true
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT);

        //Disable compaction
        when((RpcCommon.TokenMsg) corfuStoreCompactionControlsEntry.getPayload())
                .thenReturn(RpcCommon.TokenMsg.getDefaultInstance());
        when((CheckpointingStatus) corfuStoreCompactionManagerEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));

        verify(leaderServices, after((int) TIMEOUT.toMillis()).never()).validateLiveness();
        verify(leaderServices, after((int) TIMEOUT.toMillis()).never()).initCompactionCycle();
        verify(leaderServices, atLeastOnce()).finishCompactionCycle();
    }
}
