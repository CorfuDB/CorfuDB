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
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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
    private static final int SLEEP_WAIT = 8;
    private static final String NODE_0 = "0";
    private static final String SLEEP_INTERRUPTED_EXCEPTION_MSG = "Sleep interrupted";


    @Before
    public void setup() throws Exception {

        Map<String, Object> map = new HashMap<>();
        map.put("<port>", "port");
        when(serverContext.getLocalEndpoint()).thenReturn(NODE_ENDPOINT);
        when(serverContext.getServerConfig()).thenReturn(map);

        CorfuRuntime.CorfuRuntimeParameters mockParams = mock(CorfuRuntime.CorfuRuntimeParameters.class);
        when(corfuRuntime.getParameters()).thenReturn(mockParams);
        when(mockParams.getCheckpointTriggerFreqMillis()).thenReturn(1L);

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME, CompactorMetadataTables.COMPACTION_MANAGER_KEY)).thenReturn(corfuStoreCompactionManagerEntry);
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.DISABLE_COMPACTION)).thenReturn(corfuStoreCompactionControlsEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());

        CompactorService compactorService = new CompactorService(serverContext,
                SingletonResource.withInitial(() -> corfuRuntime), invokeCheckpointingJvm, dynamicTriggerPolicy);
        compactorServiceSpy = spy(compactorService);
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
        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        verify(invokeCheckpointingJvm, times(1)).shutdown();
        verify(invokeCheckpointingJvm).invokeCheckpointing();
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
        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        verify(invokeCheckpointingJvm, times(1)).shutdown();
        verify(invokeCheckpointingJvm).invokeCheckpointing();
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
        when(dynamicTriggerPolicy.shouldTrigger(anyLong(), any(CorfuStore.class))).thenReturn(true).thenReturn(false);
        doNothing().when(leaderServices).validateLiveness();
        doReturn(CompactorLeaderServices.LeaderInitStatus.SUCCESS).when(leaderServices).initCompactionCycle();
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));
        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        verify(leaderServices).validateLiveness();
        verify(leaderServices).initCompactionCycle();
        verify(invokeCheckpointingJvm, times(1)).shutdown();
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
        when(dynamicTriggerPolicy.shouldTrigger(anyLong(), any(CorfuStore.class))).thenReturn(true);
        doReturn(CompactorLeaderServices.LeaderInitStatus.SUCCESS).when(leaderServices).initCompactionCycle();

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));
        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        verify(leaderServices, never()).initCompactionCycle();
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
        when(dynamicTriggerPolicy.shouldTrigger(anyLong(), any(CorfuStore.class))).thenReturn(true).thenReturn(false);
        doNothing().when(leaderServices).validateLiveness();
        doReturn(CompactorLeaderServices.LeaderInitStatus.SUCCESS).when(leaderServices).initCompactionCycle();
        when(invokeCheckpointingJvm.isRunning())
                .thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        compactorServiceSpy.start(Duration.ofSeconds(SCHEDULER_INTERVAL));
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(SLEEP_WAIT);
                break;
            } catch (InterruptedException e) {
                //sleep gets interrupted on throwable, hence the loop
                log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
            }
        }

        verify(leaderServices).initCompactionCycle();
        verify(invokeCheckpointingJvm, times(1)).shutdown();
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
        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn(SLEEP_INTERRUPTED_EXCEPTION_MSG, e);
        }

        verify(leaderServices, never()).validateLiveness();
        verify(leaderServices, never()).initCompactionCycle();
        verify(leaderServices, atLeastOnce()).finishCompactionCycle();
    }
}
