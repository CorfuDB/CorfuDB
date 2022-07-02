package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@Slf4j
public class CompactorServiceUnitTest {
    private final ServerContext serverContext = mock(ServerContext.class);
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final InvokeCheckpointingJvm invokeCheckpointingJvm = mock(InvokeCheckpointingJvm.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);


    private final static int SCHEDULER_INTERVAL = 1;
    private final static String NODE_ENDPOINT = "NodeEndpoint";
    private final static int SLEEP_WAIT = 8;

    private CompactorLeaderServices leaderServices;
    private CompactionTriggerPolicy compactionTriggerPolicy;

    @Before
    public void setup() {
        CompactorService compactorService = new CompactorService(serverContext,
                SingletonResource.withInitial(() -> corfuRuntime), invokeCheckpointingJvm);

        Map<String, Object> map = new HashMap<>();
        map.put("<port>", "port");
        when(serverContext.getLocalEndpoint()).thenReturn(NODE_ENDPOINT);
        when(serverContext.getServerConfig()).thenReturn(map);

        CorfuRuntime.CorfuRuntimeParameters mockParams = mock(CorfuRuntime.CorfuRuntimeParameters.class);
        when(corfuRuntime.getParameters()).thenReturn(mockParams);
        when(mockParams.getCheckpointTriggerFreqMillis()).thenReturn(1L);

        MockedConstruction<CorfuStore> mockedCorfuStoreConstruction = mockConstruction(CorfuStore.class);
        MockedConstruction<CompactorLeaderServices> mockedLeaderServices = mockConstruction(CompactorLeaderServices.class);
        MockedConstruction<DynamicTriggerPolicy> mockedTriggerPolicyConstruction = mockConstruction(DynamicTriggerPolicy.class);
        compactorService.start(Duration.ofSeconds(SCHEDULER_INTERVAL));
        CorfuStore corfuStore = mockedCorfuStoreConstruction.constructed().get(0);
        this.compactionTriggerPolicy = mockedTriggerPolicyConstruction.constructed().get(0);
        this.leaderServices = mockedLeaderServices.constructed().get(0);

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());

        mockedLeaderServices.close();
        mockedCorfuStoreConstruction.close();
        mockedTriggerPolicyConstruction.close();
    }

    @Test
    public void runOrchestratorNonLeaderTest() {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        //isLeader becomes false
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT + "0");

        when(corfuStoreEntry.getPayload()).thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }

        verify(invokeCheckpointingJvm, times(2)).shutdown();
        verify(invokeCheckpointingJvm).invokeCheckpointing();
    }

    @Test
    public void runOrchestratorLeaderTest() {
        Layout mockLayout = mock(Layout.class);
        when(corfuRuntime.invalidateLayout()).thenReturn(CompletableFuture.completedFuture(mockLayout));
        //isLeader becomes true
        when(mockLayout.getPrimarySequencer()).thenReturn(NODE_ENDPOINT)
                .thenReturn(NODE_ENDPOINT)
                .thenReturn(NODE_ENDPOINT + "0");

        when(corfuStoreEntry.getPayload()).thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(compactionTriggerPolicy.shouldTrigger(anyLong())).thenReturn(true).thenReturn(false);
        doNothing().when(leaderServices).validateLiveness();
        doReturn(CompactorLeaderServices.LeaderInitStatus.SUCCESS).when(leaderServices).initCompactionCycle();
        when(invokeCheckpointingJvm.isRunning()).thenReturn(false).thenReturn(true);
        when(invokeCheckpointingJvm.isInvoked()).thenReturn(false).thenReturn(true);

        try {
            TimeUnit.SECONDS.sleep(SLEEP_WAIT);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }

        verify(leaderServices).validateLiveness();
        verify(leaderServices).initCompactionCycle();
        verify(invokeCheckpointingJvm, times(2)).shutdown();
    }
}
