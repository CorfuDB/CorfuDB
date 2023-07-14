package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
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
