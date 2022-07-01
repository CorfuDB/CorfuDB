package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class CompactorLeaderServicesUnitTest {
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);
    private final CorfuStore corfuStore = mock(CorfuStore.class);


    private final static String NAMESPACE = "TestNamespace";
    private final static String TABLE_NAME = "TestTableName";
    private final CorfuStoreMetadata.TableName tableName = CorfuStoreMetadata.TableName.newBuilder()
            .setNamespace(NAMESPACE).setTableName(TABLE_NAME).build();

    private CompactorLeaderServices compactorLeaderServices;
    private LivenessValidator livenessValidator = mock(LivenessValidator.class);

    @Before
    public void setup() throws Exception {
        MockedConstruction<CompactorMetadataTables> mockedMetadataTablesConstruction = mockConstruction(CompactorMetadataTables.class);
        MockedConstruction<LivenessValidator> mockedLivenessValidatorConstruction = mockConstruction(LivenessValidator.class);
        this.compactorLeaderServices = new CompactorLeaderServices(corfuRuntime, "NodeEndpoint", corfuStore);

        this.livenessValidator = mockedLivenessValidatorConstruction.constructed().get(0);
        when(mockedMetadataTablesConstruction.constructed().get(0).getCheckpointingStatusTable()).thenReturn(mock(Table.class));
        when(mockedMetadataTablesConstruction.constructed().get(0).getActiveCheckpointsTable()).thenReturn(mock(Table.class));

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        doNothing().when(txn).delete(anyString(), any(Message.class));
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());

        mockedMetadataTablesConstruction.close();
        mockedLivenessValidatorConstruction.close();
    }

    @Test
    public void initCompactionCycleTest() {
        mockStatic(DistributedCheckpointerHelper.class);
        when(DistributedCheckpointerHelper.isCheckpointFrozen(corfuStore)).thenReturn(true);
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, compactorLeaderServices.initCompactionCycle());

        when(DistributedCheckpointerHelper.isCheckpointFrozen(corfuStore)).thenReturn(false);

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED_ALL).build());
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, compactorLeaderServices.initCompactionCycle());

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        when(corfuStore.listTables(null)).thenReturn(Collections.singletonList(tableName));
        when(corfuRuntime.getAddressSpaceView()).thenReturn(mock(AddressSpaceView.class));
        doNothing().when(txn).putRecord(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
        Assert.assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS, compactorLeaderServices.initCompactionCycle());
    }

    @Test
    public void validateLivenessTest() {
        ArgumentCaptor<CheckpointingStatus> captor = ArgumentCaptor.forClass(CheckpointingStatus.class);
        doNothing().when(livenessValidator).clearLivenessValidator();
        doNothing().when(livenessValidator).clearLivenessMap();

        //When there's no client checkpoint activity
        when(livenessValidator.shouldChangeManagerStatus(any(Duration.class))).thenReturn(LivenessValidator.StatusToChange.STARTED_ALL);
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        compactorLeaderServices.validateLiveness();

        //When there's no checkpoint activity at all
        when(livenessValidator.shouldChangeManagerStatus(any(Duration.class))).thenReturn(LivenessValidator.StatusToChange.FINISH);
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED_ALL).build());
        compactorLeaderServices.validateLiveness();

        //When there's some checkpoint activity going on
        Set<CorfuStoreMetadata.TableName> set = new HashSet<>();
        set.add(tableName);
        when(txn.keySet(any(Table.class))).thenReturn(set);
        when(livenessValidator.isTableCheckpointActive(any(CorfuStoreMetadata.TableName.class), any(Duration.class))).thenReturn(false);
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        compactorLeaderServices.validateLiveness();

        final int numTimesPutCalled = 4;
        verify(txn, times(numTimesPutCalled)).putRecord(ArgumentMatchers.any(), ArgumentMatchers.any(),
                captor.capture(), ArgumentMatchers.any());

        Assert.assertEquals(StatusType.STARTED_ALL, captor.getAllValues().get(0).getStatus());
        Assert.assertEquals(StatusType.COMPLETED, captor.getAllValues().get(1).getStatus());
        Assert.assertEquals(StatusType.FAILED, captor.getAllValues().get(2).getStatus());
        Assert.assertEquals(StatusType.FAILED, captor.getValue().getStatus());
    }

    @Test
    public void finishCompactionCycleTest() {
        Set<CorfuStoreMetadata.TableName> set = new HashSet<>();
        set.add(tableName);
        set.add(mock(CorfuStoreMetadata.TableName.class));
        when(txn.keySet(any(Table.class))).thenReturn(set);
        ArgumentCaptor<CheckpointingStatus> captor = ArgumentCaptor.forClass(CheckpointingStatus.class);

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED_ALL).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build());
        compactorLeaderServices.finishCompactionCycle();

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED_ALL).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        compactorLeaderServices.finishCompactionCycle();

        verify(txn, times(2)).putRecord(ArgumentMatchers.any(), ArgumentMatchers.any(),
                captor.capture(), ArgumentMatchers.any());
        Assert.assertEquals(StatusType.COMPLETED, captor.getAllValues().get(0).getStatus());
        Assert.assertEquals(StatusType.FAILED, captor.getValue().getStatus());
    }
}
