package org.corfudb.runtime.checkpoint;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.*;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.view.TableRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.verify;

@Slf4j
public class DistributedCheckpointerUnitTest {
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);
    private final CheckpointLivenessUpdater livenessUpdater = mock(CheckpointLivenessUpdater.class);

    private final static String NAMESPACE = "TestNamespace";
    private final static String TABLE_NAME = "TestTableName";
    private final static TableName tableName = TableName.newBuilder().setNamespace(NAMESPACE).setTableName(TABLE_NAME).build();
    private final static NetworkException networkException = new NetworkException("message", "clusterId");

    private DistributedCheckpointer distributedCheckpointerSpy;

    @Before
    public void setup() {
        when(corfuRuntime.getParameters()).thenReturn(
                CorfuRuntime.CorfuRuntimeParameters.builder().clientName("TestClient").build());
        DistributedCheckpointer distributedCheckpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .isClient(false)
                .persistedCacheRoot(Optional.empty())
                .cpRuntime(Optional.of(mock(CorfuRuntime.class)))
                .corfuRuntime(corfuRuntime)
                .build());
        this.distributedCheckpointerSpy = spy(distributedCheckpointer);

        doReturn(corfuStore).when(this.distributedCheckpointerSpy).getCorfuStore();
        CompactorMetadataTables compactorMetadataTables;
        try (MockedConstruction<CompactorMetadataTables> mockedMetadataTablesConstruction =
                     mockConstruction(CompactorMetadataTables.class)) {
            this.distributedCheckpointerSpy.openCompactorMetadataTables();
            compactorMetadataTables = mockedMetadataTablesConstruction.constructed().get(0);
        }

        when(compactorMetadataTables.getCheckpointingStatusTable()).thenReturn(mock(Table.class));
        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        doNothing().when(txn).delete(anyString(), any(TableName.class));
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
    }

    @Test
    public void unableToLockTableTest() {
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        when(txn.commit()).thenThrow(new WrongClusterException(null, null)).thenReturn(Timestamp.getDefaultInstance());
        assert !distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        when(txn.commit()).thenThrow(new TransactionAbortedException(
                new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)),
                AbortCause.CONFLICT, new Throwable(), null));
        assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());
    }

    @Test
    public void appendCheckpointTest() {
        ArgumentCaptor<CheckpointingStatus> captor = ArgumentCaptor.forClass(CheckpointingStatus.class);
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build());
        doReturn(livenessUpdater).when(distributedCheckpointerSpy).getLivenessUpdater();
        doNothing().when(livenessUpdater).updateLiveness(tableName);
        doNothing().when(livenessUpdater).notifyOnSyncComplete();

        try (MockedConstruction<MultiCheckpointWriter> mockedConstruction = mockConstruction(MultiCheckpointWriter.class,
                withSettings().useConstructor(), (mcw, context) -> {
                    doNothing().when(mcw).addMap(any(CorfuTable.class));
                    when(mcw.appendCheckpoints(eq(corfuRuntime), anyString(),
                            any(Optional.class)))
                            .thenReturn(new Token(0, 0));
                })) {
            //Happy path
            assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());
        }

        try (MockedConstruction<MultiCheckpointWriter> mockedConstruction = mockConstruction(MultiCheckpointWriter.class,
                withSettings().useConstructor(), (mcw, context) -> {
                    doNothing().when(mcw).addMap(any(CorfuTable.class));
                    when(mcw.appendCheckpoints(eq(corfuRuntime), anyString(),
                            any(Optional.class)))
                            .thenThrow(new IllegalStateException());
                })) {
            //When appendCheckpoint throws an exception
            assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());
        }

        try (MockedConstruction<MultiCheckpointWriter> mockedConstruction = mockConstruction(MultiCheckpointWriter.class,
                withSettings().useConstructor(), (mcw, context) -> {
                    doNothing().when(mcw).addMap(any(CorfuTable.class));
                    when(mcw.appendCheckpoints(eq(corfuRuntime), anyString(), any(Optional.class)))
                            .thenThrow(networkException)
                            .thenReturn(new Token(0, 0));
                })) {
            //When appendCheckpoint throws NetworkException and later succeeds
            assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());
        }

        final int numTimesMethodInvoked = 3;
        final int putInvokedPerMethodCall = 3;
        int i = 0;
        verify(txn, times(numTimesMethodInvoked * putInvokedPerMethodCall))
                .putRecord(any(), any(), captor.capture(), any());
        Assert.assertEquals(StatusType.COMPLETED, captor.getAllValues().get(getIndex(++i, putInvokedPerMethodCall)).getStatus());
        Assert.assertEquals(0, captor.getAllValues().get(getIndex(i, putInvokedPerMethodCall)).getEpoch());
        Assert.assertEquals(StatusType.FAILED, captor.getAllValues().get(getIndex(++i, putInvokedPerMethodCall)).getStatus());
        Assert.assertEquals(0, captor.getAllValues().get(getIndex(i, putInvokedPerMethodCall)).getEpoch());
        Assert.assertEquals(StatusType.COMPLETED, captor.getAllValues().get(getIndex(++i, putInvokedPerMethodCall)).getStatus());
        Assert.assertEquals(0, captor.getAllValues().get(getIndex(i, putInvokedPerMethodCall)).getEpoch());
    }

    private int getIndex(int i, int putInvokedPerMethodCall) {
        return i * putInvokedPerMethodCall - 1;
    }

    @Test
    public void unlockTableAfterCheckpointTest() {
        doReturn(livenessUpdater).when(distributedCheckpointerSpy).getLivenessUpdater();
        doNothing().when(livenessUpdater).updateLiveness(tableName);
        doNothing().when(livenessUpdater).notifyOnSyncComplete();

        MockedConstruction<MultiCheckpointWriter> mockedConstruction = mockConstruction(MultiCheckpointWriter.class,
                withSettings().useConstructor(), (mcw, context) -> {
                    doNothing().when(mcw).addMap(any(CorfuTable.class));
                    when(mcw.appendCheckpoints(eq(corfuRuntime), anyString(), any(Optional.class)))
                            .thenReturn(new Token(0, 0));
                }
        );
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        assert !distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        assert !distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance())
                .thenThrow(networkException)
                .thenReturn(Timestamp.getDefaultInstance());
        assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).setEpoch(1).build());
        //Fail on different epoch values
        assert !distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance())
                .thenThrow(new TransactionAbortedException(
                        new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)),
                        AbortCause.CONFLICT, new Throwable(), null));
        assert !distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        mockedConstruction.close();
    }

    @Test
    public void checkpointOpenedTablesTest() {
        TableRegistry tableRegistry = mock(TableRegistry.class);
        List<DistributedCheckpointer.CorfuTableNamePair> mockList = new ArrayList<>(
                Collections.singletonList(new DistributedCheckpointer.CorfuTableNamePair(tableName, mock(CorfuTable.class)))
        );
        when(corfuRuntime.getTableRegistry()).thenReturn(tableRegistry);
        when(tableRegistry.getAllOpenTablesForCheckpointing()).thenReturn(mockList);

        doReturn(true).when(distributedCheckpointerSpy).tryCheckpointTable(any(TableName.class), any(Function.class));
        Assert.assertEquals(1, distributedCheckpointerSpy.checkpointOpenedTables());

        doReturn(false).when(distributedCheckpointerSpy).tryCheckpointTable(any(TableName.class), any(Function.class));
        Assert.assertEquals(0, distributedCheckpointerSpy.checkpointOpenedTables());
    }
}
