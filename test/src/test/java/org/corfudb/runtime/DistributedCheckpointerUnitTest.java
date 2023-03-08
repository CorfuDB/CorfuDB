package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@Slf4j
public class DistributedCheckpointerUnitTest {
    private final CorfuRuntime corfuRuntime = mock(CorfuRuntime.class);
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message> corfuStoreEntry =
            (CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message>) mock(CorfuStoreEntry.class);
    private final CheckpointWriter<ICorfuTable<?,?>> cpw = (CheckpointWriter<ICorfuTable<?,?>>) mock(CheckpointWriter.class);

    private static final String NAMESPACE = "TestNamespace";
    private static final String TABLE_NAME = "TestTableName";
    private static final TableName tableName = TableName.newBuilder().setNamespace(NAMESPACE).setTableName(TABLE_NAME).build();
    private static final NetworkException networkException = new NetworkException("message", "clusterId");

    private DistributedCheckpointer distributedCheckpointer;

    @Before
    public void setup() {
        when(corfuRuntime.getParameters()).thenReturn(
                CorfuRuntime.CorfuRuntimeParameters.builder().clientName("TestClient").build());
        CompactorMetadataTables compactorMetadataTables = null;
        try {
            compactorMetadataTables = new CompactorMetadataTables(corfuStore);
        } catch (Exception e) {
            log.warn("Caught exception while opening MetadataTables: ", e);
        }
        distributedCheckpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .isClient(false)
                .persistedCacheRoot(Optional.empty())
                .cpRuntime(Optional.of(mock(CorfuRuntime.class)))
                .corfuRuntime(corfuRuntime)
                .build(), corfuStore, compactorMetadataTables);

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        doNothing().when(txn).delete(anyString(), any(TableName.class));
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
        when(cpw.getCorfuTable()).thenReturn(mock(PersistentCorfuTable.class));
    }

    @Test
    public void unableToLockTableTest() {
        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build());
        assert !distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build());
        assert distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when(txn.commit()).thenThrow(new WrongClusterException(null, null)).thenReturn(Timestamp.getDefaultInstance());
        assert !distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when(txn.commit()).thenThrow(new TransactionAbortedException(
                new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)),
                AbortCause.CONFLICT, new Throwable(), null));
        assert distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

    }

    @Test
    public void appendCheckpointTest() {
        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build());

        //Happy path
        when(cpw.appendCheckpoint(any(Optional.class))).thenReturn(new Token(0, 0));
        assert distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build());
        //When appendCheckpoint throws an exception
        CheckpointWriter<ICorfuTable<?,?>> cpwThrowException = mock(CheckpointWriter.class);
        when(cpwThrowException.appendCheckpoint(any(Optional.class))).thenThrow(new IllegalStateException());
        assert distributedCheckpointer.tryCheckpointTable(tableName, t -> cpwThrowException);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build());
        //When appendCheckpoint throws NetworkException and later succeeds
        when(cpw.appendCheckpoint(any(Optional.class)))
                .thenThrow(networkException)
                .thenReturn(new Token(0, 0));
        assert distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        ArgumentCaptor<CheckpointingStatus> captor = ArgumentCaptor.forClass(CheckpointingStatus.class);
        final int numTimesMethodInvoked = 3;
        final int putInvokedPerMethodCall = 3;
        int i = 0;
        verify(txn, times(numTimesMethodInvoked * putInvokedPerMethodCall))
                .putRecord(any(), any(), captor.capture(), any());
        Assert.assertEquals(StatusType.COMPLETED, captor.getAllValues().get(getIndex(++i, putInvokedPerMethodCall)).getStatus());
        Assert.assertEquals(0, captor.getAllValues().get(getIndex(i, putInvokedPerMethodCall)).getCycleCount());
        Assert.assertEquals(StatusType.FAILED, captor.getAllValues().get(getIndex(++i, putInvokedPerMethodCall)).getStatus());
        Assert.assertEquals(0, captor.getAllValues().get(getIndex(i, putInvokedPerMethodCall)).getCycleCount());
        Assert.assertEquals(StatusType.COMPLETED, captor.getAllValues().get(getIndex(++i, putInvokedPerMethodCall)).getStatus());
        Assert.assertEquals(0, captor.getAllValues().get(getIndex(i, putInvokedPerMethodCall)).getCycleCount());
    }

    private int getIndex(int i, int putInvokedPerMethodCall) {
        return i * putInvokedPerMethodCall - 1;
    }

    @Test
    public void unlockTableAfterCheckpointTest() {
        when(cpw.appendCheckpoint(any(Optional.class))).thenReturn(new Token(0, 0));
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        assert !distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build());
        assert !distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance())
                .thenThrow(networkException)
                .thenReturn(Timestamp.getDefaultInstance());
        assert distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).setCycleCount(1).build());
        //Fail on different cycleCount values
        assert !distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance()) //commit in tryLockTableToCheckpoint()
                .thenThrow(new TransactionAbortedException(
                        new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)),
                        AbortCause.CONFLICT, new Throwable(), null))
                .thenReturn(Timestamp.getDefaultInstance());
        assert distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);

        when((CheckpointingStatus) corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance())
                .thenThrow(new TransactionAbortedException(
                        new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)),
                        AbortCause.CONFLICT, new Throwable(), null));
        assert !distributedCheckpointer.tryCheckpointTable(tableName, t -> cpw);
    }
}
