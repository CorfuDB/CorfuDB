package org.corfudb.runtime.checkpoint;

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
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class DistributedCheckpointerUnitTest {

    private final static String NAMESPACE = "TestNamespace";
    private final static String TABLE_NAME = "TestTableName";
    private final TableName tableName = TableName.newBuilder().setNamespace(NAMESPACE).setTableName(TABLE_NAME).build();
    private DistributedCheckpointer distributedCheckpointer;

    @Mock
    CorfuRuntime corfuRuntime;
    @Mock
    CorfuStore corfuStore;
    @Mock
    CheckpointLivenessUpdater livenessUpdater;
    @Mock
    TxnContext txn;
    @Mock
    CorfuStoreEntry corfuStoreEntry;
    @Mock
    CompactorMetadataTables compactorMetadataTables;

    @Before
    public void init() {
        Mockito.when(corfuRuntime.getParameters()).thenReturn(
                CorfuRuntime.CorfuRuntimeParameters.builder().clientName("TestClient").build());
        distributedCheckpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .isClient(false)
                .persistedCacheRoot(Optional.empty())
                .cpRuntime(Optional.empty())
                .corfuRuntime(corfuRuntime)
                .build());
    }

    @Test
    public void tryLockTableToCheckpointTest() {
        Mockito.when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        Mockito.when(txn.getRecord(CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, tableName)).thenReturn(corfuStoreEntry);
        Mockito.doNothing().when(txn).putRecord(Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        Mockito.when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());

        Mockito.when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build());
        assert !distributedCheckpointer.tryLockTableToCheckpoint(corfuStore, compactorMetadataTables, tableName);

        Mockito.when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).build());
        assert distributedCheckpointer.tryLockTableToCheckpoint(corfuStore, compactorMetadataTables, tableName);

        Mockito.when(txn.commit()).thenThrow(new NetworkException("message", "clusterId")).thenReturn(Timestamp.getDefaultInstance());
        assert distributedCheckpointer.tryLockTableToCheckpoint(corfuStore, compactorMetadataTables, tableName);

        Mockito.when(txn.commit()).thenThrow(new WrongClusterException(null, null)).thenReturn(Timestamp.getDefaultInstance());
        assert !distributedCheckpointer.tryLockTableToCheckpoint(corfuStore, compactorMetadataTables, tableName);

        Mockito.when(txn.commit()).thenThrow(new TransactionAbortedException(
                new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)),
                AbortCause.CONFLICT, new Throwable(), null));
        assert !distributedCheckpointer.tryLockTableToCheckpoint(corfuStore, compactorMetadataTables, tableName);
    }

    @Test
    public void unlockTableAfterCheckpointTest() {
        Mockito.when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        Mockito.when(txn.getRecord(CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME,
                tableName)).thenReturn(corfuStoreEntry);
        Mockito.when(txn.getRecord(CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                CompactorMetadataTables.COMPACTION_MANAGER_KEY)).thenReturn(corfuStoreEntry);
        Mockito.doNothing().when(txn).putRecord(Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        Mockito.doNothing().when(txn).delete(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, tableName);
        Mockito.when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());

        CheckpointingStatus completedStatus = CheckpointingStatus.newBuilder().setStatus(StatusType.COMPLETED).build();
        CheckpointingStatus failedStatus = CheckpointingStatus.newBuilder().setStatus(StatusType.FAILED).build();
        CheckpointingStatus startedStatus = CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).build();

        Mockito.when(corfuStoreEntry.getPayload())
                .thenReturn(startedStatus).thenReturn(startedStatus);
        assert distributedCheckpointer.unlockTableAfterCheckpoint(corfuStore, compactorMetadataTables, tableName,
                completedStatus);

        Mockito.when(txn.commit()).thenThrow(new NetworkException("message", "clusterId")).thenReturn(Timestamp.getDefaultInstance());
        assert distributedCheckpointer.unlockTableAfterCheckpoint(corfuStore, compactorMetadataTables, tableName,
                completedStatus);

        Mockito.when(corfuStoreEntry.getPayload())
                .thenReturn(CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED_ALL).build())
                .thenReturn(failedStatus);
        assert !distributedCheckpointer.unlockTableAfterCheckpoint(corfuStore, compactorMetadataTables, tableName,
                completedStatus);

        Mockito.when(corfuStoreEntry.getPayload()).thenReturn(failedStatus);
        assert !distributedCheckpointer.unlockTableAfterCheckpoint(corfuStore, compactorMetadataTables, tableName,
                completedStatus);

        Mockito.when(txn.commit()).thenThrow(new TransactionAbortedException(
                new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)),
                AbortCause.CONFLICT, new Throwable(), null));
        Mockito.when(corfuStoreEntry.getPayload())
                .thenReturn(startedStatus).thenReturn(startedStatus)
                .thenReturn(failedStatus);
        assert !distributedCheckpointer.unlockTableAfterCheckpoint(corfuStore, compactorMetadataTables, tableName,
                completedStatus);
    }

    @Test
    public void tryCheckpointTableTest() {
        DistributedCheckpointer distributedCheckpointerSpy = Mockito.spy(distributedCheckpointer);
        MultiCheckpointWriter<CorfuTable> mcw = Mockito.mock(MultiCheckpointWriter.class);
        Mockito.doNothing().when(mcw).addMap(Matchers.isA(CorfuTable.class));
        Mockito.doReturn(corfuStore).when(distributedCheckpointerSpy).getCorfuStore();
        Mockito.doReturn(livenessUpdater).when(distributedCheckpointerSpy).getLivenessUpdater();
        Mockito.doNothing().when(livenessUpdater).updateLiveness(tableName);
        Mockito.doNothing().when(livenessUpdater).notifyOnSyncComplete();

        Mockito.doReturn(true).when(distributedCheckpointerSpy).tryLockTableToCheckpoint(Matchers.anyObject(),
                Matchers.anyObject(), Matchers.anyObject());
        Mockito.when(mcw.appendCheckpoints(Matchers.eq(corfuRuntime), Matchers.anyString(), Matchers.isA(Optional.class)))
                .thenReturn(new Token(0, 0));
        Mockito.doReturn(true).when(distributedCheckpointerSpy).unlockTableAfterCheckpoint(Matchers.anyObject(),
                Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        Mockito.when(mcw.appendCheckpoints(Matchers.eq(corfuRuntime), Matchers.anyString(), Matchers.isA(Optional.class)))
                .thenThrow(new IllegalStateException());
        assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        Mockito.doReturn(false).when(distributedCheckpointerSpy).unlockTableAfterCheckpoint(Matchers.anyObject(),
                Matchers.anyObject(), Matchers.anyObject(), Matchers.anyObject());
        assert !distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());

        Mockito.doReturn(false).when(distributedCheckpointerSpy).tryLockTableToCheckpoint(Matchers.anyObject(),
                Matchers.anyObject(), Matchers.anyObject());
        assert distributedCheckpointerSpy.tryCheckpointTable(tableName, t -> new CorfuTable<>());
    }

    @Test
    public void checkpointOpenedTablesTest() {
        DistributedCheckpointer distributedCheckpointerSpy = Mockito.spy(distributedCheckpointer);
        TableRegistry tableRegistry = Mockito.mock(TableRegistry.class);
        List<DistributedCheckpointer.CorfuTableNamePair> mockList = new ArrayList<>(
                Arrays.asList(new DistributedCheckpointer.CorfuTableNamePair(tableName, Mockito.mock(CorfuTable.class)))
        );
        Mockito.when(corfuRuntime.getTableRegistry()).thenReturn(tableRegistry);
        Mockito.when(tableRegistry.getAllOpenTablesForCheckpointing()).thenReturn(mockList);

        Mockito.doReturn(true).when(distributedCheckpointerSpy).tryCheckpointTable(Matchers.isA(TableName.class),
                Matchers.isA(Function.class));
        Assert.assertEquals(1, distributedCheckpointerSpy.checkpointOpenedTables());

        Mockito.doReturn(false).when(distributedCheckpointerSpy).tryCheckpointTable(Matchers.isA(TableName.class),
                Matchers.isA(Function.class));
        Assert.assertEquals(0, distributedCheckpointerSpy.checkpointOpenedTables());
    }
}
