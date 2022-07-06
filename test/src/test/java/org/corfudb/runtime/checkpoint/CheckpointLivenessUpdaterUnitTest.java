package org.corfudb.runtime.checkpoint;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CheckpointLivenessUpdater;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doNothing;

@Slf4j
public class CheckpointLivenessUpdaterUnitTest {
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);

    private final static Duration INTERVAL = Duration.ofSeconds(15);
    private final static TableName tableName = TableName.newBuilder().setNamespace("TestNamespace").setTableName("TestTableName").build();

    private CheckpointLivenessUpdater livenessUpdater;

    @Before
    public void setup() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        livenessUpdater = new CheckpointLivenessUpdater(corfuStore);
        when(corfuStore.openTable(any(), any(), any(), any(), any(), any())).thenReturn(mock(Table.class));

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
    }

    @Test
    public void testUpdateLiveness() {
        ActiveCPStreamMsg activeCPStreamMsg = ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(0).build();
        when(corfuStoreEntry.getPayload()).thenReturn(activeCPStreamMsg);

        livenessUpdater.updateLiveness(tableName);
        try {
            TimeUnit.SECONDS.sleep(INTERVAL.getSeconds());
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }
        livenessUpdater.notifyOnSyncComplete();

        ArgumentCaptor<ActiveCPStreamMsg> captor = ArgumentCaptor.forClass(ActiveCPStreamMsg.class);
        verify(txn).putRecord(any(), any(), captor.capture(), any());
        Assert.assertEquals(1, captor.getValue().getSyncHeartbeat());
    }
}
