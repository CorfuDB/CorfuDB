package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class CheckpointLivenessUpdaterUnitTest {
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry<Message, Message, Message> corfuStoreEntry =
            (CorfuStoreEntry<Message, Message, Message>) mock(CorfuStoreEntry.class);

    private static final Duration INTERVAL = Duration.ofSeconds(15);
    private static final TableName tableName = TableName.newBuilder().setNamespace("TestNamespace").setTableName("TestTableName").build();

    private CheckpointLivenessUpdater livenessUpdater;

    @Before
    public void setup() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        when(corfuStore.openTable(any(), any(), any(), any(), any(), any())).thenReturn(mock(Table.class));

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).putRecord(any(), any(), any(), any());
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
        livenessUpdater = new CheckpointLivenessUpdater(corfuStore);
    }

    @Test
    public void testUpdateLiveness() {
        ActiveCPStreamMsg activeCPStreamMsg = ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(0).build();
        when((ActiveCPStreamMsg) corfuStoreEntry.getPayload()).thenReturn(activeCPStreamMsg);

        livenessUpdater.start();
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

    @Test
    public void testUpdateLivenessRaceCondition() {
        ActiveCPStreamMsg activeCPStreamMsg = ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(0).build();
        when((ActiveCPStreamMsg) corfuStoreEntry.getPayload()).thenReturn(activeCPStreamMsg);

        livenessUpdater.updateLiveness(tableName);
        livenessUpdater.updateHeartbeat();
        livenessUpdater.notifyOnSyncComplete();

        livenessUpdater.updateHeartbeat();

        ArgumentCaptor<ActiveCPStreamMsg> captor = ArgumentCaptor.forClass(ActiveCPStreamMsg.class);
        verify(txn).putRecord(any(), any(), captor.capture(), any());
        Assert.assertEquals(1, captor.getValue().getSyncHeartbeat());
    }

    @Test
    public void testSchedulerOnException() {
        ActiveCPStreamMsg activeCPStreamMsg = ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(0).build();
        when((ActiveCPStreamMsg) corfuStoreEntry.getPayload())
                .thenReturn(null)
                .thenReturn(activeCPStreamMsg);
        livenessUpdater.start();

        livenessUpdater.updateLiveness(tableName);
        try {
            TimeUnit.SECONDS.sleep(INTERVAL.getSeconds()*2);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }
        livenessUpdater.notifyOnSyncComplete();

        ArgumentCaptor<ActiveCPStreamMsg> captor = ArgumentCaptor.forClass(ActiveCPStreamMsg.class);
        verify(txn).putRecord(any(), any(), captor.capture(), any());
        Assert.assertEquals(1, captor.getValue().getSyncHeartbeat());
    }
}
