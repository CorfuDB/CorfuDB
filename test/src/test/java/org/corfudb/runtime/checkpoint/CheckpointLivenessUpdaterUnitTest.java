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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class CheckpointLivenessUpdaterUnitTest {
    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final TxnContext txn = mock(TxnContext.class);
    private final CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message> corfuStoreEntry =
            (CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message>) mock(CorfuStoreEntry.class);

    private static final Duration INTERVAL = Duration.ofSeconds(15);
    private static final int LONG_INTERVAL = 60;
    private static final int NUM_REPETITIONS = 5;
    private static final TableName tableName = TableName.newBuilder().setNamespace("TestNamespace").setTableName("TestTableName").build();

    private CheckpointLivenessUpdater livenessUpdater;

    @BeforeEach
    @Before
    public void setup() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        livenessUpdater = new CheckpointLivenessUpdater(corfuStore);
        when(corfuStore.openTable(Matchers.any(), Matchers.any(), Matchers.any(), Matchers.any(), Matchers.any(), Matchers.any())).thenReturn(mock(Table.class));

        when(corfuStore.txn(CORFU_SYSTEM_NAMESPACE)).thenReturn(txn);
        when(txn.getRecord(Matchers.anyString(), Matchers.any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).putRecord(Matchers.any(), Matchers.any(), Matchers.any(), Matchers.any());
        when(txn.commit()).thenReturn(Timestamp.getDefaultInstance());
    }

    @Test
    public void testUpdateLiveness() {
        ActiveCPStreamMsg activeCPStreamMsg = ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(0).build();
        when((ActiveCPStreamMsg) corfuStoreEntry.getPayload()).thenReturn(activeCPStreamMsg);

        livenessUpdater.updateLiveness(tableName);
        try {
            TimeUnit.SECONDS.sleep(INTERVAL.getSeconds());
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }
        livenessUpdater.notifyOnSyncComplete();

        ArgumentCaptor<ActiveCPStreamMsg> captor = ArgumentCaptor.forClass(ActiveCPStreamMsg.class);
        verify(txn).putRecord(Matchers.any(), Matchers.any(), captor.capture(), Matchers.any());
        Assert.assertEquals(1, captor.getValue().getSyncHeartbeat());
    }

    @RepeatedTest(NUM_REPETITIONS)
    public void testUpdateLivenessRaceCondition() {
        ActiveCPStreamMsg activeCPStreamMsg = ActiveCPStreamMsg.newBuilder().setSyncHeartbeat(0).build();
        when((ActiveCPStreamMsg) corfuStoreEntry.getPayload()).thenReturn(activeCPStreamMsg);

        //This block helps with possibly triggering a race condition
        long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(LONG_INTERVAL, TimeUnit.SECONDS);
        boolean clear = true;
        while (System.nanoTime() < endTime) {
            if (clear) {
                livenessUpdater.notifyOnSyncComplete();
            } else {
                livenessUpdater.updateLiveness(tableName);
            }
            clear = !clear;
        }

        //ensures atLeastOnce invocation of putRecord if there were no exceptions in updateHeartbeat method
        livenessUpdater.updateLiveness(tableName);
        try {
            TimeUnit.SECONDS.sleep(INTERVAL.getSeconds());
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }
        livenessUpdater.notifyOnSyncComplete();

        ArgumentCaptor<ActiveCPStreamMsg> captor = ArgumentCaptor.forClass(ActiveCPStreamMsg.class);
        verify(txn, atLeastOnce()).putRecord(Matchers.any(), Matchers.any(), captor.capture(), Matchers.any());
    }
}
