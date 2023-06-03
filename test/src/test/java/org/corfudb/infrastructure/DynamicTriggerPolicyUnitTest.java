package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class DynamicTriggerPolicyUnitTest {

    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private static final long INTERVAL = 1000;

    private DynamicTriggerPolicy dynamicTriggerPolicy;
    private final CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message> corfuStoreEntry =
            (CorfuStoreEntry<? extends Message, ? extends Message, ? extends Message>) mock(CorfuStoreEntry.class);
    private final TxnContext txn = mock(TxnContext.class);

    @Before
    public void setup() {
        this.dynamicTriggerPolicy = new DynamicTriggerPolicy();

        when(corfuStore.txn(Matchers.any())).thenReturn(txn);
        when(txn.getRecord(Matchers.anyString(), Matchers.any(Message.class))).thenReturn(corfuStoreEntry);
        doNothing().when(txn).delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());
    }

    @Test
    public void testShouldTrigger() throws Exception {
        //this makes shouldForceTrigger and isCheckpointFrozen to return false
        when(corfuStoreEntry.getPayload()).thenReturn(null);

        dynamicTriggerPolicy.markCompactionCycleStart();
        assert !dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore));

        try {
            TimeUnit.MILLISECONDS.sleep(INTERVAL * 2);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }
        assert dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore));
    }

    @Test
    public void testShouldForceTrigger() throws Exception {
        when((RpcCommon.TokenMsg) corfuStoreEntry.getPayload()).thenReturn(null)
                .thenReturn(null)
                .thenReturn(RpcCommon.TokenMsg.getDefaultInstance());
        assert dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore));
    }

    @Test
    public void testDisableCompaction() throws Exception {
        when((RpcCommon.TokenMsg) corfuStoreEntry.getPayload()).thenReturn(RpcCommon.TokenMsg.getDefaultInstance()).thenReturn(null);
        assert !dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore));
    }

    @Test
    public void testCheckpointFrozen() throws Exception {
        when((RpcCommon.TokenMsg) corfuStoreEntry.getPayload()).thenReturn(null).thenReturn(RpcCommon.TokenMsg.newBuilder()
                .setSequence(System.currentTimeMillis()).build());
        assert !dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore));
    }

    @Test
    public void testCheckpointFrozenReturnFalse() throws Exception {
        final long patience = 3 * 60 * 60 * 1000; //freezeToken found but expired
        when((RpcCommon.TokenMsg) corfuStoreEntry.getPayload())
                .thenReturn(null)
                .thenReturn(RpcCommon.TokenMsg.newBuilder().setSequence(System.currentTimeMillis() - patience).build())
                .thenReturn(RpcCommon.TokenMsg.newBuilder().setSequence(System.currentTimeMillis()).build());
        assert dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore));
        verify(txn, times(1)).delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
    }
}
