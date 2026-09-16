package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.SnapshotSyncLeaseStore;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.junit.Assert.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class DynamicTriggerPolicyUnitTest {

    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private static final long INTERVAL = 1000;

    private DynamicTriggerPolicy dynamicTriggerPolicy;
    private final Map<StringKey, RpcCommon.TokenMsg> controls = new HashMap<>();
    private final TxnContext txn = mock(TxnContext.class);

    @Before
    public void setup() {
        this.dynamicTriggerPolicy = new DynamicTriggerPolicy();

        when(corfuStore.txn(any())).thenReturn(txn);
        when(txn.getRecord(eq(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE), any(Message.class)))
                .thenAnswer(call -> new CorfuStoreEntry<>((StringKey) call.getArgument(1), controls.get(call.getArgument(1)), null));
        when(txn.getRecord(SnapshotSyncLeaseStore.TABLE_NAME, SnapshotSyncLeaseStore.DOMAIN))
                .thenReturn(new CorfuStoreEntry<>(SnapshotSyncLeaseStore.DOMAIN, null, null));
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME, CompactorMetadataTables.COMPACTION_MANAGER_KEY))
                .thenReturn(new CorfuStoreEntry<>(CompactorMetadataTables.COMPACTION_MANAGER_KEY, null, null));
        doAnswer(call -> controls.remove(call.getArgument(1))).when(txn)
                .delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());
    }

    @Test
    public void testShouldTrigger() throws Exception {

        dynamicTriggerPolicy.markCompactionCycleStart();
        assertFalse(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore)));

        try {
            TimeUnit.MILLISECONDS.sleep(INTERVAL * 2);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }
        assertTrue(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore)));
    }

    @Test
    public void testShouldForceTrigger() throws Exception {
        dynamicTriggerPolicy.markCompactionCycleStart();
        controls.put(CompactorMetadataTables.INSTANT_TIGGER, RpcCommon.TokenMsg.getDefaultInstance());
        assertTrue(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore)));
    }

    @Test
    public void testDisableCompaction() throws Exception {
        controls.put(CompactorMetadataTables.DISABLE_COMPACTION, RpcCommon.TokenMsg.getDefaultInstance());
        assertFalse(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore)));
    }

    @Test
    public void testCheckpointFrozen() throws Exception {
        controls.put(CompactorMetadataTables.FREEZE_TOKEN, RpcCommon.TokenMsg.newBuilder()
                .setSequence(System.currentTimeMillis()).build());
        assertFalse(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore)));
    }

    @Test
    public void testCheckpointFrozenReturnFalse() throws Exception {
        final long patience = 3 * 60 * 60 * 1000; //freezeToken found but expired
        controls.put(CompactorMetadataTables.INSTANT_TIGGER, RpcCommon.TokenMsg.getDefaultInstance());
        controls.put(CompactorMetadataTables.FREEZE_TOKEN,
                RpcCommon.TokenMsg.newBuilder().setSequence(System.currentTimeMillis() - patience).build());
        assertTrue(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore)));
        verify(txn, times(1)).delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
        assertFalse(controls.containsKey(CompactorMetadataTables.FREEZE_TOKEN));
    }

    @Test
    public void ownedProtectionBlocksForcedCheckpointEvenAfterItsDeadline() throws Exception {
        controls.put(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, RpcCommon.TokenMsg.getDefaultInstance());
        SnapshotSyncLeaseRecord lease = SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1)
                .setProtectionHeld(true).setProtectedAfter(20).setDeadlineMs(1).build();
        when(txn.getRecord(SnapshotSyncLeaseStore.TABLE_NAME, SnapshotSyncLeaseStore.DOMAIN))
                .thenReturn(new CorfuStoreEntry<>(SnapshotSyncLeaseStore.DOMAIN, lease, null));
        assertFalse(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, new DistributedCheckpointerHelper(corfuStore)));
    }

    @Test
    public void onlyAnExistingCheckpointWithASafeCutoffCanRunUnderProtection() throws Exception {
        controls.put(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, RpcCommon.TokenMsg.getDefaultInstance());
        SnapshotSyncLeaseRecord lease = SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1)
                .setProtectionHeld(true).setProtectedAfter(20).build();
        when(txn.getRecord(SnapshotSyncLeaseStore.TABLE_NAME, SnapshotSyncLeaseStore.DOMAIN))
                .thenReturn(new CorfuStoreEntry<>(SnapshotSyncLeaseStore.DOMAIN, lease, null));
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME, CompactorMetadataTables.COMPACTION_MANAGER_KEY))
                .thenReturn(new CorfuStoreEntry<>(CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                        CheckpointingStatus.newBuilder().setStatus(CheckpointingStatus.StatusType.STARTED).build(), null));
        DistributedCheckpointerHelper helper = new DistributedCheckpointerHelper(corfuStore);
        assertFalse(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, helper));
        controls.put(CompactorMetadataTables.MIN_CHECKPOINT, RpcCommon.TokenMsg.newBuilder().setSequence(20).build());
        assertTrue(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, helper));
        controls.put(CompactorMetadataTables.MIN_CHECKPOINT, RpcCommon.TokenMsg.newBuilder().setSequence(21).build());
        assertFalse(dynamicTriggerPolicy.shouldTrigger(INTERVAL, corfuStore, helper));
    }
}
