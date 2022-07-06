package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class DynamicTriggerPolicyUnitTest {

    private final CorfuStore corfuStore = mock(CorfuStore.class);
    private final static long INTERVAL = 1000;

    private DynamicTriggerPolicy dynamicTriggerPolicy;
    private final CorfuStoreEntry corfuStoreEntry = mock(CorfuStoreEntry.class);;
    private final TxnContext txn = mock(TxnContext.class);

    @Before
    public void setup() {
        this.dynamicTriggerPolicy = new DynamicTriggerPolicy(corfuStore);

        when(corfuStore.txn(any())).thenReturn(txn);
        when(txn.getRecord(anyString(), any(Message.class))).thenReturn(corfuStoreEntry);
        when(txn.commit()).thenReturn(CorfuStoreMetadata.Timestamp.getDefaultInstance());
    }

    @Test
    public void testShouldTrigger() {
        when(corfuStoreEntry.getPayload()).thenReturn(null);

        dynamicTriggerPolicy.markCompactionCycleStart();
        assert !dynamicTriggerPolicy.shouldTrigger(INTERVAL);

        try {
          TimeUnit.MILLISECONDS.sleep(INTERVAL);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted: ", e);
        }
        assert dynamicTriggerPolicy.shouldTrigger(INTERVAL);
    }

    @Test
    public void testShouldForceTrigger() {
        when(corfuStoreEntry.getPayload()).thenReturn(RpcCommon.TokenMsg.getDefaultInstance());
        assert dynamicTriggerPolicy.shouldTrigger(INTERVAL);
    }
}
