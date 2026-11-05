package org.corfudb.runtime;

import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class SnapshotSyncLeaseStoreTest {
    @Test
    void registrationFailurePreventsUsingAnUnprotectedDomain() throws Exception {
        CorfuStore storage = mock(CorfuStore.class);
        when(storage.openTable(any(), any(), any(), any(), any(), any()))
                .thenThrow(new IllegalStateException("schema unavailable"));
        assertThrows(IllegalStateException.class, () -> new SnapshotSyncLeaseStore(storage));
    }

    @Test
    void transactionRetryIsBoundedAndInterruptionStopsIt() {
        CorfuStore storage = mock(CorfuStore.class);
        TxnContext txn = mock(TxnContext.class);
        when(storage.txn(anyString())).thenReturn(txn);
        SnapshotSyncLeaseStore leases = new SnapshotSyncLeaseStore(storage);
        TransactionAbortedException conflict = mock(TransactionAbortedException.class);
        when(txn.commit()).thenThrow(conflict);
        assertThrows(TransactionAbortedException.class, () -> leases.update((context, state) -> state));
        verify(txn, times(3)).commit();
        clearInvocations(txn);
        Thread.currentThread().interrupt();
        try {
            assertThrows(TransactionAbortedException.class, () -> leases.update((context, state) -> state));
            verify(txn, times(1)).commit();
        } finally { Thread.interrupted(); }
    }
}
