package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;
import org.immutables.value.internal.$processor$.meta.$GsonMirrors;
import org.rocksdb.RocksDBException;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;

/**
 * This class contains the logic for the clear operation from stream listener to database.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class ClearOperation implements SMROperation {
    @Getter
    private final long timestamp;
    @Getter
    @NonNull
    private final UUID streamId;
    private final CountDownLatch isApplied = new CountDownLatch(1);
    @Getter
    private Exception exception;
    /**
     * {@inheritDoc}
     * <p>
     *     Performs clear database operation with given keys.
     * </p>
     */
    @Override
    public void applySMRMethod(@NonNull DatabaseHandler dbHandler) {
        try {
            dbHandler.clearAsync(streamId, timestamp).join();
        } catch (CancellationException e) {
            exception = e;
        } catch (CompletionException e) {
            exception = (Exception) e.getCause();
        } finally {
            isApplied.countDown();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Always returns empty list.
     * </p>
     */
    @Override
    public List<RemoteCorfuTableDatabaseEntry> getEntryBatch() {
        return new LinkedList<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RemoteCorfuTableSMRMethods getType() {
        return RemoteCorfuTableSMRMethods.CLEAR;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void waitUntilApply() throws InterruptedException {
        isApplied.await();
    }
}
