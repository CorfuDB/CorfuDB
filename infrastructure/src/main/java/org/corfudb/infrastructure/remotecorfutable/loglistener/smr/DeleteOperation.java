package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import com.google.protobuf.ByteString;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;
import org.rocksdb.RocksDBException;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;

/**
 * This class contains the logic for the delete operation from stream listener to database.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
@EqualsAndHashCode
public class DeleteOperation implements SMROperation {
    private final List<ByteString> keys;
    @Getter
    private final long timestamp;
    @Getter
    private final UUID streamId;
    private final CountDownLatch isApplied = new CountDownLatch(1);
    @Getter
    private Exception exception;

    public DeleteOperation(@NonNull List<ByteString> keys, long timestamp, @NonNull UUID streamId) {
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("Cannot have delete operation with no keys to delete");
        }
        this.keys = keys;
        this.timestamp = timestamp;
        this.streamId = streamId;
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Performs delete database operation with given keys.
     * </p>
     */
    @Override
    public void applySMRMethod(@NonNull DatabaseHandler dbHandler) {
        try {
            //optimization in case of only one key being deleted
            if (keys.size() == 1) {
                //single delete case - take first key in the list and delete it
                RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(keys.get(0), timestamp);
                dbHandler.updateAsync(key, ByteString.EMPTY, streamId).join();
            } else {
                dbHandler.updateAllAsync(getEntryBatch(), streamId).join();
            }
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
     *     Returns the given keys with empty values.
     * </p>
     */
    @Override
    public List<RemoteCorfuTableDatabaseEntry> getEntryBatch() {
        List<RemoteCorfuTableDatabaseEntry> entries = new LinkedList<>();
        for (ByteString unencodedKey : keys) {
            RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(unencodedKey, timestamp);
            RemoteCorfuTableDatabaseEntry entry = new RemoteCorfuTableDatabaseEntry(key, ByteString.EMPTY);
            entries.add(entry);
        }
        return entries;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RemoteCorfuTableSMRMethods getType() {
        return RemoteCorfuTableSMRMethods.DELETE;
    }

    @Override
    public void waitUntilApply() throws InterruptedException {
        isApplied.await();
    }
}
