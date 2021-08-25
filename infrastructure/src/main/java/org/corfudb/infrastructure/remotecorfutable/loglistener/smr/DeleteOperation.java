package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import com.google.protobuf.ByteString;
import lombok.NonNull;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;
import org.rocksdb.RocksDBException;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * This class contains the logic for the delete operation from stream listener to database.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
public class DeleteOperation implements SMROperation {
    private final List<ByteString> keys;
    private final long timestamp;
    private final UUID streamId;

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
    public void applySMRMethod(@NonNull DatabaseHandler dbHandler) throws RocksDBException {
        //optimization in case of only one key being deleted
        if (keys.size() == 1) {
            //single delete case - take first key in the list and delete it
            RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(keys.get(0), timestamp);
            dbHandler.update(key, ByteString.EMPTY, streamId);
        } else {
            dbHandler.updateAll(getEntryBatch(), streamId);
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
}
