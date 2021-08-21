package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
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
 * This class contains the logic for the update operation from stream listener to database.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
@AllArgsConstructor
public class UpdateOperation implements SMROperation {
    @NonNull
    private final ByteString[] args;

    private final long timestamp;
    @NonNull
    private final UUID streamId;

    /**
     * {@inheritDoc}
     * <p>
     *     Performs update database operation with given key-value pairs.
     * </p>
     */
    @Override
    public void applySMRMethod(@NonNull DatabaseHandler dbHandler) throws RocksDBException {
        int size = args.length;
        if (size % 2 == 1) {
            throw new IllegalArgumentException("args must consist of key-value pairs");
        } else if (size == 2) {
            //single update case
            RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(args[0], timestamp);
            dbHandler.update(key, args[1], streamId);
        } else {
            dbHandler.updateAll(getEntryBatch(), streamId);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Returns the key-value pairs to be inserted to the database.
     * </p>
     */
    @Override
    public List<RemoteCorfuTableDatabaseEntry> getEntryBatch() {
        List<RemoteCorfuTableDatabaseEntry> entries = new LinkedList<>();
        for (int i = 0; i < args.length; i += 2) {
            RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(args[i], timestamp);
            RemoteCorfuTableDatabaseEntry entry = new RemoteCorfuTableDatabaseEntry(key, args[i+1]);
            entries.add(entry);
        }
        return entries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RemoteCorfuTableSMRMethods getType() {
        return RemoteCorfuTableSMRMethods.UPDATE;
    }
}
