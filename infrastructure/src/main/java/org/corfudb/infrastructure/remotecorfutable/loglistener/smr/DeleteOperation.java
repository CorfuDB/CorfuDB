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
 * This class contains the logic for the delete operation from stream listener to database.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
@AllArgsConstructor
public class DeleteOperation implements SMROperation {
    @NonNull
    private final ByteString[] args;

    private final long timestamp;
    @NonNull
    private final UUID streamId;

    /**
     * {@inheritDoc}
     * <p>
     *     Performs delete database operation with given keys.
     * </p>
     */
    @Override
    public void applySMRMethod(@NonNull DatabaseHandler dbHandler) throws RocksDBException {
        if (args.length == 1) {
            //single delete case
            RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(args[0], timestamp);
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
        for (ByteString arg : args) {
            RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(arg, timestamp);
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
