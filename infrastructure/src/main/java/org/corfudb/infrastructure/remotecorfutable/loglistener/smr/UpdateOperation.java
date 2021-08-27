package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import com.google.protobuf.ByteString;
import lombok.EqualsAndHashCode;
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
@EqualsAndHashCode
public class UpdateOperation implements SMROperation {
    private final List<RemoteCorfuTableDatabaseEntry> entries;
    private final UUID streamId;

    public UpdateOperation(@NonNull List<ByteString> args, long timestamp, @NonNull UUID streamId) {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("Cannot have update operation with no keys to delete");
        }
        //if there is an odd number of arguments input, there is a key that is missing a value
        if (args.size() % 2 == 1) {
            throw new IllegalArgumentException("args must consist of key-value pairs");
        }
        entries = new LinkedList<>();
        for (int i = 0; i < args.size(); i += 2) {
            RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(args.get(i), timestamp);
            RemoteCorfuTableDatabaseEntry entry = new RemoteCorfuTableDatabaseEntry(key, args.get(i+1));
            entries.add(entry);
        }
        this.streamId = streamId;
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Performs update database operation with given key-value pairs.
     * </p>
     */
    @Override
    public void applySMRMethod(@NonNull DatabaseHandler dbHandler) throws RocksDBException {
        //optimization in case of only one key being added
        if (entries.size() == 1) {
            //single update case
            RemoteCorfuTableDatabaseEntry entry = entries.get(0);
            dbHandler.update(entry.getKey(), entry.getValue(), streamId);
        } else {
            dbHandler.updateAll(entries, streamId);
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
