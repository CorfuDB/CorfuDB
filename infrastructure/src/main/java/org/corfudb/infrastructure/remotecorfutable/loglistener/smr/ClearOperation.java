package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;
import org.rocksdb.RocksDBException;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * This class contains the logic for the clear operation from stream listener to database.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
@AllArgsConstructor
public class ClearOperation implements SMROperation {

    private final long timestamp;
    @NonNull
    private final UUID streamId;

    /**
     * {@inheritDoc}
     * <p>
     *     Performs clear database operation with given keys.
     * </p>
     */
    @Override
    public void applySMRMethod(@NonNull DatabaseHandler dbHandler) throws RocksDBException {
        dbHandler.clear(streamId, timestamp);
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
}
