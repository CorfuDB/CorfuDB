package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import lombok.NonNull;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.UUID;

/**
 * This interface defines the behavior of all SMR operations read from the log.
 *
 * Created by nvaishampayan517 on 08/19/21
 */
public interface SMROperation {
    /**
     * Apply SMR method with arguments to the appropriate server side table.
     * @param dbHandler Database to persist arguments to.
     * @throws RocksDBException An error in database operation.
     */
    void applySMRMethod(@NonNull DatabaseHandler dbHandler) throws RocksDBException;

    /**
     * Creates the database entry batch for writing to the database.
     * Used to compose operations in CompositeOperation.
     * @return The batch of entries to be written by this operation.
     */
    List<RemoteCorfuTableDatabaseEntry> getEntryBatch();

    /**
     * Returns the type of SMR Method contained in the operation.
     * @return Enum representing type of SMR method.
     */
    RemoteCorfuTableSMRMethods getType();

    /**
     * Returns the stream ID associated with this operation.
     * @return The stream ID associated with this operation.
     */
    UUID getStreamId();
}
