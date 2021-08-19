package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import lombok.NonNull;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;
import org.rocksdb.RocksDBException;

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
     * Returns the type of SMR Method contained in the operation.
     * @return Enum representing type of SMR method.
     */
    RemoteCorfuTableSMRMethods getType();
}
