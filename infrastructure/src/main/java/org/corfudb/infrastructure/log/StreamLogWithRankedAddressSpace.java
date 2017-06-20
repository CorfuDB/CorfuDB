/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.corfudb.infrastructure.log;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;


/**
 * Stream log that is able to compare the data on the same log address using the rank and type.
 *
 * <p>Created by Konstantin Spirov on 3/15/2017.
 */

public interface StreamLogWithRankedAddressSpace extends StreamLog {

    /**
     * Check whether the data can be appended to a given log address.
     * Note that it is not permitted multiple threads to access the same log address
     * concurrently through this method, this method does not lock or synchronize.
     * This method needs
     * @param address  log address of append
     * @param newEntry log entry to append
     * @throws DataOutrankedException if the log entry cannot be assigned to this log address
     *     as there is a data with higher rank
     * @throws ValueAdoptedException if the new message is a proposal during the two phase recovery
     *     write and there is an existing
     *     data at this log address already.
     * @throw OverwriteException if the new data is with rank 0 (not from recovery write).
     *     This can happen only if there is a bug in the client implementation.
     */
    default void assertAppendPermittedUnsafe(long address, LogData newEntry)
            throws DataOutrankedException, ValueAdoptedException {
        LogData oldEntry = read(address);
        if (oldEntry.getType() == DataType.EMPTY) {
            return;
        }
        if (newEntry.getRank().getRank() == 0) {
            // data consistency in danger
            throw new OverwriteException();
        }
        int compare = newEntry.getRank().compareTo(oldEntry.getRank());
        if (compare < 0) {
            throw new DataOutrankedException();
        }
        if (compare > 0) {
            if (newEntry.getType() == DataType.RANK_ONLY
                    && oldEntry.getType() != DataType.RANK_ONLY) {
                // the new data is a proposal, the other data is not,
                // so the old value should be adopted
                ReadResponse resp = new ReadResponse();
                resp.put(address, oldEntry);
                throw new ValueAdoptedException(resp);
            } else {
                return;
            }
        }
    }
}
