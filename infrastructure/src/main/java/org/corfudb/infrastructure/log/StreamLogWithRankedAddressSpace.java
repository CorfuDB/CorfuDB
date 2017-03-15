/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.infrastructure.log;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.Optional;

/**
 * Stream log that is able to compare the data on the same log address using the rank and type.
 *
 * Created by Konstantin Spirov on 3/15/2017.
 */
public interface StreamLogWithRankedAddressSpace extends StreamLog {
    /**
     * Check whether the data can be appended to a given log address
     * @param logAddress
     * @param newEntry
     * @return true if the new entry can be appended, false if the new entry cannot be appended,
     * empty if the entry is the same so the request should be ignored in order to be idempotent.
     */
    default Optional<Boolean> isAppendPermitted(LogAddress logAddress, LogData newEntry) {
        LogData oldEntry = read(logAddress);
        if (oldEntry.getType()==DataType.EMPTY) {
            return Optional.of(Boolean.TRUE);
        }
        if (newEntry.getType()==DataType.PROPOSAL && oldEntry.getType() != DataType.PROPOSAL) {
            // the new data is a proposal, the other data is not, so the old value should be accepted
            return Optional.of(Boolean.FALSE);
        }
        int compare = newEntry.getRank().compareTo(oldEntry.getRank());
        return compare==0? Optional.empty(): compare<0? Optional.of(Boolean.FALSE): Optional.of(Boolean.TRUE);
    }
}
