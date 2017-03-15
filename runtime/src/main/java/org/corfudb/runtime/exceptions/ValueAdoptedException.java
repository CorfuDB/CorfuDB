package org.corfudb.runtime.exceptions;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.ReadResponse;

/**
 * Returned during recovery write of the quorum replication, when a new value should be adopted
 * Created by kspirov on 3/21/17.
 */
@Data
@AllArgsConstructor
public class ValueAdoptedException extends DataRejectedException {
    private ReadResponse readResponse;
}
