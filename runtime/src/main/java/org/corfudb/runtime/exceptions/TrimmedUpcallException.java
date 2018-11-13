package org.corfudb.runtime.exceptions;

import org.corfudb.protocols.wireprotocol.LogicalSequenceNumber;

/**
 * This exception is thrown when a client attempts to resolve
 * an upcall, but the address is trimmed before the upcall
 * result can be resolved.
 *
 * <p>Created by mwei on 6/7/17.
 */
public class TrimmedUpcallException extends TrimmedException {

    public TrimmedUpcallException(LogicalSequenceNumber address) {
        super("Attempted to get upcall result @" + address
                + " but it was trimmed before we could read it");
    }
}
