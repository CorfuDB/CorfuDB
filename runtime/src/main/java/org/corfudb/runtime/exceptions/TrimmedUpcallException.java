package org.corfudb.runtime.exceptions;

/**
 * This exception is thrown when a client attempts to resolve
 * an upcall, but the address is trimmed before the upcall
 * result can be resolved.
 *
 * <p>Created by mwei on 6/7/17.
 */
public class TrimmedUpcallException extends TrimmedException {

    public TrimmedUpcallException() {
        super("Attempted to get upcall result"
                + " but it was trimmed before we could read it");
    }
}
