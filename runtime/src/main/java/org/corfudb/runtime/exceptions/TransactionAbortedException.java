package org.corfudb.runtime.exceptions;

import lombok.Getter;

/**
 * Created by mwei on 1/11/16.
 */
public class TransactionAbortedException extends RuntimeException {
    @Getter
    long conflictAddress;

    @Getter
    AbortCause abortCause;

    public TransactionAbortedException(long conflictAddress, AbortCause abortCause) {

        super("TX abort. " +
                "cause=" + abortCause +
                "; conflict address=" + conflictAddress );
        this.abortCause = abortCause;
        this.conflictAddress = conflictAddress;
    }

}
