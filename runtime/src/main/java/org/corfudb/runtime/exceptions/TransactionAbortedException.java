package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.runtime.view.Layout;

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
