package org.corfudb.runtime.exceptions;

import org.corfudb.protocols.wireprotocol.TxResolutionInfo;

import lombok.Getter;

/**
 * Created by mwei on 1/11/16.
 */
public class TransactionAbortedException extends RuntimeException {

    @Getter
    AbortCause abortCause;

    @Getter
    TxResolutionInfo txResolutionInfo;

    @Getter
    Integer conflictKey;

    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            Integer conflictKey, AbortCause abortCause) {
        super("TX ABORT " +
                " | Snapshot Time = " + txResolutionInfo.getSnapshotTimestamp() +
                " | Transaction ID = " + txResolutionInfo.getTXid() +
                " | Conflict Key = " + conflictKey +
                " | Cause = " + abortCause );
        this.txResolutionInfo = txResolutionInfo;
        this.conflictKey = conflictKey;
        this.abortCause = abortCause;
    }

}
