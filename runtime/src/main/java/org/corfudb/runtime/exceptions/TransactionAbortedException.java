package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.object.transactions.TransactionType;

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

    @Getter
    Throwable cause;

    /**
     * Constructor.
     * @param txResolutionInfo transaction information
     * @param conflictKey conflict key
     * @param abortCause cause
     */
    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            Integer conflictKey, AbortCause abortCause) {
        this(txResolutionInfo, conflictKey, abortCause, null, TransactionType.OPTIMISTIC);
    }

    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            Integer conflictKey, AbortCause abortCause, Throwable cause) {
        this(txResolutionInfo, conflictKey, abortCause, cause, TransactionType.OPTIMISTIC);
    }

    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            Integer conflictKey, AbortCause abortCause, Throwable cause, TransactionType txType) {
        super("TX ABORT "
                + " | Snapshot Time = " + txResolutionInfo.getSnapshotTimestamp()
                + " | Transaction ID = " + txResolutionInfo.getTXid()
                + " | Transaction Type = " + txType.name()
                + " | Conflict Key = " + conflictKey
                + " | Cause = " + abortCause);
        this.txResolutionInfo = txResolutionInfo;
        this.conflictKey = conflictKey;
        this.abortCause = abortCause;
        this.cause = cause;
    }

}
