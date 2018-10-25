package org.corfudb.runtime.exceptions;

import java.util.UUID;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.util.Utils;

/**
 * Created by mwei on 1/11/16.
 */
public class TransactionAbortedException extends RuntimeException {

    @Getter
    AbortCause abortCause;

    @Getter
    TxResolutionInfo txResolutionInfo;

    @Getter
    byte[] conflictKey;

    @Getter
    UUID conflictStream;

    @Getter
    Throwable cause;

    @Getter
    AbstractTransactionalContext context;

    /**
     * Constructor.
     * @param txResolutionInfo transaction information
     * @param conflictKey conflict key
     * @param abortCause cause
     */
    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            byte[] conflictKey, AbortCause abortCause, AbstractTransactionalContext context) {
        this(txResolutionInfo, conflictKey, null, null, abortCause, null, context);
    }

    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            byte[] conflictKey, UUID conflictStream, String conflictStreamName,
            AbortCause abortCause, Throwable cause, AbstractTransactionalContext context) {
        super("TX ABORT "
                + " | Snapshot Time = " + txResolutionInfo.getSnapshotTimestamp()
                + " | Transaction ID = " + txResolutionInfo.getTXid()
                + (conflictKey == null ? "" : " | Conflict Key = " + Utils.bytesToHex(conflictKey))
                + (conflictStream == null ? "" : " | Conflict Stream UUID = " + conflictStream)
                + (conflictStreamName == null ? "" : " | Conflict Stream Name = " + conflictStreamName)
                + " | Cause = " + abortCause
                + " | Time = " + (context == null ? "Unknown" :
                System.currentTimeMillis() -
                context.getStartTime()) + " ms"
                + (cause == null ? "" : " | Message = " + cause.getMessage()));
        this.txResolutionInfo = txResolutionInfo;
        this.conflictKey = conflictKey;
        this.abortCause = abortCause;
        this.cause = cause;
        this.conflictStream = conflictStream;
        this.context = context;
    }
}