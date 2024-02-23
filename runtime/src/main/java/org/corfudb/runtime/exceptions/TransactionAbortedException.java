package org.corfudb.runtime.exceptions;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

import java.util.UUID;

/**
 * Created by mwei on 1/11/16.
 */
public class TransactionAbortedException extends RuntimeException {

    @Getter
    AbortCause abortCause;

    @Getter
    TxResolutionInfo txResolutionInfo;

    /**
     * The key that triggered the abort.
     */
    @Getter
    byte[] conflictKey;

    /**
     * The stream ID at which the conflict occurred.
     */
    @Getter
    UUID conflictStream;

    /**
     * The address at which the conflict occurred.
     */
    @Getter
    Long offendingAddress;

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
    public TransactionAbortedException(TxResolutionInfo txResolutionInfo,
            byte[] conflictKey, UUID conflictStream, Long offendingAddress, AbortCause abortCause,
            AbstractTransactionalContext context) {
        this(txResolutionInfo, conflictKey, conflictStream, offendingAddress, abortCause, null, context);
    }

    public TransactionAbortedException(TxResolutionInfo txResolutionInfo,
            AbortCause abortCause, Throwable cause, AbstractTransactionalContext context) {
        this(txResolutionInfo, TokenResponse.NO_CONFLICT_KEY, TokenResponse.NO_CONFLICT_STREAM,
                Address.NON_ADDRESS, abortCause, cause, context);
    }

    public TransactionAbortedException(TxResolutionInfo txResolutionInfo,
            byte[] conflictKey, UUID conflictStream, Long offendingAddress,
            AbortCause abortCause, Throwable cause, AbstractTransactionalContext context) {
        super("TX ABORT "
                + " | Snapshot Time = " + txResolutionInfo.getSnapshotTimestamp()
                + " | Failed Transaction ID = " + txResolutionInfo.getTXid()
                + " | Offending Address = " + offendingAddress
                + " | Conflict Key = " + Utils.bytesToHex(conflictKey)
                + " | Conflict Stream = " + (TransactionalContext.getRootContext() != null &&
                TransactionalContext.getRootContext().getTxnContext() != null ?
                TransactionalContext.getRootContext().getTxnContext()
                        .getTableNameFromUuid(conflictStream): conflictStream)
                + " | Cause = " + abortCause
                + " | Time = " + (context == null ? "Unknown" :
                System.currentTimeMillis() -
                context.getStartTime()) + " ms"
                + (cause == null ? "" : " | Message = " + cause.getMessage()));
        this.txResolutionInfo = txResolutionInfo;
        this.conflictKey = conflictKey;
        this.conflictStream = conflictStream;
        this.offendingAddress = offendingAddress;
        this.abortCause = abortCause;
        this.cause = cause;
        this.context = context;
    }

}
