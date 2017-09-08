package org.corfudb.runtime.exceptions;

import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.object.transactions.AbstractTransaction;
import org.corfudb.util.Utils;
import org.corfudb.runtime.view.Address;

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
    AbstractTransaction context;

    /** True, if it is known that this abort was not caused by a false conflict. */
    @Getter
    @Setter
    boolean precise = false;

    /** If available, the address where the conflict was detected,
     *  otherwise, Address.NON_EXIST.
     */
    @Getter
    long conflictAddress = Address.NON_EXIST;

    /**
     * Constructor.
     * @param txResolutionInfo transaction information
     * @param conflictKey conflict key
     * @param abortCause cause
     */
    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            byte[] conflictKey, AbortCause abortCause, AbstractTransaction context) {
        this(txResolutionInfo, conflictKey, null, abortCause, null, context);
    }

    public TransactionAbortedException(
            TxResolutionInfo txResolutionInfo,
            byte[] conflictKey, UUID conflictStream,
            AbortCause abortCause, Throwable cause, AbstractTransaction context) {
        super("TX ABORT "
                + " | Snapshot Time = " + txResolutionInfo.getSnapshotTimestamp()
                + " | Transaction ID = " + txResolutionInfo.getTXid()
                + " | Conflict Key = " + Utils.bytesToHex(conflictKey)
                + " | Conflict Stream = " + conflictStream
                + " | Cause = " + abortCause
                + " | Time = " + (context == null ? "Unknown" :
                System.currentTimeMillis() -
                context.getStartTime()) + " ms");
        this.txResolutionInfo = txResolutionInfo;
        this.conflictKey = conflictKey;
        this.abortCause = abortCause;
        this.cause = cause;
        this.conflictStream = conflictStream;
        this.context = context;
    }

    public TransactionAbortedException(TxResolutionInfo txResolutionInfo,
                                       byte[] conflictKey, UUID conflictStream,
                                       AbortCause abortCause, Throwable cause,
                                       long conflictAddress, AbstractTransaction context) {
        this(txResolutionInfo, conflictKey, conflictStream,
                abortCause, cause, context);
        this.conflictAddress = conflictAddress;
    }

}
