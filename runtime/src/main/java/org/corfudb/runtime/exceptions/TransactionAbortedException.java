package org.corfudb.runtime.exceptions;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

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
    Set<UUID> invalidStreams;

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
            byte[] conflictKey, UUID conflictStream, Long offendingAddress, Set<UUID> invalidStreams,
           AbortCause abortCause, AbstractTransactionalContext context) {
        this(txResolutionInfo, conflictKey, conflictStream, offendingAddress, invalidStreams, abortCause,
                null, context);
    }

    public TransactionAbortedException(TxResolutionInfo txResolutionInfo,
            AbortCause abortCause, Throwable cause, AbstractTransactionalContext context) {
        this(txResolutionInfo, TokenResponse.NO_CONFLICT_KEY, TokenResponse.NO_CONFLICT_STREAM,
                Address.NON_ADDRESS, Collections.emptySet(), abortCause, cause, context);
    }

    public TransactionAbortedException(TxResolutionInfo txResolutionInfo,
                                       byte[] conflictKey, UUID conflictStream, Long offendingAddress,
                                       Set<UUID> invalidStreams, AbortCause abortCause, Throwable cause, AbstractTransactionalContext context) {
        super("TX ABORT "
                + " | Snapshot Time = " + txResolutionInfo.getSnapshotTimestamp()
                + " | Failed Transaction ID = " + txResolutionInfo.getTXid()
                + " | Offending Address = " + offendingAddress
                + " | Conflict Key = " + Utils.bytesToHex(conflictKey)
                + " | Invalid Streams = " + invalidStreams
                + " | Conflict Stream = " + conflictStream
                + " | Cause = " + abortCause
                + " | Time = " + (context == null ? "Unknown" :
                System.currentTimeMillis() -
                context.getStartTime()) + " ms"
                + (cause == null ? "" : " | Message = " + cause.getMessage()));
        this.txResolutionInfo = txResolutionInfo;
        this.conflictKey = conflictKey;
        this.conflictStream = conflictStream;
        this.offendingAddress = offendingAddress;
        this.invalidStreams = invalidStreams;
        this.abortCause = abortCause;
        this.cause = cause;
        this.context = context;
    }

}
