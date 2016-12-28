package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import jdk.nashorn.internal.objects.annotations.Getter;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A token request is at the heart of the Corfu log protocol.
 *
 * There are four token request scenarios, designated by the relevant constants :
 * 0. {@link TokenRequest::TK_QUERY} : Query of the current log tail and of specific stream-tails.
 * 1. {@link TokenRequest::TK_RAW} : Ask for raw (global) log token(s).
 *              This extends the global log tail by the requested # of tokens.
 * 2. {@link TokenRequest::TK_STREAM} : Ask for token(s) on a specific stream.
 *          This extends both the global log tail, and the specific stream tail, by the requested # of tokens.
 * 3. {@link TokenRequest::TK_MULTI_STREAM} : Ask for token(s) on multiple branches.
 *          This extends both the global log tail, and each of the specified stream tails, by the requested # of tokens.
 * 4. {@link TokenRequest::TK_TX} :
 *          First, check transaction resolution. If transaction can commit, then behave like {@link TokenRequest::TK_MULTI_STREAM}.
 */
@Data
@AllArgsConstructor
public class TokenRequest implements ICorfuPayload<TokenRequest> {

    public static final byte TK_QUERY = 0;
    public static final byte TK_RAW = 1;
    // todo: remove ..public static final byte TK_STREAM = 2;
    public static final byte TK_MULTI_STREAM = 3;
    public static final byte TK_TX = 4;

    /** The type of request, one of the above */
    final byte reqType;

    /** The number of tokens to request. */
    final Long numTokens;

    /** The branches which are written to by this token request. */
    final Set<UUID> branches;

    /* True if the Replex protocol encountered an overwrite at the global log layer. */
    @Deprecated
    final Boolean overwrite = false; // todo : deprecate

    /* True if the Replex protocol encountered an overwrite at the local stream layer. */
    @Deprecated
    final Boolean replexOverwrite = false; // todo: deprecate

    /* used for transaction resolution. */
    final TxResolutionInfo txnResolution;

    public TokenRequest(Long numTokens, Set<UUID> branches, Boolean overwrite, Boolean replexOverwrite,
                        boolean isTx, TxResolutionInfo conflictInfo) {
        reqType = TK_TX;
        this.numTokens = numTokens;
        this.branches = branches;
        txnResolution = conflictInfo;
    }

    public TokenRequest(Long numTokens, Set<UUID> branches, Boolean overwrite, Boolean replexOverwrite) {
        if (numTokens == 0)
            this.reqType = TK_QUERY;
        else if (branches == null || branches.size() == 0)
            this.reqType = TK_RAW;
        else
            this.reqType = TK_MULTI_STREAM;
        this.numTokens = numTokens;
        this.branches = branches;
        txnResolution = null;
    }

    public TokenRequest(ByteBuf buf) {
        reqType = ICorfuPayload.fromBuffer(buf, Byte.class);

        switch (reqType) {

            case TK_QUERY:
                numTokens = 0L;
                branches = ICorfuPayload.setFromBuffer(buf, UUID.class);
                txnResolution = null;
                break;

            case TK_RAW:
                numTokens = ICorfuPayload.fromBuffer(buf, Long.class);
                branches = null;
                txnResolution = null;
                break;

            case TK_MULTI_STREAM:
                numTokens = ICorfuPayload.fromBuffer(buf, Long.class);
                branches = ICorfuPayload.setFromBuffer(buf, UUID.class);
                txnResolution = null;
                break;

            case TK_TX:
                numTokens = ICorfuPayload.fromBuffer(buf, Long.class);
                branches = ICorfuPayload.setFromBuffer(buf, UUID.class);
                txnResolution = new TxResolutionInfo(buf);
                break;

            default:
                numTokens = -1L;
                branches = null;
                txnResolution = null;
                break;
        }
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, reqType);
        if (reqType != TK_QUERY)
            ICorfuPayload.serialize(buf, numTokens);

        if (reqType != TK_RAW)
            ICorfuPayload.serialize(buf, branches);

        if (reqType == TK_TX)
            ICorfuPayload.serialize(buf, txnResolution);
    }
}