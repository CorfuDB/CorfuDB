package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A token request is at the heart of the Corfu log protocol.
 *
 * <p>There are four token request scenarios, designated by the relevant constants :
 * 0. {@link TokenRequest::TK_QUERY} : Query of the current log tail and of specific stream-tails.
 * 1. {@link TokenRequest::TK_RAW} : Ask for raw (global) log token(s).
 *              This extends the global log tail by the requested # of tokens.
 * 2. {@link TokenRequest::TK_MULTI_STREAM} : Ask for token(s) on multiple streams.
 *          This extends both the global log tail, and each of the specified stream tails,
 *          by the requested # of tokens.
 * 3. {@link TokenRequest::TK_TX} :
 *          First, check transaction resolution. If transaction can commit, then behave
 *          like {@link TokenRequest::TK_MULTI_STREAM}.</p>
 */
@Data
@AllArgsConstructor
public class TokenRequest implements ICorfuPayload<TokenRequest> {

    public static final byte TK_QUERY = 0;
    public static final byte TK_RAW = 1;
    public static final byte TK_QUERY_STREAM = 2;
    public static final byte TK_MULTI_STREAM = 3;
    public static final byte TK_TX = 4;

    /** The type of request, one of the above. */
    final byte reqType;

    /** The streams which are written to by this token request. */
    final List<UUID> streams;

    /* used for transaction resolution. */
    final TxResolutionInfo txnResolution;

    /**
     * Constructor for generating TokenRequest.
     *
     * @param streams streams UUIDs required in the token request
     * @param conflictInfo transaction resolution information
     */
    public TokenRequest(@Nonnull List<UUID> streams,
                        @Nonnull TxResolutionInfo conflictInfo) {
        reqType = TK_TX;
        this.streams = streams;
        txnResolution = conflictInfo;
    }

    public TokenRequest(@Nonnull List<UUID> streams) {
        reqType = TK_MULTI_STREAM;
        this.streams = streams;
        txnResolution = null;
    }

    public TokenRequest(@Nonnull UUID stream) {
        reqType = TK_QUERY_STREAM;
        this.streams = Collections.singletonList(stream);
        txnResolution = null;
    }

    public TokenRequest(boolean query) {
       reqType = query ? TK_QUERY : TK_RAW;
       this.streams = null;
       txnResolution = null;
    }

    /** Deprecated, use list instead of set. */
    @Deprecated
    public TokenRequest(Long numTokens, Set<UUID> streams) {
        this(numTokens, Lists.newArrayList(streams));
    }

    /**
     * Constructor for generating TokenRequest.
     *
     * @param numTokens number of tokens to request
     * @param streams streams UUIDs required in the token request
     */
    @Deprecated
    public TokenRequest(Long numTokens, List<UUID> streams) {
        if (numTokens > 1) {
            throw new UnsupportedOperationException("Requesting more than one token"
                    + " no longer supported.");
        }
        if (numTokens == 0) {
            this.reqType = streams.isEmpty() ? TK_QUERY : TK_QUERY_STREAM;
        } else if (streams == null || streams.size() == 0) {
            this.reqType = TK_RAW;
        } else {
            this.reqType = TK_MULTI_STREAM;
        }
        this.streams = streams;
        txnResolution = null;
    }

    /**
     * Deserialization Constructor from Bytebuf to TokenRequest.
     *
     * @param buf The buffer to deserialize
     */
    public TokenRequest(ByteBuf buf) {
        reqType = buf.readByte();

        switch (reqType) {

            case TK_QUERY:
                streams = null;
                txnResolution = null;
                break;
            case TK_QUERY_STREAM:
                streams = ICorfuPayload.listFromBuffer(buf, UUID.class);
                txnResolution = null;
                break;
            case TK_RAW:
                streams = null;
                txnResolution = null;
                break;
            case TK_MULTI_STREAM:
                streams = ICorfuPayload.listFromBuffer(buf, UUID.class);
                txnResolution = null;
                break;
            case TK_TX:
                streams = ICorfuPayload.listFromBuffer(buf, UUID.class);
                txnResolution = new TxResolutionInfo(buf);
                break;
            default:
                streams = null;
                txnResolution = null;
                break;
        }
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, reqType);
        if (reqType != TK_RAW && reqType != TK_QUERY) {
            ICorfuPayload.serialize(buf, streams);
        }

        if (reqType == TK_TX) {
            ICorfuPayload.serialize(buf, txnResolution);
        }
    }
}