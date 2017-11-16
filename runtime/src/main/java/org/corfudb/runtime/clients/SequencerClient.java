package org.corfudb.runtime.clients;

import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerTailsRecoveryMsg;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;


/**
 * A sequencer client.
 *
 * <p>This client allows the client to obtain sequence numbers from a sequencer.
 *
 * <p>Created by mwei on 12/10/15.
 */
public class SequencerClient implements IClient {


    @Setter
    @Getter
    IClientRouter router;

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    @ClientHandler(type = CorfuMsgType.TOKEN_RES)
    private static Object handleTokenResponse(CorfuPayloadMsg<TokenResponse> msg,
                                              ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    /** Deprecated method, use a {@link List} for the streamIDs parameter instead. */
    @Deprecated
    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ.payloadMsg(
                        new TokenRequest(numTokens, Lists.newArrayList(streamIDs))));
    }

    @Deprecated
    public CompletableFuture<TokenResponse> nextToken(List<UUID> streamIDs, long numTokens) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ.payloadMsg(
                        new TokenRequest(numTokens, streamIDs)));
    }

    public CompletableFuture<TokenResponse> query(@Nonnull UUID stream) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ.payloadMsg(
                        new TokenRequest(stream)));
    }

    public CompletableFuture<TokenResponse> query() {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ.payloadMsg(
                        new TokenRequest(true)));
    }

    /** Deprecated method, use a {@link List} for the streamIDs parameter instead. */
    @Deprecated
    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens,
                                                      TxResolutionInfo conflictInfo) {
        if (numTokens <= 0 || numTokens > 1) {
            throw new UnsupportedOperationException("Requesting other than 1 token "
                    + "no longer supported!");
        }
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ
                        .payloadMsg(new TokenRequest(
                                Lists.newArrayList(streamIDs), conflictInfo)));
    }

    /**
     * Fetches the next available token from the sequencer.
     *
     * @param streamIDs    Set of streamIDs to be assigned ot the token.
     * @param conflictInfo Transaction resolution conflict parameters.
     * @return A completable future with the token response from the sequencer.
     */
    public CompletableFuture<TokenResponse> nextToken(List<UUID> streamIDs,
                                                      TxResolutionInfo conflictInfo) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ
                        .payloadMsg(new TokenRequest(streamIDs, conflictInfo)));
    }

    public CompletableFuture<TokenResponse> nextToken(List<UUID> streamIDs) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ
                        .payloadMsg(new TokenRequest(streamIDs)));
    }

    public CompletableFuture<Void> trimCache(Long address) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.SEQUENCER_TRIM_REQ
                .payloadMsg(address));
    }

    /**
     * Resets the sequencer with the specified initialToken
     *
     * @param initialToken Token Number which the sequencer starts distributing.
     * @return A CompletableFuture which completes once the sequencer is reset.
     */
    public CompletableFuture<Boolean> bootstrap(Long initialToken, Map<UUID, Long> sequencerTails,
                                            Long readyStateEpoch) {
        return router.sendMessageAndGetCompletable(CorfuMsgType.BOOTSTRAP_SEQUENCER
                .payloadMsg(new SequencerTailsRecoveryMsg(initialToken, sequencerTails,
                        readyStateEpoch)));
    }
}
