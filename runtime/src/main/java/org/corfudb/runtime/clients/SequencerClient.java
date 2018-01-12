package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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

    /**
     * Handle an ACK response from the server.
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return Always True, since the ACK message was successful.
     */
    @ClientHandler(type = CorfuMsgType.ACK)
    private static Object handleAck(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a NACK response from the server.
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return Always True, since the ACK message was successful.
     */
    @ClientHandler(type = CorfuMsgType.NACK)
    private static Object handleNack(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return false;
    }

    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ.payloadMsg(new TokenRequest(numTokens, streamIDs)));
    }

    /**
     * Fetches the next available token from the sequencer.
     *
     * @param streamIDs    Set of streamIDs to be assigned ot the token.
     * @param numTokens    Number of tokens to be reserved.
     * @param conflictInfo Transaction resolution conflict parameters.
     * @return A completable future with the token response from the sequencer.
     */
    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens,
                                                      TxResolutionInfo conflictInfo) {
        return router.sendMessageAndGetCompletable(
                CorfuMsgType.TOKEN_REQ
                        .payloadMsg(new TokenRequest(numTokens, streamIDs, conflictInfo)));
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

    public CompletableFuture<Boolean> isReady() {
        return router.sendMessageAndGetCompletable(new CorfuMsg(CorfuMsgType.SEQUENCER_STATUS_REQ));
    }
}
