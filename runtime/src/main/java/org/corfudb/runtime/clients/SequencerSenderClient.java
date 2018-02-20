package org.corfudb.runtime.clients;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.SequencerTailsRecoveryMsg;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;


/**
 * A sequencer client.
 *
 * <p>This client allows the client to obtain sequence numbers from a sequencer.
 *
 * <p>Created by zlokhandwala on 2/20/18.
 */
public class SequencerSenderClient {

    private final SequencerClient sequencerClient;

    private final long epoch;

    public SequencerSenderClient(SequencerClient sequencerClient, long epoch) {
        this.sequencerClient = sequencerClient;
        this.epoch = epoch;
    }

    /**
     * Fetches the next available token from the sequencer.
     *
     * @param streamIDs Set of streamIDs to be assigned ot the token.
     * @param numTokens Number of tokens to be reserved.
     * @return A completable future with the token response from the sequencer.
     */
    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens) {
        return sequencerClient.router
                .sendMessageAndGetCompletable(CorfuMsgType.TOKEN_REQ.payloadMsg(
                        new TokenRequest(numTokens, streamIDs)).setEpoch(epoch));
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
        return sequencerClient.router
                .sendMessageAndGetCompletable(CorfuMsgType.TOKEN_REQ.payloadMsg(
                        new TokenRequest(numTokens, streamIDs, conflictInfo)).setEpoch(epoch));
    }

    public CompletableFuture<Void> trimCache(Long address) {
        return sequencerClient.router.sendMessageAndGetCompletable(
                CorfuMsgType.SEQUENCER_TRIM_REQ.payloadMsg(address).setEpoch(epoch));
    }

    /**
     * Resets the sequencer with the specified initialToken
     *
     * @param initialToken Token Number which the sequencer starts distributing.
     * @return A CompletableFuture which completes once the sequencer is reset.
     */
    public CompletableFuture<Boolean> bootstrap(Long initialToken, Map<UUID, Long> sequencerTails,
                                                Long readyStateEpoch) {
        return sequencerClient.router
                .sendMessageAndGetCompletable(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(
                        new SequencerTailsRecoveryMsg(initialToken, sequencerTails,
                                readyStateEpoch)).setEpoch(epoch));
    }
}
