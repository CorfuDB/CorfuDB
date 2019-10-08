package org.corfudb.runtime.clients;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerRecoveryMsg;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressRequest;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.StreamsIdResponse;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

/**
 * A sequencer client.
 *
 * <p>This client allows the client to obtain sequence numbers from a sequencer.
 *
 * <p>Created by mwei on 12/10/15.
 */
public class SequencerClient extends AbstractClient {

    public SequencerClient(IClientRouter router, long epoch) {
        super(router, epoch);
    }

    /**
     * Sends a metrics request to the sequencer server.
     */
    public CompletableFuture<SequencerMetrics> requestMetrics() {
        return sendMessageWithFuture(CorfuMsgType.SEQUENCER_METRICS_REQUEST.msg());
    }

    /**
     * Fetches the next available token from the sequencer.
     *
     * @param streamIDs Set of streamIDs to be assigned ot the token.
     * @param numTokens Number of tokens to be reserved.
     * @return A completable future with the token response from the sequencer.
     */
    public CompletableFuture<TokenResponse> nextToken(List<UUID> streamIDs, long numTokens) {
        return sendMessageWithFuture(CorfuMsgType.TOKEN_REQ.payloadMsg(
                new TokenRequest(numTokens, streamIDs)));
    }

    /**
     * Retrieves from the sequencer the address space for the specified streams in the given ranges.
     *
     * @param streamsAddressesRange requested streams and ranges.
     * @return streams address maps in the given range.
     */
    public CompletableFuture<StreamsAddressResponse> getStreamsAddressSpace(List<StreamAddressRange> streamsAddressesRange) {
        return sendMessageWithFuture(CorfuMsgType.STREAMS_ADDRESS_REQUEST.payloadMsg(
                new StreamsAddressRequest(streamsAddressesRange)));
    }

    /**
     * Fetches the next available token from the sequencer.
     *
     * @param streamIDs    Set of streamIDs to be assigned ot the token.
     * @param numTokens    Number of tokens to be reserved.
     * @param conflictInfo Transaction resolution conflict parameters.
     * @return A completable future with the token response from the sequencer.
     */
    public CompletableFuture<TokenResponse> nextToken(List<UUID> streamIDs, long numTokens,
                                                      TxResolutionInfo conflictInfo) {
        return sendMessageWithFuture(CorfuMsgType.TOKEN_REQ.payloadMsg(
                new TokenRequest(numTokens, streamIDs, conflictInfo)));
    }

    /**
     * Resets the sequencer with the specified initialToken
     *
     * @param initialToken                Token Number which the sequencer starts distributing.
     * @param streamAddressSpaceMap       Per stream map of address space.
     * @param readyStateEpoch             Epoch at which the sequencer is ready and to stamp tokens.
     * @param bootstrapWithoutTailsUpdate True, if this is a delta message and just updates an
     *                                    existing primary sequencer with the new epoch.
     *                                    False otherwise.
     * @return A CompletableFuture which completes once the sequencer is reset.
     */
    public CompletableFuture<Boolean> bootstrap(Long initialToken, Map<UUID, StreamAddressSpace> streamAddressSpaceMap,
                                                Long readyStateEpoch,
                                                boolean bootstrapWithoutTailsUpdate) {
        return sendMessageWithFuture(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(
                new SequencerRecoveryMsg(initialToken, streamAddressSpaceMap, readyStateEpoch,
                        bootstrapWithoutTailsUpdate)));
    }

    /**
     * Retrieves from the sequencer the stream ids.
     *
     * @return A CompletableFuture which wraps stream IDs.
     */
    public CompletableFuture<StreamsIdResponse> getStreamsId() {
        return sendMessageWithFuture(CorfuMsgType.STREAMS_ID_REQUEST.payloadMsg(
                StreamsAddressRequest.TOTAL));
    }

    public CompletableFuture<Boolean> streamsAddressSpaceReplace(StreamsAddressResponse streamAddressSpaceMap) {
        return sendMessageWithFuture(CorfuMsgType.STREAMS_ADDRESS_REPLACE.payloadMsg(streamAddressSpaceMap));
    }

    /**
     * Resets the sequencer with the specified initialToken.
     * BootstrapWithoutTailsUpdate defaulted to false.
     *
     * @param initialToken          Token Number which the sequencer starts distributing.
     * @param streamAddressSpaceMap Per stream map of address space.
     * @param readyStateEpoch       Epoch at which the sequencer is ready and to stamp tokens.
     * @return A CompletableFuture which completes once the sequencer is reset.
     */
    public CompletableFuture<Boolean> bootstrap(Long initialToken,
                                                Map<UUID, StreamAddressSpace> streamAddressSpaceMap,
                                                Long readyStateEpoch) {
        return bootstrap(initialToken, streamAddressSpaceMap, readyStateEpoch, false);
    }
}
