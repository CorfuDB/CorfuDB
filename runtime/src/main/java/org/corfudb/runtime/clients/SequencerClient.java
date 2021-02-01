package org.corfudb.runtime.clients;

import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.protocols.service.CorfuProtocolSequencer.getBootstrapSequencerRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getDefaultSequencerMetricsRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getSequencerTrimRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getStreamsAddressRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getTokenRequestMsg;

/**
 * A sequencer client.
 *
 * <p>This client allows the client to obtain sequence numbers from a sequencer.
 *
 * <p>Created by mwei on 12/10/15.
 */
public class SequencerClient extends AbstractClient {

    public SequencerClient(IClientRouter router, long epoch, UUID clusterID) {
        super(router, epoch, clusterID);
    }

    /**
     * Sends a metrics request to the sequencer server.
     */
    public CompletableFuture<SequencerMetrics> requestMetrics() {
        return sendRequestWithFuture(getDefaultSequencerMetricsRequestMsg(),
                ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

    /**
     * Fetches the next available token from the sequencer.
     *
     * @param streamIDs Set of streamIDs to be assigned ot the token.
     * @param numTokens Number of tokens to be reserved.
     * @return A completable future with the token response from the sequencer.
     */
    public CompletableFuture<TokenResponse> nextToken(List<UUID> streamIDs, long numTokens) {
        return sendRequestWithFuture(getTokenRequestMsg(numTokens, streamIDs),
                ClusterIdCheck.CHECK, EpochCheck.CHECK);
    }

    /**
     * Retrieves from the sequencer the address space for the specified streams in the given ranges.
     *
     * @param streamsAddressesRange requested streams and ranges.
     * @return streams address maps in the given range.
     */
    public CompletableFuture<StreamsAddressResponse> getStreamsAddressSpace(
            List<StreamAddressRange> streamsAddressesRange) {
        return sendRequestWithFuture(getStreamsAddressRequestMsg(streamsAddressesRange),
                ClusterIdCheck.CHECK, EpochCheck.CHECK);
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
        return sendRequestWithFuture(getTokenRequestMsg(numTokens, streamIDs, conflictInfo),
                ClusterIdCheck.CHECK, EpochCheck.CHECK);
    }

    public CompletableFuture<Void> trimCache(Long address) {
        return sendRequestWithFuture(getSequencerTrimRequestMsg(address),
                ClusterIdCheck.CHECK, EpochCheck.CHECK);
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
        return sendRequestWithFuture(
                getBootstrapSequencerRequestMsg(
                        streamAddressSpaceMap,
                        initialToken,
                        readyStateEpoch,
                        bootstrapWithoutTailsUpdate),
                ClusterIdCheck.CHECK, EpochCheck.CHECK);
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
