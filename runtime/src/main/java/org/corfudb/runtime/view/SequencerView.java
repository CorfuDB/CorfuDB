package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.util.CFUtils;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 12/10/15.
 */
public class SequencerView extends AbstractView {

    public SequencerView(CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Return the next token in the sequence for a particular stream.
     *
     * If numTokens == 0, then the streamAddressesMap returned is the last handed out token for
     * each stream (if streamIDs is not empty). The token returned is the global address as
     * previously defined, namely, max global address across all the streams.
     *
     * @param streamIDs The stream IDs to retrieve from.
     * @param numTokens The number of tokens to reserve.
     * @return The first token retrieved.
     */
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens) {
        return layoutHelper(l -> CFUtils.getUninterruptibly(l.getSequencer(0).nextToken(streamIDs, numTokens)));
    }

    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens, boolean overwrite, boolean replexOverwrite) {
        return layoutHelper(l -> CFUtils.getUninterruptibly(l.getSequencer(0).nextToken(
                streamIDs, numTokens, overwrite, replexOverwrite)));
    }

    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens, boolean overwrite, boolean replexOverwrite,
                                                        boolean txnResolution, long readTimestamp, Set<UUID> readSet) {
        return layoutHelper(l -> CFUtils.getUninterruptibly(l.getSequencer(0).nextToken(
                streamIDs, numTokens, overwrite, replexOverwrite, txnResolution, readTimestamp, readSet)));
    }
}
