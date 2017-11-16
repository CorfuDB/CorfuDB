package org.corfudb.runtime.view;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.CFUtils;


/**
 * Created by mwei on 12/10/15.
 */

public class SequencerView extends AbstractView {

    public SequencerView(CorfuRuntime runtime) {
        super(runtime);
    }

    public TokenResponse query() {
        return layoutHelper(l -> CFUtils.getUninterruptibly(l.getSequencer(0).query()));
    }

    public TokenResponse query(@Nonnull UUID stream) {
        return layoutHelper(l -> CFUtils.getUninterruptibly(l.getSequencer(0).query(stream)));
    }

    /**
     * Return the next token in the sequencer for a particular stream.
     *
     * <p>If numTokens == 0, then the streamAddressesMap returned is the last handed out token for
     * each stream (if streamIDs is not empty). The token returned is the global address as
     * previously defined, namely, max global address across all the streams.</p>
     *
     * @param streamIds The stream IDs to retrieve from.
     * @param numTokens The number of tokens to reserve.
     * @return The first token retrieved.
     */
    @Deprecated
    public TokenResponse nextToken(List<UUID> streamIds, int numTokens) {
        return layoutHelper(l -> CFUtils.getUninterruptibly(l.getSequencer(0)
                .nextToken(streamIds, numTokens)));
    }

    /** Deprecated. Use List as a streamIds parameter instead. */
    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens) {
        return nextToken(Lists.newArrayList(streamIDs), numTokens);
    }

    /** Deprecated. Use List as a streamIds parameter instead. */
    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens,
                                   TxResolutionInfo conflictInfo) {
        if (numTokens > 1) {
            throw new UnsupportedOperationException("Requesting more than one token "
                    + "no longer supported");
        } else if (numTokens <= 0) {
            throw new UnsupportedOperationException("Querying with conflict info "
                    + "no longer supported");
        }
        return nextToken(Lists.newArrayList(streamIDs), conflictInfo);
    }


    public TokenResponse nextToken(List<UUID> streamIDs, TxResolutionInfo conflictInfo) {
        return layoutHelper(l -> CFUtils.getUninterruptibly(l.getSequencer(0)
                .nextToken(streamIDs, conflictInfo)));
    }

    public TokenResponse nextToken(List<UUID> streamIDs) {
        return layoutHelper(l -> CFUtils.getUninterruptibly(l.getSequencer(0)
                .nextToken(streamIDs)));
    }

    public void trimCache(long address) {
        getCurrentLayout().getSequencer(0)
                .trimCache(address);
    }
}