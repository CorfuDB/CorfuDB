package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.CFUtils;


/**
 * Created by mwei on 12/10/15.
 */

public class SequencerView extends AbstractView {

    public SequencerView(CorfuRuntime runtime) {
        super(runtime);
    }

    /** Conditionally request a token using the given transactional resolution information.
     *
     * @param info      The {@link TxResolutionInfo} which specifies the condition to issue
     *                  the token with.
     * @param streams   The streams which we are requesting a token for. At least one stream must
     *                  be specified.
     *
     * @return          A {@link TokenResponse} containing the next token or an abort type if no
     *                  token was issued.
     */
    public TokenResponse conditional(@Nonnull TxResolutionInfo info, UUID... streams) {
        if (streams.length == 0) {
            throw new IllegalArgumentException("At least one stream must be provided!");
        }
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
            .nextToken(new HashSet<>(Arrays.asList(streams)), 1, info)));
    }

    /** Request the next token for the given streams.
     *
     * @param streams   The streams which we are requesting a token for. At least one stream must
     *                  be specified.
     * @return          A {@link TokenResponse} containing the next token.
     */
    public TokenResponse next(UUID... streams) {
        if (streams.length == 0) {
            throw new IllegalArgumentException("At least one stream must be provided!");
        }
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
            .nextToken(new HashSet<>(Arrays.asList(streams)), 1)));
    }

    /** Request the current tail (the last issued token). This method is guaranteed to call
     * the sequencer for the current tail, so it is always up to date.
     *
     * @param streams   The streams which we want the current tail for. If no streams are provided,
     *                  the global tail is returned.
     * @return          A {@link TokenResponse} containing the requested tail.
     */
    public TokenResponse tail(UUID... streams) {
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
            .nextToken(new HashSet<>(Arrays.asList(streams)), 0)));
    }

    /** Request the current tail, which may be cached.
     *
     * @param streams   The streams which we want the current tail for. If no streams are provided,
     *                  the global tail is returned.
     * @return          A {@link TokenResponse} containing the requested tail, which may be
     *                  cached.
     */
    public TokenResponse cachedTail(UUID... streams) {
        return tail(streams);
    }

    /**
     * Return the next token in the sequencer for a particular stream.
     *
     * <p>If numTokens == 0, then the streamAddressesMap returned is the last handed out token for
     * each stream (if streamIDs is not empty). The token returned is the global address as
     * previously defined, namely, max global address across all the streams.</p>
     *
     * @param streamIDs The stream IDs to retrieve from.
     * @param numTokens The number of tokens to reserve.
     * @return The first token retrieved.
     *
     * @deprecated Use {@link SequencerView#next(UUID...)} or {@link SequencerView#tail(UUID...)}
     *             instead.
     */
    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens) {
        if (numTokens == 0) {
            return tail(streamIDs.toArray(new UUID[0]));
        } else if (numTokens == 1) {
            return next(streamIDs.toArray(new UUID[0]));
        } else {
            throw new UnsupportedOperationException("nextToken other than 0,1 not supported");
        }
    }

    /**
     * Return a next token, possibly conditionally.
     *
     * @param streamIDs     The streams to retrieve a token for
     * @param numTokens     The number of tokens. This must be 1.
     * @param conflictInfo  The conflict information to use.
     * @return              The next token, which may not be issued if the request was conditional.
     *
     * @deprecated Use {@link SequencerView#conditional(TxResolutionInfo, UUID...)} instead.
     */
    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens,
                                   TxResolutionInfo conflictInfo) {
        if (numTokens == 1) {
            if (conflictInfo != null) {
                return conditional(conflictInfo, streamIDs.toArray(new UUID[0]));
            } else {
                return next(streamIDs.toArray(new UUID[0]));
            }
        }
        throw new UnsupportedOperationException("nextToken with TX other than 1 not supported");
    }

    public void trimCache(long address) {
        runtime.getLayoutView().getRuntimeLayout().getPrimarySequencerClient().trimCache(address);
    }
}