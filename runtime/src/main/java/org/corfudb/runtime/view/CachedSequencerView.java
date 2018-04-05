package org.corfudb.runtime.view;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;

/** This is a sequencer view which implements a simple cache for the global and
 *  stream tails at the small cost of memory to hold the tails
 *  (equal to approximately 24 bytes per stream).
 *
 *  Only requests that use the {@link SequencerView#cachedTail(UUID...)} method will use
 *  the cache. All other requests go to the sequencer.
 *
 */
public class CachedSequencerView extends SequencerView {

    // A cache of the current tail
    LongAccumulator tailCache = new LongAccumulator(Long::max, Address.NEVER_READ);

    // A cache of stream tails
    Map<UUID, LongAccumulator> streamTailCache = new ConcurrentHashMap<>();

    /** Construct a new cached sequencer view.
     *
     * @param runtime   The runtime to use.
     */
    public CachedSequencerView(CorfuRuntime runtime) {
        super(runtime);
    }

    @Override
    public TokenResponse conditional(@Nonnull TxResolutionInfo info, UUID... streams) {
        return cacheToken(null, super.conditional(info, streams));
    }

    @Override
    public TokenResponse next(UUID... streams) {
        return cacheToken(null, super.next(streams));
    }

    @Override
    public TokenResponse tail(UUID... streams) {
        return cacheToken(null, super.tail(streams));
    }

    /**
     * {@inheritDoc}
     *
     * When information for a tail has been previously cached, we return the cached tails
     * otherwise, we go to the sequencer if it is not available.
     *
     * @param streams   The streams to retrieve a tail for.
     * @return          A {@link TokenResponse}, which may be cached.
     */
    @Override
    public TokenResponse cachedTail(UUID... streams) {
        // If the request is for more than one stream (which we don't yet support)
        // or if the cache is not populated, go to the sequencer.
        if (streams.length > 1 || tailCache.get() == Address.NEVER_READ) {
            return tail(streams);
        }

        // If the request is for a stream tail
        if (streams.length == 1) {
            // Get the accumulator for the stream
            LongAccumulator streamTail = streamTailCache.get(streams[0]);
            // We don't have this stream cached, go to the sequencer.
            if (streamTail == null) {
                return tail(streams);
            }
            // Return the cached tail.
            return new TokenResponse(streamTail.get(), 0, Collections.emptyMap());
        }

        // Return the cached global tail.
        return new TokenResponse(tailCache.longValue(), 0, Collections.emptyMap());
    }

    /** Cache a incoming token response.
     *
     * @param info      Transaction resolution info, if available.
     * @param response  The token that was returned.
     * @return          The token that was passed in, for convenience.
     */
    private TokenResponse cacheToken(@Nullable TxResolutionInfo info,
                                     @Nonnull TokenResponse response) {
        final long tail = response.getTokenValue();

        // Advance the global tail cache
        tailCache.accumulate(tail);

        // Advance the stream tail cache (every stream in the backpointer map = new global tail)
        response.getBackpointerMap().keySet()
                .forEach(k -> {
                    LongAccumulator streamTailAccumulator =
                           streamTailCache.getOrDefault(k, new LongAccumulator(Long::max, tail));
                    streamTailAccumulator.accumulate(tail);
                });

        return response;
    }
}
