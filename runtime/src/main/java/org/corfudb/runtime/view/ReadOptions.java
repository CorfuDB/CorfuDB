package org.corfudb.runtime.view;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

/**
 * When the client issues a read, the request traverses multiple layers and different configurations
 * can be set for that read requests. This ReadOptions objects enables different configurations per
 * read request.
 *
 * <p>Created by Maithem on 7/9/19.</p>
 */

@Builder(toBuilder=true)
@Data
public class ReadOptions {
    /**
     * Ignore trimmed exceptions encountered while syncing
     */
    @Getter
    @Builder.Default
    final boolean ignoreTrim = false;

    /**
     * The readers behavior when a hole is encountered
     */
    @Getter
    @Builder.Default
    private final boolean waitForHole = true;

    /**
     * Whether to cache the read on the client side
     */
    @Getter
    @Builder.Default
    private final boolean clientCacheable = true;

    /**
     * Cache hint for the server to determine whether to cache the read request or not
     */
    @Getter
    @Builder.Default
    private final boolean serverCacheable = true;
}
