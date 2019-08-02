package org.corfudb.runtime.view;

import lombok.Builder;
import lombok.Getter;

/**
 * Options for the stream layer to configure a stream's caching/read behavior.
 *
 * Created by maithem on 6/20/17.
 */
@Builder
public class StreamOptions {
    public static StreamOptions DEFAULT = StreamOptions.builder()
            .ignoreTrimmed(false)
            .cacheEntries(true)
            .build();

    /**
     * Ignore trimmed exceptions encountered while syncing
     */
    @Getter
    @Builder.Default
    private final boolean ignoreTrimmed = false;

    /**
     * Cache this stream's entries
     */
    @Getter
    @Builder.Default
    private final boolean cacheEntries = true;
}
