package org.corfudb.runtime.view;

import lombok.Builder;
import lombok.Getter;

/**
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
    final boolean ignoreTrimmed;

    /**
     * Cache this stream's entries
     */
    @Getter
    final boolean cacheEntries;
}
