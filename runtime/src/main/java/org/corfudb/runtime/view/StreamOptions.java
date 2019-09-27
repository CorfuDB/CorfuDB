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
            .cacheEntries(true)
            .build();

    /**
     * Cache this stream's entries
     */
    @Getter
    @Builder.Default
    private final boolean cacheEntries = true;
}
